import { Component, OnInit, OnDestroy, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';
import { TierLabelService } from '../services/tier-label.service';
import { PipelineService } from '../services/pipeline.service';
import { PipelineRunRequest } from '../models';
import { JiraIntegrationComponent } from './jira-integration';

// ─────────────────────────────────────────────────────────────────────────
// System defaults — single source of truth for "Reset to Defaults".
// Mirrors the column DEFAULTs in db/walmart_crp_universal.sql for
// client_config (and login_window_days from migration 2026_04_24). When the
// schema changes, update both at the same time so a "reset" actually
// matches what a freshly-created tenant would get.
//
// MUST live above @Component — decorators have to be immediately adjacent
// to the class they decorate, so module-scope constants used inside the
// class belong before the decorator block.
// ─────────────────────────────────────────────────────────────────────────
const DEFAULTS = {
  churnWindow:        90,
  loginWindow:        30,
  repeatThreshold:    2,
  // 2026-04-25: highValuePct removed — the HIGH VALUE — SPEND PERCENTILE
  // input was retired because is_high_value duplicated customer_tier.
  // Platinum tier is now the single source of truth for high-value
  // bucketing.
  recentGapWindow:    3,
  tierMethod:         'quartile' as 'quartile' | 'custom',
  platMin:            500,
  goldMin:            250,
  silverMin:          100,
  bronzeMin:          0,
  tierLabelPlatinum:  '💎 Platinum',
  tierLabelGold:      '🥇 Gold',
  tierLabelSilver:    '🥈 Silver',
  tierLabelBronze:    '🥉 Bronze',
};

@Component({
  selector: 'app-settings',
  standalone: true,
  imports: [CommonModule, FormsModule, JiraIntegrationComponent],
  templateUrl: './settings.html',
  styleUrls: ['./settings.scss']
})
export class SettingsComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private auth = inject(AuthService);
  private tierLabels = inject(TierLabelService);
  pipelineSvc = inject(PipelineService);
  private clientId = this.auth.getClientId();

  // Pipeline rules (loaded from database)
  churnWindow = signal(90);
  // Recent-login window (days): drives the point-in-time recent_logins feature
  // in the temporal model (logins within this window of each cutoff T). A
  // SEPARATE engagement signal from orders — the churn LABEL stays order-based.
  // Has effect only once Login Events are uploaded (login_events log). NOT the
  // snapshot cadence (that's its own snapshot_cadence_days knob now).
  loginWindow = signal(30);
  // True once the tenant has uploaded Login Events — gates the Recent Login
  // Window input (the setting only affects the model when login data exists).
  hasLoginData = signal(false);
  repeatThreshold = signal(2);
  // highValuePct signal removed 2026-04-25 (see DEFAULTS comment).
  recentGapWindow = signal(3);
  tierMethod = signal('quartile');
  platMin = signal(500); goldMin = signal(250); silverMin = signal(100); bronzeMin = signal(0);

  // Value-tier display labels — persisted per client so they survive reload
  tierLabelPlatinum = signal('💎 Platinum');
  tierLabelGold     = signal('🥇 Gold');
  tierLabelSilver   = signal('🥈 Silver');
  tierLabelBronze   = signal('🥉 Bronze');

  // State
  loading = signal(true);
  saved = signal(false);
  error = signal('');

  // Static display data
  // segments array removed 2026-06-08 — Customer Categories table dropped (CTO).

  // vpRules array removed 2026-04-24 — see settings.html for the full
  // rationale. Per-tier × per-risk discount templates are now owned by the
  // Retention Agent and edited there.

  // Vendor config (loaded from database)
  vendorCfg = signal<{param: string; val: string; type: string; desc: string}[]>([]);

  // Pipeline run state
  private readonly predMode = 'full' as const;
  runError   = signal<string | null>(null);
  runSuccess = signal(false);  // ← NEW: tracks successful pipeline completion
  private sub?: Subscription;

  get running() { return this.pipelineSvc.isRunning(); }
  get progress() { return this.pipelineSvc.currentJob()?.progress ?? 0; }

  ngOnInit() {
    this.loadSettings();
  }

  ngOnDestroy() { this.sub?.unsubscribe(); }

  runPipeline() {
    this.runError.set(null);
    this.runSuccess.set(false);  // reset success banner on each new run

    const req: PipelineRunRequest = { clientId: this.clientId, mode: this.predMode };
    this.pipelineSvc.run(req).subscribe({
      next: job => {
        this.sub = this.pipelineSvc.pollJob(job.jobId, this.clientId).subscribe({
          next: (polledJob: any) => {
            // Some pipeline services emit the final job object via next()
            // rather than complete() — handle both cases here.
            if (polledJob?.status === 'done' || polledJob?.status === 'completed') {
              this.runSuccess.set(true);
              setTimeout(() => this.runSuccess.set(false), 5000);
            }
          },
          complete: () => {
            // Fires when the polling observable closes without error —
            // i.e. the job finished successfully.
            this.runSuccess.set(true);
            setTimeout(() => this.runSuccess.set(false), 5000);
          },
          error: e => this.runError.set(e.message ?? 'Pipeline failed.'),
        });
      },
      error: e => this.runError.set(e.message ?? 'Failed to start pipeline.'),
    });
  }

  loadSettings() {
    this.loading.set(true);
    this.api.get<any>(`/settings?clientId=${this.clientId}`).subscribe({
      next: (cfg) => {
        this.churnWindow.set(cfg.churn_window_days ?? 90);
        this.loginWindow.set(cfg.login_window_days ?? 30);
        this.hasLoginData.set(!!cfg.has_login_data);
        this.repeatThreshold.set(cfg.min_repeat_orders ?? 2);
        // high_value_percentile no longer returned by /settings (column dropped).
        this.recentGapWindow.set(cfg.recent_order_gap_window ?? 3);
        this.tierMethod.set(cfg.tier_method ?? 'quartile');
        this.platMin.set(cfg.custom_platinum_min ?? 500);
        this.goldMin.set(cfg.custom_gold_min ?? 250);
        this.silverMin.set(cfg.custom_silver_min ?? 100);
        this.bronzeMin.set(cfg.custom_bronze_min ?? 0);

        this.tierLabelPlatinum.set(cfg.tier_label_platinum ?? '💎 Platinum');
        this.tierLabelGold.set(    cfg.tier_label_gold     ?? '🥇 Gold');
        this.tierLabelSilver.set(  cfg.tier_label_silver   ?? '🥈 Silver');
        this.tierLabelBronze.set(  cfg.tier_label_bronze   ?? '🥉 Bronze');

        // Build vendor config display from real data
        this.vendorCfg.set([
          { param: 'client_name',       val: cfg.client_name,                    type: 'string',  desc: 'Full legal name of the retail client' },
          { param: 'client_id',         val: cfg.client_id,                      type: 'string',  desc: 'Unique client identifier' },
          { param: 'churn_window_days', val: String(cfg.churn_window_days),      type: 'integer', desc: 'Days without an order before a customer is treated as churned' },
          { param: 'login_window_days', val: String(cfg.login_window_days ?? 30), type: 'integer', desc: 'Recent-login window (days) — used as a churn-predictor feature from Login Events' },
          { param: 'min_repeat_orders', val: String(cfg.min_repeat_orders),      type: 'integer', desc: 'Completed orders needed to count as a repeat customer' },
          { param: 'currency',          val: cfg.currency,                       type: 'string',  desc: 'Transaction currency for all monetary columns' },
          { param: 'timezone',          val: cfg.timezone,                       type: 'string',  desc: 'Client timezone for date calculations' },
        ]);

        this.loading.set(false);
      },
      error: (err) => {
        this.error.set('Could not load settings. Is the backend running?');
        this.loading.set(false);
      }
    });
  }

  /**
   * Reset every editable field on the page back to the schema defaults
   * (see DEFAULTS const above). This is a LOCAL reset — it just rewrites
   * the form signals, it does NOT call /settings PUT. The user still has
   * to click "Save Settings" to persist. That keeps the action reversible
   * (navigate away or change values back) and avoids an irreversible
   * one-click wipe of carefully-tuned per-tenant config.
   */
  resetToDefaults() {
    this.churnWindow.set(DEFAULTS.churnWindow);
    this.loginWindow.set(DEFAULTS.loginWindow);
    this.repeatThreshold.set(DEFAULTS.repeatThreshold);
    // highValuePct reset removed 2026-04-25 — see DEFAULTS comment.
    this.recentGapWindow.set(DEFAULTS.recentGapWindow);
    this.tierMethod.set(DEFAULTS.tierMethod);
    this.platMin.set(DEFAULTS.platMin);
    this.goldMin.set(DEFAULTS.goldMin);
    this.silverMin.set(DEFAULTS.silverMin);
    this.bronzeMin.set(DEFAULTS.bronzeMin);
    this.tierLabelPlatinum.set(DEFAULTS.tierLabelPlatinum);
    this.tierLabelGold.set(    DEFAULTS.tierLabelGold);
    this.tierLabelSilver.set(  DEFAULTS.tierLabelSilver);
    this.tierLabelBronze.set(  DEFAULTS.tierLabelBronze);
    // Clear any prior "Saved!" badge so the user knows these values
    // haven't been persisted yet — they still need to click Save.
    this.saved.set(false);
    this.error.set('');
  }

  save() {
    this.saved.set(false);
    this.error.set('');

    // Validate custom threshold values — must be non-negative numbers
    if (this.tierMethod() === 'custom') {
      const thresholds = [
        { label: 'Platinum', value: this.platMin() },
        { label: 'Gold',     value: this.goldMin() },
        { label: 'Silver',   value: this.silverMin() },
        { label: 'Bronze',   value: this.bronzeMin() },
      ];
      for (const t of thresholds) {
        if (t.value < 0) {
          this.error.set(`${t.label} min spend cannot be negative.`);
          return;
        }
      }
    }

    const body = {
      churn_window_days: this.churnWindow(),
      login_window_days: this.loginWindow(),
      min_repeat_orders: this.repeatThreshold(),
      // high_value_percentile removed 2026-04-25 (column dropped backend-side).
      recent_order_gap_window: this.recentGapWindow(),
      tier_method: this.tierMethod(),
      custom_platinum_min: this.platMin(),
      custom_gold_min: this.goldMin(),
      custom_silver_min: this.silverMin(),
      custom_bronze_min: this.bronzeMin(),
      tier_label_platinum: this.tierLabelPlatinum(),
      tier_label_gold:     this.tierLabelGold(),
      tier_label_silver:   this.tierLabelSilver(),
      tier_label_bronze:   this.tierLabelBronze(),
    };

    this.api.put<any>(`/settings?clientId=${this.clientId}`, body).subscribe({
      next: () => {
        this.saved.set(true);
        setTimeout(() => this.saved.set(false), 2500);
        // Reload to show updated vendor config table
        this.loadSettings();
        // Tell the global tier-label cache to re-fetch so any OTHER open page
        // (Dashboard, Churn Scores, Messages) picks up the new names instantly.
        this.tierLabels.refresh();
      },
      error: (err) => {
        this.error.set(err?.error?.detail ?? 'Failed to save settings');
      }
    });
  }
}