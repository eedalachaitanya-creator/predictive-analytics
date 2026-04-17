import { Component, OnInit, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';
import { TierLabelService } from '../services/tier-label.service';

@Component({
  selector: 'app-settings',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './settings.html',
  styleUrls: ['./settings.scss']
})
export class SettingsComponent implements OnInit {
  private api = inject(ApiService);
  private auth = inject(AuthService);
  private tierLabels = inject(TierLabelService);
  private clientId = this.auth.getClientId();

  // Pipeline rules (loaded from database)
  churnWindow = signal(90);
  repeatThreshold = signal(2);
  highValuePct = signal(75);
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
  segments = [
    { name:'Champions',      cond:'R=5 · F\u22654 · M\u22654', focus:'Upsell premium / loyalty rewards',    active:true },
    { name:'Loyal Customers',cond:'F\u22654',              focus:'Exclusive offers, early access',       active:true },
    { name:'Potential Loyal',cond:'R\u22653 · F=2-3',      focus:'Onboarding series, nurture',           active:true },
    { name:'At-Risk',        cond:'R=2 · F\u22653',        focus:'Win-back campaigns, discount',         active:true },
    { name:'Hibernating',    cond:'R\u22642 · F\u22643',        focus:'Re-engagement / low-cost nudge',       active:true },
    { name:'New Customers',  cond:'R=5 · F=1',        focus:'Welcome flow, product education',      active:true },
    { name:'Lost Customers', cond:'R=1 · F\u22642',        focus:'Final win-back or suppress',           active:true },
    { name:'Casual Shoppers',cond:'RFM total 6\u201311',   focus:'Seasonal promotions, awareness',       active:true },
  ];

  vpRules = [
    { tier:'\uD83D\uDC8E Platinum', risk:'At-Risk',    action:'Personalised win-back', disc:'15%', ch:'Email+SMS', tpl:'We miss you, {name}\u2026' },
    { tier:'\uD83D\uDC8E Platinum', risk:'Returning',  action:'Loyalty reward',        disc:'10%', ch:'Email',     tpl:'Welcome back, {name}!' },
    { tier:'\uD83E\uDD47 Gold',     risk:'At-Risk',    action:'Discount offer',        disc:'10%', ch:'Email',     tpl:'Special offer for you\u2026' },
    { tier:'\uD83E\uDD47 Gold',     risk:'Returning',  action:'Cross-sell',            disc:'5%',  ch:'Push',      tpl:'Based on your last buy\u2026' },
    { tier:'\uD83E\uDD48 Silver',   risk:'At-Risk',    action:'Nudge campaign',        disc:'5%',  ch:'SMS',       tpl:"Don't miss out\u2026" },
    { tier:'\uD83E\uDD48 Silver',   risk:'New',        action:'Onboarding series',     disc:'0%',  ch:'Email',     tpl:'Getting started with\u2026' },
    { tier:'\uD83E\uDD49 Bronze',   risk:'Reactivated',action:'Re-engagement',         disc:'8%',  ch:'Email+Push',tpl:'Come back & save\u2026' },
    { tier:'\uD83E\uDD49 Bronze',   risk:'New',        action:'Welcome offer',         disc:'5%',  ch:'Email',     tpl:"Welcome! Here's 5% off\u2026" },
  ];

  // Vendor config (loaded from database)
  vendorCfg = signal<{param: string; val: string; type: string; desc: string}[]>([]);

  ngOnInit() {
    this.loadSettings();
  }

  loadSettings() {
    this.loading.set(true);
    this.api.get<any>(`/settings?clientId=${this.clientId}`).subscribe({
      next: (cfg) => {
        this.churnWindow.set(cfg.churn_window_days ?? 90);
        this.repeatThreshold.set(cfg.min_repeat_orders ?? 2);
        this.highValuePct.set(cfg.high_value_percentile ?? 75);
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
          { param: 'churn_window_days', val: String(cfg.churn_window_days),      type: 'integer', desc: 'Days since last order before flagging as churned' },
          { param: 'min_repeat_orders', val: String(cfg.min_repeat_orders),      type: 'integer', desc: 'Min completed orders to qualify as repeat customer' },
          { param: 'high_value_pct',    val: String(cfg.high_value_percentile),  type: 'integer', desc: 'Percentile cutoff for High Value flag' },
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

  save() {
    this.saved.set(false);
    this.error.set('');

    const body = {
      churn_window_days: this.churnWindow(),
      min_repeat_orders: this.repeatThreshold(),
      high_value_percentile: this.highValuePct(),
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
