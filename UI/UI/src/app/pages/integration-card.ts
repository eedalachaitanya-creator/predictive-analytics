import { Component, Input, OnInit, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';

export interface ProviderMeta {
  label: string;
  fields: string[];        // which config inputs to show (base_url, email, api_token, project_key)
  strategies: string[];    // customer-link options for the dropdown
}

interface IntegrationStatus {
  configured: boolean;
  enabled: boolean;
  base_url?: string;
  email?: string;
  project_key?: string;
  customer_strategy?: string;
  customer_field_name?: string;
  token_set?: boolean;
  last_sync_at?: string | null;
  last_sync_status?: string | null;
  last_sync_detail?: string | null;
}

/**
 * Provider-agnostic integration card (one per provider on the Settings page).
 * Driven by PROVIDER_META: it renders only the fields the provider needs and
 * talks to /api/v1/integrations/{provider}. The token is WRITE-ONLY — sent on
 * Save when non-empty, never received back (the API reports token_set only).
 */
@Component({
  selector: 'app-integration-card',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './integration-card.html',
})
export class IntegrationCardComponent implements OnInit {
  @Input({ required: true }) provider!: string;
  @Input({ required: true }) meta!: ProviderMeta;
  // Hide the built-in "Sync now" (which writes straight to live tables) when the
  // card is embedded in the Upload-page modal, where syncing goes through the
  // staging batch instead. Defaults true so Settings is unchanged.
  @Input() showSync = true;

  private api = inject(ApiService);
  private auth = inject(AuthService);
  private clientId = this.auth.getClientId();

  loading = signal(true);

  // form fields
  baseUrl = signal('');
  email = signal('');
  apiToken = signal('');                  // only sent when non-empty
  projectKey = signal('');
  strategy = signal('');
  fieldName = signal('Customer ID');
  enabled = signal(false);

  // server status (read-only)
  tokenSet = signal(false);
  lastSyncAt = signal<string | null>(null);
  lastSyncStatus = signal<string | null>(null);
  lastSyncDetail = signal<string | null>(null);

  // action state
  saving = signal(false);
  saved = signal(false);
  error = signal('');
  testing = signal(false);
  testResult = signal<{ ok: boolean; text: string } | null>(null);
  syncing = signal(false);
  syncResult = signal<{ ok: boolean; text: string } | null>(null);

  // ── plain-English copy (no jargon) ──────────────────────────────────────────
  private static FIELD_LABELS: Record<string, string> = {
    base_url: 'Site URL', email: 'Account email', api_token: 'API token',
    project_key: 'Project key',
  };
  private static FIELD_PLACEHOLDERS: Record<string, string> = {
    base_url: 'https://your-site.atlassian.net', email: 'you@company.com',
    api_token: 'Paste your API token', project_key: 'e.g. KAN',
  };
  private static STRATEGY_LABELS: Record<string, string> = {
    auto: 'Automatic (recommended)', email: 'By email (recommended)',
    field: 'From a field', label: 'From a label',
  };
  private static DESCRIPTIONS: Record<string, string> = {
    jira: 'Connect your Jira to automatically pull in your customer support tickets. ' +
          'We read each ticket to spot unhappy customers and use that to improve churn ' +
          'predictions. Your Jira details are stored securely.',
    hubspot: 'Connect your HubSpot to automatically pull in support tickets and customer ' +
             'feedback. We read them to spot unhappy customers and use that to improve churn ' +
             'predictions. Your details are stored securely.',
  };

  has(field: string): boolean { return !!this.meta?.fields?.includes(field); }
  fieldLabel(f: string): string { return IntegrationCardComponent.FIELD_LABELS[f] ?? f; }
  fieldPlaceholder(f: string): string { return IntegrationCardComponent.FIELD_PLACEHOLDERS[f] ?? ''; }
  strategyLabel(s: string): string { return IntegrationCardComponent.STRATEGY_LABELS[s] ?? s; }
  get description(): string { return IntegrationCardComponent.DESCRIPTIONS[this.provider] ?? ''; }
  get showFieldName(): boolean { return this.strategy() !== 'label'; }

  ngOnInit() {
    this.strategy.set(this.meta.strategies[0] ?? '');
    this.load();
  }

  private apply(s: IntegrationStatus) {
    this.baseUrl.set(s.base_url ?? '');
    this.email.set(s.email ?? '');
    this.projectKey.set(s.project_key ?? '');
    this.strategy.set(s.customer_strategy ?? this.meta.strategies[0] ?? '');
    this.fieldName.set(s.customer_field_name ?? 'Customer ID');
    this.enabled.set(!!s.enabled);
    this.tokenSet.set(!!s.token_set);
    this.lastSyncAt.set(s.last_sync_at ?? null);
    this.lastSyncStatus.set(s.last_sync_status ?? null);
    this.lastSyncDetail.set(s.last_sync_detail ?? null);
    this.apiToken.set('');               // never prefill the secret
  }

  load() {
    this.loading.set(true);
    this.api.get<IntegrationStatus>(`/integrations/${this.provider}?clientId=${this.clientId}`).subscribe({
      next: s => { this.apply(s); this.loading.set(false); },
      error: e => { this.error.set(e.message ?? 'Could not load integration'); this.loading.set(false); },
    });
  }

  save() {
    this.saving.set(true); this.saved.set(false); this.error.set('');
    this.testResult.set(null); this.syncResult.set(null);
    const body: any = {
      base_url: this.baseUrl().trim() || null,
      email: this.email().trim() || null,
      project_key: this.projectKey().trim() || null,
      customer_strategy: this.strategy(),
      customer_field_name: this.fieldName().trim() || null,
      enabled: this.enabled(),
    };
    const tok = this.apiToken().trim();
    if (tok) body.api_token = tok;       // omit to keep the existing token
    this.api.put<IntegrationStatus>(`/integrations/${this.provider}?clientId=${this.clientId}`, body).subscribe({
      next: s => {
        this.apply(s); this.saving.set(false);
        this.saved.set(true); setTimeout(() => this.saved.set(false), 2500);
      },
      error: e => { this.error.set(e?.error?.detail ?? e.message ?? 'Failed to save'); this.saving.set(false); },
    });
  }

  test() {
    this.testing.set(true); this.testResult.set(null);
    this.api.post<any>(`/integrations/${this.provider}/test?clientId=${this.clientId}`, {}).subscribe({
      next: r => {
        this.testing.set(false);
        this.testResult.set(r.ok
          ? { ok: true, text: `Connected as ${r.account?.display_name ?? r.account?.email ?? this.meta.label}` }
          : { ok: false, text: r.error ?? 'Connection failed' });
      },
      error: e => { this.testing.set(false); this.testResult.set({ ok: false, text: e.message ?? 'Connection failed' }); },
    });
  }

  sync() {
    this.syncing.set(true); this.syncResult.set(null);
    this.api.post<any>(`/integrations/${this.provider}/sync?clientId=${this.clientId}`, {}).subscribe({
      next: r => {
        this.syncing.set(false);
        this.syncResult.set({ ok: true,
          text: `Synced ${r.tickets ?? 0} ticket(s), ${r.reviews ?? 0} review(s)` });
        this.load();                     // refresh last-sync status
      },
      error: e => { this.syncing.set(false); this.syncResult.set({ ok: false, text: e.message ?? 'Sync failed' }); },
    });
  }
}
