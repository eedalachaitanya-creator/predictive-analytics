import { Component, OnInit, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';

interface JiraStatus {
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
 * Per-tenant Jira integration card (embedded in the Settings page). Talks to
 * /api/v1/integrations/jira — the token is WRITE-ONLY: we send it on Save when
 * the field is non-empty and never receive it back (the API reports token_set
 * only). All calls are scoped to the current tenant server-side.
 */
@Component({
  selector: 'app-jira-integration',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './jira-integration.html',
})
export class JiraIntegrationComponent implements OnInit {
  private api = inject(ApiService);
  private auth = inject(AuthService);
  private clientId = this.auth.getClientId();

  loading = signal(true);

  // form fields
  baseUrl = signal('');
  email = signal('');
  apiToken = signal('');                 // only sent when non-empty
  projectKey = signal('');
  strategy = signal('auto');
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

  ngOnInit() { this.load(); }

  private apply(s: JiraStatus) {
    this.baseUrl.set(s.base_url ?? '');
    this.email.set(s.email ?? '');
    this.projectKey.set(s.project_key ?? '');
    this.strategy.set(s.customer_strategy ?? 'auto');
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
    this.api.get<JiraStatus>(`/integrations/jira?clientId=${this.clientId}`).subscribe({
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
    this.api.put<JiraStatus>(`/integrations/jira?clientId=${this.clientId}`, body).subscribe({
      next: s => {
        this.apply(s); this.saving.set(false);
        this.saved.set(true); setTimeout(() => this.saved.set(false), 2500);
      },
      error: e => { this.error.set(e.message ?? 'Failed to save'); this.saving.set(false); },
    });
  }

  test() {
    this.testing.set(true); this.testResult.set(null);
    this.api.post<any>(`/integrations/jira/test?clientId=${this.clientId}`, {}).subscribe({
      next: r => {
        this.testing.set(false);
        this.testResult.set(r.ok
          ? { ok: true, text: `Connected as ${r.account?.display_name ?? r.account?.email ?? 'Jira user'}` }
          : { ok: false, text: r.error ?? 'Connection failed' });
      },
      error: e => { this.testing.set(false); this.testResult.set({ ok: false, text: e.message ?? 'Connection failed' }); },
    });
  }

  sync() {
    this.syncing.set(true); this.syncResult.set(null);
    this.api.post<any>(`/integrations/jira/sync?clientId=${this.clientId}`, {}).subscribe({
      next: r => {
        this.syncing.set(false);
        this.syncResult.set({ ok: true, text: `Synced ${r.tickets ?? 0} ticket(s)` });
        this.load();                     // refresh last-sync status
      },
      error: e => { this.syncing.set(false); this.syncResult.set({ ok: false, text: e.message ?? 'Sync failed' }); },
    });
  }
}
