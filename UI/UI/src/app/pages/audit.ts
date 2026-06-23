import { Component, OnInit, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { UiSelectComponent, UiSelectOption } from '../components/ui-select/ui-select.component';

// ── Shape of a single audit event coming back from /api/v1/audit ────────────
// Matches `_row_to_event()` in backend/audit_router.py exactly. SYSTEM rows
// have client_id === "SYSTEM" (the backend substitutes that string when the
// underlying column is NULL).
interface AuditEvent {
  id:          number;
  ts:          string;   // ISO
  user_id:     string | null;
  user_email:  string;
  client_id:   string;
  action_type: string;
  details:     string;
  ip_address:  string;
  outcome:     'success' | 'warning' | 'failure' | string;
}

interface FilterOptions {
  clients:      { client_id: string; client_name: string }[];
  users:        string[];
  action_types: string[];
  outcomes:     string[];
}

interface AuditStats {
  events_today:    number;
  warnings:        number;
  security_alerts: number;
}

@Component({
  selector: 'app-audit',
  standalone: true,
  imports: [CommonModule, FormsModule, UiSelectComponent],
  templateUrl: './audit.html',
  styleUrls: ['./audit.scss']
})
export class AuditComponent implements OnInit {
  private http = inject(HttpClient);
  private base = environment.apiUrl;  // already includes /api/v1

  // ── UI state ───────────────────────────────────────────────────────────
  loading  = signal(false);
  error    = signal<string | null>(null);
  stats    = signal<AuditStats>({ events_today: 0, warnings: 0, security_alerts: 0 });
  options  = signal<FilterOptions>({ clients: [], users: [], action_types: [], outcomes: ['success','warning','failure'] });
  events   = signal<AuditEvent[]>([]);
  total    = signal(0);

  // ── Pagination state ───────────────────────────────────────────────────
  readonly pageSize = 50;
  page       = signal(1);
  totalPages = computed(() => Math.max(1, Math.ceil(this.total() / this.pageSize)));

  // ── Filter model — bound to the form via ngModel ───────────────────────
  // Defaults: last 30 days → today. Leaving these empty means "no bound".
  filters = signal({
    start:       this.defaultStart(),
    end:         this.today(),
    client_id:   'ALL',
    user_email:  'ALL',
    action_type: 'ALL',
    outcome:     'ALL',
  });

  // Row-count banner, e.g. "Showing 1–50 of 247 events"
  rowSummary = computed(() => {
    const t = this.total();
    if (t === 0) return 'No events match the current filters.';
    const start = (this.page() - 1) * this.pageSize + 1;
    const end   = Math.min(this.page() * this.pageSize, t);
    return `Showing ${start}–${end} of ${t} events`;
  });

  // ── Option lists for the custom <app-ui-select> filters ────────────────
  // Built from the backend filter-options payload; each leads with an "ALL"
  // sentinel that maps to no server-side constraint (see baseFilterParams()).
  clientOptions = computed<UiSelectOption[]>(() => [
    { value: 'ALL', label: 'All Clients' },
    ...this.options().clients.map(c => ({ value: c.client_id, label: `${c.client_id} · ${c.client_name}` })),
  ]);
  userOptions = computed<UiSelectOption[]>(() => [
    { value: 'ALL', label: 'All Users' },
    ...this.options().users.map(u => ({ value: u, label: u })),
  ]);
  // Combined (multi-value) filter values that mirror the backend KPI-stat
  // definitions, so a card click reproduces its count EXACTLY. The list
  // endpoint accepts these comma-separated values (= ANY(...)); they're also
  // offered as dropdown options so the bar always shows what's applied.
  readonly PROBLEM_OUTCOMES = 'warning,failure';
  readonly AUTH_ACTIONS     = 'login,logout,token_refresh';

  actionOptions = computed<UiSelectOption[]>(() => [
    { value: 'ALL', label: 'All' },
    { value: this.AUTH_ACTIONS, label: 'Auth (login / logout / token refresh)' },
    ...this.options().action_types.map(a => ({ value: a, label: this.actionLabel(a) })),
  ]);
  outcomeOptions = computed<UiSelectOption[]>(() => [
    { value: 'ALL', label: 'All' },
    { value: this.PROBLEM_OUTCOMES, label: 'Warning + Failure' },
    ...this.options().outcomes.map(o => ({ value: o, label: o.charAt(0).toUpperCase() + o.slice(1) })),
  ]);

  // ── Clickable KPI cards → today-scoped filter presets ──────────────────
  // Each card is a shortcut that POPULATES the visible filter bar (so the
  // applied filter is always transparent — no hidden mismatch with the count)
  // then re-runs the query. Clicking the active card again clears back to the
  // default 30-day view. All three card stats are scoped to "today".
  activeCard = signal<'' | 'today' | 'warnings' | 'security'>('');

  setCardPreset(card: 'today' | 'warnings' | 'security') {
    if (this.activeCard() === card) { this.resetFilters(); return; }
    const t = this.today();
    // Mirror the backend stat definitions exactly so the filtered row count
    // EQUALS the card number (pinned by test_audit_card_presets.py):
    //   warnings = outcome IN (warning, failure)
    //   security = warnings AND action_type IN (login, logout, token_refresh)
    this.filters.set({
      start: t, end: t, client_id: 'ALL', user_email: 'ALL',
      action_type: card === 'security' ? this.AUTH_ACTIONS : 'ALL',
      outcome:     card === 'today'    ? 'ALL' : this.PROBLEM_OUTCOMES,
    });
    this.activeCard.set(card);
    this.applyFilters();
  }

  // Restore the default view (last 30 days, no narrowing) — the unambiguous
  // "show me the whole log again" action, surfaced as a Reset button so it
  // isn't hidden behind re-clicking the active card.
  resetFilters() {
    this.filters.set({
      start: this.defaultStart(), end: this.today(),
      client_id: 'ALL', user_email: 'ALL', action_type: 'ALL', outcome: 'ALL',
    });
    this.activeCard.set('');
    this.applyFilters();
  }

  // True when the filter bar differs from the default view — drives whether
  // the Reset button is shown.
  isFiltered = computed(() => {
    const f = this.filters();
    return this.activeCard() !== '' ||
      f.client_id !== 'ALL' || f.user_email !== 'ALL' ||
      f.action_type !== 'ALL' || f.outcome !== 'ALL' ||
      f.start !== this.defaultStart() || f.end !== this.today();
  });

  // ── Lifecycle ──────────────────────────────────────────────────────────
  ngOnInit() {
    this.loadStats();
    this.loadFilterOptions();
    this.applyFilters();
  }

  // ── Loaders ────────────────────────────────────────────────────────────
  loadStats() {
    this.http.get<AuditStats>(`${this.base}/audit/stats`, { headers: this.authHeaders() })
      .subscribe({
        next: s   => this.stats.set(s),
        error: _  => { /* stats are non-critical — don't block the page */ }
      });
  }

  loadFilterOptions() {
    this.http.get<FilterOptions>(`${this.base}/audit/filter-options`, { headers: this.authHeaders() })
      .subscribe({
        next: o   => this.options.set(o),
        error: _  => { /* dropdowns stay empty; user can still filter by date */ }
      });
  }

  applyFilters() {
    this.page.set(1);  // reset to first page on new filter
    this.fetchPage();
  }

  fetchPage() {
    this.loading.set(true);
    this.error.set(null);
    const offset = (this.page() - 1) * this.pageSize;
    this.http.get<{ events: AuditEvent[]; total: number }>(
      `${this.base}/audit`,
      { headers: this.authHeaders(), params: this.filterParams(offset) }
    ).subscribe({
      next: resp => {
        this.events.set(resp.events);
        this.total.set(resp.total);
        this.loading.set(false);
      },
      error: e => {
        this.error.set(e?.error?.detail ?? e?.message ?? 'Could not load audit events.');
        this.events.set([]);
        this.total.set(0);
        this.loading.set(false);
      }
    });
  }

  goToPage(p: number) {
    if (p < 1 || p > this.totalPages()) return;
    this.page.set(p);
    this.fetchPage();
  }

  // ── CSV export ─────────────────────────────────────────────────────────
  exportCsv() {
    this.http.get(`${this.base}/audit/export`, {
      headers: this.authHeaders(),
      params:  this.exportParams(),
      responseType: 'blob',
      observe: 'response',
    }).subscribe({
      next: resp => {
        const blob = resp.body!;
        const url  = window.URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href     = url;
        const cd = resp.headers.get('Content-Disposition') ?? '';
        const m  = /filename="([^"]+)"/.exec(cd);
        a.download = m?.[1] ?? `audit_log_${Date.now()}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
      },
      error: e => {
        this.error.set(e?.error?.detail ?? e?.message ?? 'CSV export failed.');
      }
    });
  }

  // ── UI helpers ─────────────────────────────────────────────────────────
  onApplyClick() {
    this.applyFilters();
    this.loadStats();
  }

  formatTs(iso: string): string {
    if (!iso) return '';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ` +
           `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }

  actionLabel(a: string): string {
    const map: Record<string, string> = {
      login:             'Login',
      logout:            'Logout',
      pipeline_run:      'Pipeline Run',
      file_upload:       'File Upload',
      settings_saved:    'Settings Saved',
      client_created:    'Client Created',
      client_deleted:    'Client Deleted',
      user_updated:      'User Updated',
      user_deleted:      'User Deleted',
    };
    return map[a] ?? a;
  }

  actionColor(a: string): string {
    if (a.startsWith('pipeline'))  return 'purple';
    if (a.startsWith('file'))      return 'blue';
    if (a.startsWith('settings'))  return 'cyan';
    if (a.startsWith('client'))    return 'green';
    if (a.startsWith('user'))      return 'yellow';
    if (a === 'login' || a === 'logout') return 'gray';
    return 'blue';
  }

  updateFilter<K extends keyof ReturnType<typeof this.filters>>(key: K, val: any) {
    this.filters.update(f => ({ ...f, [key]: val }));
    this.activeCard.set('');  // a manual filter edit means we're no longer in a card preset
  }

  // ── Private helpers ────────────────────────────────────────────────────
  private authHeaders(): HttpHeaders {
    const token = sessionStorage.getItem('wap_token');
    return new HttpHeaders(token ? { Authorization: `Bearer ${token}` } : {});
  }

  private baseFilterParams(): HttpParams {
    const f = this.filters();
    let p = new HttpParams();
    if (f.start)                                  p = p.set('start',       f.start);
    if (f.end)                                    p = p.set('end',         f.end);
    if (f.client_id   && f.client_id   !== 'ALL') p = p.set('client_id',   f.client_id);
    if (f.user_email  && f.user_email  !== 'ALL') p = p.set('user_email',  f.user_email);
    if (f.action_type && f.action_type !== 'ALL') p = p.set('action_type', f.action_type);
    if (f.outcome     && f.outcome     !== 'ALL') p = p.set('outcome',     f.outcome);
    return p;
  }

  // Paginated fetch — always sends limit + offset
  private filterParams(offset = 0): HttpParams {
    return this.baseFilterParams()
      .set('limit',  String(this.pageSize))
      .set('offset', String(offset));
  }

  // Export fetch — no limit/offset so backend returns ALL matching rows
  private exportParams(): HttpParams {
    return this.baseFilterParams();
  }

  private today(): string {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  }

  private defaultStart(): string {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  }
}