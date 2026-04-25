import { Component, OnInit, computed, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';

interface ClientRow {
  client_id: string;
  client_name: string;
  client_code: string;
  created_at: string | null;
  is_active: boolean;
  deactivated_at: string | null;
}

// Shape returned by GET /clients/{id}/data-overview — one row per table.
// row_count: how many rows this client owns in that table
// last_updated: most recent timestamp (null if table has no natural ts, or empty)
interface DataOverviewRow {
  table: string;
  label: string;
  row_count: number;
  last_updated: string | null;
}

interface DataOverview {
  client_id: string;
  client_name: string;
  uploaded: DataOverviewRow[];
  generated: DataOverviewRow[];
  totals: { uploaded_rows: number; generated_rows: number };
}

// Shape returned by GET /clients/{id}/data/{table}?limit=&offset=
// columns is the DB column order; rows is an array of {col: value} objects.
interface TableDataResponse {
  table: string;
  client_id: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  limit: number;
  offset: number;
}

@Component({
  selector: 'app-clients',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './clients.html',
  styleUrls: ['./clients.scss']
})
export class ClientsComponent implements OnInit {
  private api = inject(ApiService);
  private auth = inject(AuthService);

  clients = signal<ClientRow[]>([]);
  loading = signal(true);
  selected = signal<ClientRow | null>(null);

  // Split the list so the stat cards can show Active vs Inactive counts.
  activeClients   = computed(() => this.clients().filter(c => c.is_active));
  inactiveClients = computed(() => this.clients().filter(c => !c.is_active));

  // Which bucket the table is showing. The three stat cards (Total / Active /
  // Inactive) act as filter tabs; clicking one flips this and the table
  // re-renders via visibleClients(). Defaults to 'active' so the page keeps
  // its existing behaviour of hiding deleted clients until the admin asks.
  statusFilter = signal<'all' | 'active' | 'inactive'>('active');

  visibleClients = computed<ClientRow[]>(() => {
    switch (this.statusFilter()) {
      case 'active':   return this.activeClients();
      case 'inactive': return this.inactiveClients();
      default:         return this.clients();
    }
  });

  setStatusFilter(f: 'all' | 'active' | 'inactive') {
    this.statusFilter.set(f);
    // If the currently-selected detail panel is no longer in the visible
    // list, clear it so the detail panel doesn't mismatch the table.
    const sel = this.selected();
    if (sel && !this.visibleClients().some(c => c.client_id === sel.client_id)) {
      const fallback = this.visibleClients()[0] ?? null;
      if (fallback) this.selectClient(fallback);
      else { this.selected.set(null); this.overview.set(null); }
    }
  }

  // Only super admins can create or delete clients. Everyone else sees the
  // page read-only (View buttons still work — they'll just not see the
  // "+ Add New Client" button or the trash icons).
  readonly isSuperAdmin = this.auth.isSuperAdmin;

  // ── Add-client form state ────────────────────────────────────────────
  // Mirrors the public self-registration form. showAddForm collapses/expands
  // the form below the Refresh button (same UX pattern as the Users page).
  showAddForm   = signal(false);
  addForm       = signal({ client_name: '', client_code: '', contact_name: '', contact_email: '', password: '' });
  addSaving     = signal(false);
  addError      = signal('');
  addSuccess    = signal<string>('');   // success banner after a client is created

  // ── Delete-client state ──────────────────────────────────────────────
  // Two-step delete: first click sets deleteConfirmId; second click actually
  // deletes. Anywhere-else click clears it. This prevents accidental wipes
  // of an entire tenant's data.
  deleteConfirmId = signal<string | null>(null);
  deleting        = signal(false);
  deleteError     = signal('');

  // ── Data-overview state ──────────────────────────────────────────────
  // overview:        the latest payload from /clients/{id}/data-overview
  // overviewLoading: true while that call is in flight (shows a spinner
  //                  instead of stale data in the detail panel)
  // overviewError:   error string to render if the call fails
  overview        = signal<DataOverview | null>(null);
  overviewLoading = signal(false);
  overviewError   = signal('');

  // ── Per-table data-viewer modal state ────────────────────────────────
  // When the super admin clicks "View" on a specific dataset row (e.g.,
  // Customers), we pop open a modal and fetch a page of actual rows.
  //
  // viewTable   : the DB table name currently being viewed (null = closed)
  // viewLabel   : friendly name for the modal title (e.g., "Customers")
  // viewData    : the latest response from /clients/{id}/data/{table}
  // viewOffset  : current page offset for pagination
  // viewLimit   : rows per page — bumped from 50 → 100 per CTO direction.
  //               The .data-viewer-scroll wrapper already has max-height:60vh
  //               + overflow:auto + sticky headers, so 100 rows scroll inside
  //               the modal instead of enlarging it.
  viewTable    = signal<string | null>(null);
  viewLabel    = signal<string>('');
  viewData     = signal<TableDataResponse | null>(null);
  viewLoading  = signal(false);
  viewError    = signal('');
  viewOffset   = signal(0);
  readonly viewLimit = 100;

  ngOnInit() {
    this.loadClients();
  }

  loadClients() {
    this.loading.set(true);
    // Super admins need the deactivated rows too so the Inactive counter
    // and Total card reflect reality. Other roles always get active-only
    // (the backend ignores includeInactive for non-admins).
    const qs = this.isSuperAdmin() ? '?includeInactive=true' : '';
    this.api.get<ClientRow[]>(`/clients${qs}`).subscribe({
      next: (data) => {
        this.clients.set(data);
        this.loading.set(false);
        // Auto-select first active client for detail view.
        const firstActive = data.find(c => c.is_active) ?? null;
        if (firstActive) {
          this.selectClient(firstActive);
        }
      },
      error: () => {
        this.loading.set(false);
      }
    });
  }

  selectClient(c: ClientRow) {
    this.selected.set(c);
    this.fetchOverview(c.client_id);
  }

  // Pull fresh counts + timestamps for this client. Called on every View
  // click so the super admin sees current numbers, not a cached snapshot.
  fetchOverview(clientId: string) {
    this.overview.set(null);
    this.overviewError.set('');
    this.overviewLoading.set(true);
    this.api.get<DataOverview>(`/clients/${clientId}/data-overview`).subscribe({
      next: (data) => {
        this.overview.set(data);
        this.overviewLoading.set(false);
      },
      error: (err) => {
        this.overviewLoading.set(false);
        this.overviewError.set(
          err?.error?.detail ??
          err?.error?.message ??
          err?.message ??
          'Could not load data overview for this client.'
        );
      }
    });
  }

  // Manual refresh button next to the detail panel header.
  refreshOverview() {
    const s = this.selected();
    if (s) this.fetchOverview(s.client_id);
  }

  formatDate(iso: string | null): string {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
  }

  // Friendlier timestamp for the "Last updated" column — shows date+time so
  // the super admin can tell "this was computed 2 hours ago" vs "stale from
  // last month" at a glance.
  formatTimestamp(iso: string | null): string {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;  // backend sent a non-ISO string; show raw
    return d.toLocaleString('en-US', {
      year: 'numeric', month: 'short', day: 'numeric',
      hour: 'numeric', minute: '2-digit'
    });
  }

  // Thousands-separated row counts — tens-of-thousands of customers read
  // much easier as "45,231" than "45231".
  formatCount(n: number): string {
    return (n ?? 0).toLocaleString('en-US');
  }

  // ── Per-table data viewer ──────────────────────────────────────────────
  // Open the modal for a specific dataset (e.g., Customers). Resets
  // pagination to page 1 so each fresh "View" click starts at the top.
  openDataView(table: string, label: string) {
    this.viewTable.set(table);
    this.viewLabel.set(label);
    this.viewOffset.set(0);
    this.viewData.set(null);
    this.viewError.set('');
    this.fetchTableRows();
  }

  closeDataView() {
    this.viewTable.set(null);
    this.viewLabel.set('');
    this.viewData.set(null);
    this.viewError.set('');
    this.viewOffset.set(0);
    this.viewLoading.set(false);
  }

  // Pull the current page. Shared by openDataView + pagination buttons.
  fetchTableRows() {
    const client = this.selected();
    const table = this.viewTable();
    if (!client || !table) return;

    this.viewLoading.set(true);
    this.viewError.set('');
    const url = `/clients/${client.client_id}/data/${table}?limit=${this.viewLimit}&offset=${this.viewOffset()}`;
    this.api.get<TableDataResponse>(url).subscribe({
      next: (data) => {
        this.viewData.set(data);
        this.viewLoading.set(false);
      },
      error: (err) => {
        this.viewLoading.set(false);
        this.viewError.set(
          err?.error?.detail ??
          err?.error?.message ??
          err?.message ??
          'Could not load data for this table.'
        );
      }
    });
  }

  nextPage() {
    const d = this.viewData();
    if (!d) return;
    const next = this.viewOffset() + this.viewLimit;
    if (next >= d.total) return;  // already on last page
    this.viewOffset.set(next);
    this.fetchTableRows();
  }

  prevPage() {
    const prev = Math.max(0, this.viewOffset() - this.viewLimit);
    this.viewOffset.set(prev);
    this.fetchTableRows();
  }

  // "1–50 of 200" style pagination label.
  paginationLabel(): string {
    const d = this.viewData();
    if (!d || d.total === 0) return 'No rows';
    const start = d.offset + 1;
    const end = Math.min(d.offset + d.rows.length, d.total);
    return `${start}–${end} of ${this.formatCount(d.total)}`;
  }

  // Render a cell value. Booleans become ✓/em-dash, null becomes em-dash,
  // objects get JSON-stringified. Integers get a thousands separator;
  // decimals pass through (we don't want to lose precision on a 0.78
  // churn probability or a 4.5 rating). Date-looking strings pass through
  // since backend already ISO-formatted them.
  renderCell(value: unknown): string {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'boolean') return value ? '✓' : '—';
    if (typeof value === 'number') {
      if (Number.isInteger(value)) return value.toLocaleString('en-US');
      // Trim trailing zeros on decimals: 0.7800 → 0.78
      return String(Number(value.toFixed(4)));
    }
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  // ── Add a new client ──────────────────────────────────────────────────
  // Opens the collapsible form. Called from the "+ Add New Client" button.
  openAddForm() {
    this.addForm.set({ client_name: '', client_code: '', contact_name: '', contact_email: '', password: '' });
    this.addError.set('');
    this.addSuccess.set('');
    this.showAddForm.set(true);
  }

  closeAddForm() {
    this.showAddForm.set(false);
    this.addError.set('');
    this.addSaving.set(false);
  }

  // Used by each [(ngModel)] input in the Add form to patch one field of
  // the addForm signal — keeps the signal immutable instead of mutating.
  updateAddField(field: 'client_name' | 'client_code' | 'contact_name' | 'contact_email' | 'password', value: string) {
    this.addForm.update(f => ({ ...f, [field]: value }));
  }

  saveNewClient() {
    const f = this.addForm();

    // Client-side guardrails. The backend validates too, but catching here
    // means the user sees the error immediately without a round-trip.
    if (!f.client_name.trim())  { this.addError.set('Company name is required.'); return; }
    if (!f.client_code.trim())  { this.addError.set('Company code is required.'); return; }
    if (f.client_code.length > 10) { this.addError.set('Company code must be 10 characters or less.'); return; }
    if (!f.contact_name.trim()) { this.addError.set('Contact name is required.'); return; }
    if (!f.contact_email.trim() || !f.contact_email.includes('@')) {
      this.addError.set('A valid contact email is required.'); return;
    }
    if (f.password.length < 6) { this.addError.set('Password must be at least 6 characters.'); return; }

    this.addSaving.set(true);
    this.addError.set('');
    this.api.post<{ client_id: string; client_name: string; message: string }>(
      '/clients/admin-create',
      f,
    ).subscribe({
      next: (res) => {
        this.addSaving.set(false);
        // Show the new client_id so the super admin can share it with the
        // tenant. Leaving the form collapsed and reloading the list is more
        // explicit than silently closing.
        this.addSuccess.set(
          `✅ ${res.client_name} created (${res.client_id}). User ${f.contact_email} can now sign in.`
        );
        this.showAddForm.set(false);
        this.loadClients();  // refresh the table so the new row shows up
      },
      error: (err) => {
        this.addSaving.set(false);
        this.addError.set(
          err?.error?.detail ??
          err?.error?.message ??
          err?.message ??
          'Could not create client. Please try again.'
        );
      }
    });
  }

  // ── Delete a client ───────────────────────────────────────────────────
  // Two-click confirmation: first click arms the row, second click fires
  // the DELETE. Any other click (elsewhere on the trash icon, another row's
  // delete, etc.) replaces or clears the arm state.
  confirmDelete(clientId: string) {
    this.deleteError.set('');
    this.deleteConfirmId.set(clientId);
  }

  cancelDelete() {
    this.deleteConfirmId.set(null);
    this.deleteError.set('');
  }

  deleteClient(clientId: string) {
    this.deleting.set(true);
    this.deleteError.set('');
    this.api.delete<{ client_id: string; deleted: Record<string, number>; message: string }>(
      `/clients/${clientId}`,
    ).subscribe({
      next: () => {
        this.deleting.set(false);
        this.deleteConfirmId.set(null);

        // If we just deleted the currently-selected client, clear the panel
        // so we don't leave a stale overview sitting there.
        if (this.selected()?.client_id === clientId) {
          this.selected.set(null);
          this.overview.set(null);
        }
        this.loadClients();
      },
      error: (err) => {
        this.deleting.set(false);
        this.deleteError.set(
          err?.error?.detail ??
          err?.error?.message ??
          err?.message ??
          'Could not delete client.'
        );
      }
    });
  }
}
