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

  activeClients   = computed(() => this.clients().filter(c => c.is_active));
  inactiveClients = computed(() => this.clients().filter(c => !c.is_active));

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
    const sel = this.selected();
    if (sel && !this.visibleClients().some(c => c.client_id === sel.client_id)) {
      const fallback = this.visibleClients()[0] ?? null;
      if (fallback) this.selectClient(fallback);
      else { this.selected.set(null); this.overview.set(null); }
    }
  }

  readonly isSuperAdmin = this.auth.isSuperAdmin;

  // ── Add-client form state ─────────────────────────────────────────────
  showAddForm   = signal(false);
  showPassword  = signal(false);   // eye toggle for the password field
  addForm       = signal({ client_name: '', client_code: '', contact_name: '', contact_email: '', password: '' });
  addSaving     = signal(false);
  addError      = signal('');
  addSuccess    = signal<string>('');

  // ── Delete-client state ───────────────────────────────────────────────
  deleteConfirmId = signal<string | null>(null);
  deleting        = signal(false);
  deleteError     = signal('');

  // ── Reactivate state ──────────────────────────────────────────────────
  reactivateConfirmId = signal<string | null>(null);
  reactivating        = signal(false);
  reactivateError     = signal('');

  // ── Data-overview state ───────────────────────────────────────────────
  overview        = signal<DataOverview | null>(null);
  overviewLoading = signal(false);
  overviewError   = signal('');

  // ── Per-table data-viewer modal state ─────────────────────────────────
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
    const qs = this.isSuperAdmin() ? '?includeInactive=true' : '';
    this.api.get<ClientRow[]>(`/clients${qs}`).subscribe({
      next: (data) => {
        this.clients.set(data);
        this.loading.set(false);
        const firstActive = data.find(c => c.is_active) ?? null;
        if (firstActive) this.selectClient(firstActive);
      },
      error: () => { this.loading.set(false); }
    });
  }

  selectClient(c: ClientRow) {
    this.selected.set(c);
    this.fetchOverview(c.client_id);
  }

  fetchOverview(clientId: string) {
    this.overview.set(null);
    this.overviewError.set('');
    this.overviewLoading.set(true);
    this.api.get<DataOverview>(`/clients/${clientId}/data-overview`).subscribe({
      next: (data) => { this.overview.set(data); this.overviewLoading.set(false); },
      error: (err) => {
        this.overviewLoading.set(false);
        this.overviewError.set(
          err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not load data overview for this client.'
        );
      }
    });
  }

  refreshOverview() {
    const s = this.selected();
    if (s) this.fetchOverview(s.client_id);
  }

  formatDate(iso: string | null): string {
    if (!iso) return '—';
    return new Date(iso).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
  }

  formatTimestamp(iso: string | null): string {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleString('en-US', { year: 'numeric', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
  }

  formatCount(n: number): string {
    return (n ?? 0).toLocaleString('en-US');
  }

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

  fetchTableRows() {
    const client = this.selected();
    const table = this.viewTable();
    if (!client || !table) return;
    this.viewLoading.set(true);
    this.viewError.set('');
    const url = `/clients/${client.client_id}/data/${table}?limit=${this.viewLimit}&offset=${this.viewOffset()}`;
    this.api.get<TableDataResponse>(url).subscribe({
      next: (data) => { this.viewData.set(data); this.viewLoading.set(false); },
      error: (err) => {
        this.viewLoading.set(false);
        this.viewError.set(err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not load data for this table.');
      }
    });
  }

  nextPage() {
    const d = this.viewData();
    if (!d) return;
    const next = this.viewOffset() + this.viewLimit;
    if (next >= d.total) return;
    this.viewOffset.set(next);
    this.fetchTableRows();
  }

  prevPage() {
    const prev = Math.max(0, this.viewOffset() - this.viewLimit);
    this.viewOffset.set(prev);
    this.fetchTableRows();
  }

  paginationLabel(): string {
    const d = this.viewData();
    if (!d || d.total === 0) return 'No rows';
    const start = d.offset + 1;
    const end = Math.min(d.offset + d.rows.length, d.total);
    return `${start}–${end} of ${this.formatCount(d.total)}`;
  }

  renderCell(value: unknown): string {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'boolean') return value ? '✓' : '—';
    if (typeof value === 'number') {
      if (Number.isInteger(value)) return value.toLocaleString('en-US');
      return String(Number(value.toFixed(4)));
    }
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  // ── Add a new client ──────────────────────────────────────────────────
  openAddForm() {
    this.addForm.set({ client_name: '', client_code: '', contact_name: '', contact_email: '', password: '' });
    this.addError.set('');
    this.addSuccess.set('');
    this.showPassword.set(false);
    this.showAddForm.set(true);
  }

  closeAddForm() {
    this.showAddForm.set(false);
    this.addError.set('');
    this.addSaving.set(false);
    this.showPassword.set(false);
  }

  updateAddField(field: 'client_name' | 'client_code' | 'contact_name' | 'contact_email' | 'password', value: string) {
    this.addForm.update(f => ({ ...f, [field]: value }));
  }

  saveNewClient() {
    const f = this.addForm();

    if (!f.client_name.trim())  { this.addError.set('Company name is required.'); return; }
    if (!f.client_code.trim())  { this.addError.set('Company code is required.'); return; }
    if (f.client_code.length > 10) { this.addError.set('Company code must be 10 characters or less.'); return; }
    if (!f.contact_name.trim()) { this.addError.set('Contact name is required.'); return; }
    if (!f.contact_email.trim() || !f.contact_email.includes('@')) {
      this.addError.set('A valid contact email is required.'); return;
    }
    if (f.password.length < 8) {
      this.addError.set('Password must be at least 8 characters.'); return;
    }
    if (!/[A-Z]/.test(f.password)) {
      this.addError.set('Password must contain at least one uppercase letter.'); return;
    }
    if (!/[0-9]/.test(f.password)) {
      this.addError.set('Password must contain at least one number.'); return;
    }
    if (!/[^A-Za-z0-9]/.test(f.password)) {
      this.addError.set('Password must contain at least one special character (e.g. !@#$%).'); return;
    }

    this.addSaving.set(true);
    this.addError.set('');
    this.api.post<{ client_id: string; client_name: string; message: string }>(
      '/clients/admin-create', f,
    ).subscribe({
      next: (res) => {
        this.addSaving.set(false);
        this.addSuccess.set(
          `✅ ${res.client_name} created (${res.client_id}). A welcome email has been sent to ${f.contact_email}.`
        );
        this.showAddForm.set(false);
        this.showPassword.set(false);
        this.loadClients();
      },
      error: (err) => {
        this.addSaving.set(false);
        this.addError.set(
          err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not create client. Please try again.'
        );
      }
    });
  }

  // ── Delete ────────────────────────────────────────────────────────────
  confirmDelete(clientId: string) { this.deleteError.set(''); this.deleteConfirmId.set(clientId); }
  cancelDelete()  { this.deleteConfirmId.set(null); this.deleteError.set(''); }

  deleteClient(clientId: string) {
    this.deleting.set(true);
    this.deleteError.set('');
    this.api.delete<{ client_id: string; deleted: Record<string, number>; message: string }>(
      `/clients/${clientId}`,
    ).subscribe({
      next: () => {
        this.deleting.set(false);
        this.deleteConfirmId.set(null);
        if (this.selected()?.client_id === clientId) { this.selected.set(null); this.overview.set(null); }
        this.loadClients();
      },
      error: (err) => {
        this.deleting.set(false);
        this.deleteError.set(err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not delete client.');
      }
    });
  }

  // ── Reactivate ────────────────────────────────────────────────────────
  confirmReactivate(clientId: string) { this.reactivateError.set(''); this.reactivateConfirmId.set(clientId); }
  cancelReactivate() { this.reactivateConfirmId.set(null); this.reactivateError.set(''); }

  reactivateClient(clientId: string) {
    this.reactivating.set(true);
    this.reactivateError.set('');
    this.api.post<{ client_id: string; message: string }>(
      `/clients/${clientId}/reactivate`, {},
    ).subscribe({
      next: () => { this.reactivating.set(false); this.reactivateConfirmId.set(null); this.loadClients(); },
      error: (err) => {
        this.reactivating.set(false);
        this.reactivateError.set(err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not reactivate client.');
      }
    });
  }
}