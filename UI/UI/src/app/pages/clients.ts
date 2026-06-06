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
  private api  = inject(ApiService);
  private auth = inject(AuthService);

  clients  = signal<ClientRow[]>([]);
  loading  = signal(true);
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
  showAddForm  = signal(false);
  showPassword = signal(false);
  addForm      = signal({ client_name: '', client_code: '', contact_name: '', contact_email: '', password: '' });
  addSaving    = signal(false);
  addError     = signal('');
  addSuccess   = signal<string>('');

  // ── Touched flags — one per field ─────────────────────────────────────
  addNameTouched     = signal(false);
  addCodeTouched     = signal(false);
  addContactTouched  = signal(false);
  addEmailTouched    = signal(false);
  addPassTouched     = signal(false);

  // ── Validation regexes ────────────────────────────────────────────────
  private readonly EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{3,}$/;
  private readonly CODE_RE  = /^[A-Za-z0-9]+$/;

  // ── Inline computed errors ────────────────────────────────────────────
  addNameError = computed(() => {
    if (!this.addNameTouched()) return '';
    if (!this.addForm().client_name.trim()) return 'Company name is required.';
    return '';
  });

  addCodeError = computed(() => {
    if (!this.addCodeTouched()) return '';
    const code = this.addForm().client_code.trim();
    if (!code) return 'Company code is required (e.g. TARGET, COSTCO).';
    if (code.length > 10) return 'Company code must be 10 characters or less.';
    if (!this.CODE_RE.test(code)) return 'Letters and numbers only — no special characters.';
    return '';
  });

  addContactError = computed(() => {
    if (!this.addContactTouched()) return '';
    if (!this.addForm().contact_name.trim()) return 'Contact name is required.';
    return '';
  });

  addEmailError = computed(() => {
    if (!this.addEmailTouched()) return '';
    const v = this.addForm().contact_email.trim();
    if (!v) return 'Contact email is required.';
    if (!this.EMAIL_RE.test(v)) return 'Please enter a valid email (e.g. jane@target.com).';
    return '';
  });

  addPassError = computed(() => {
    if (!this.addPassTouched()) return '';
    const p = this.addForm().password;
    if (!p) return 'Password is required.';
    if (p.length < 8) return 'Must be at least 8 characters.';
    if (!/[A-Z]/.test(p)) return 'Must contain at least one uppercase letter.';
    if (!/[0-9]/.test(p)) return 'Must contain at least one number.';
    if (!/[^A-Za-z0-9]/.test(p)) return 'Must contain at least one special character (e.g. !@#$%).';
    return '';
  });

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
  viewTable  = signal<string | null>(null);
  viewLabel  = signal<string>('');
  viewData   = signal<TableDataResponse | null>(null);
  viewLoading = signal(false);
  viewError  = signal('');
  viewOffset = signal(0);
  readonly viewLimit = 100;

  ngOnInit() { this.loadClients(); }

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
          err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not load data overview.'
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
    const table  = this.viewTable();
    if (!client || !table) return;
    this.viewLoading.set(true);
    this.viewError.set('');
    const url = `/clients/${client.client_id}/data/${table}?limit=${this.viewLimit}&offset=${this.viewOffset()}`;
    this.api.get<TableDataResponse>(url).subscribe({
      next: (data) => { this.viewData.set(data); this.viewLoading.set(false); },
      error: (err) => {
        this.viewLoading.set(false);
        this.viewError.set(err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not load data.');
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
    const end   = Math.min(d.offset + d.rows.length, d.total);
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
    // Reset all touched flags
    this.addNameTouched.set(false);
    this.addCodeTouched.set(false);
    this.addContactTouched.set(false);
    this.addEmailTouched.set(false);
    this.addPassTouched.set(false);
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

  private addFormValid(): boolean {
    const f = this.addForm();
    return (
      !!f.client_name.trim() &&
      !!f.client_code.trim() &&
      f.client_code.length <= 10 &&
      this.CODE_RE.test(f.client_code) &&
      !!f.contact_name.trim() &&
      !!f.contact_email.trim() &&
      this.EMAIL_RE.test(f.contact_email.trim()) &&
      f.password.length >= 8 &&
      /[A-Z]/.test(f.password) &&
      /[0-9]/.test(f.password) &&
      /[^A-Za-z0-9]/.test(f.password)
    );
  }

  saveNewClient() {
    // Touch all fields so inline errors appear
    this.addNameTouched.set(true);
    this.addCodeTouched.set(true);
    this.addContactTouched.set(true);
    this.addEmailTouched.set(true);
    this.addPassTouched.set(true);
    this.addError.set('');

    if (!this.addFormValid()) return;

    this.addSaving.set(true);
    this.api.post<{ client_id: string; client_name: string; message: string }>(
      '/clients/admin-create', this.addForm(),
    ).subscribe({
      next: (res) => {
        this.addSaving.set(false);
        this.addSuccess.set(
          `✅ Client "${res.client_name}" created! Client ID: ${res.client_id} · Welcome email sent to ${this.addForm().contact_email}.`
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
  cancelDelete() { this.deleteConfirmId.set(null); this.deleteError.set(''); }

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