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
  address: string | null;
  city: string | null;
  state_province: string | null;
  postal_code: string | null;
  country: string | null;
  contact_email: string | null;
  company_phone: string | null;
}

interface DataOverviewRow {
  table: string;
  label: string;
  row_count: number;
  last_updated: string | null;
  uploaded_at: string | null;
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

type AddField =
  | 'organization_name' | 'address' | 'city' | 'state_province'
  | 'postal_code' | 'country' | 'company_contact_email' | 'company_phone'
  | 'admin_name' | 'admin_phone' | 'admin_email' | 'password';

const COUNTRIES: string[] = [
  'Argentina', 'Australia', 'Austria', 'Bangladesh', 'Belgium', 'Brazil',
  'Bulgaria', 'Canada', 'Chile', 'China', 'Colombia', 'Croatia', 'Czechia',
  'Denmark', 'Egypt', 'Estonia', 'Finland', 'France', 'Germany', 'Ghana',
  'Greece', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Ireland',
  'Israel', 'Italy', 'Japan', 'Jordan', 'Kenya', 'Kuwait', 'Latvia', 'Lithuania',
  'Luxembourg', 'Malaysia', 'Mexico', 'Morocco', 'Netherlands', 'New Zealand',
  'Nigeria', 'Norway', 'Oman', 'Pakistan', 'Peru', 'Philippines', 'Poland',
  'Portugal', 'Qatar', 'Romania', 'Saudi Arabia', 'Singapore', 'Slovakia',
  'Slovenia', 'South Africa', 'South Korea', 'Spain', 'Sri Lanka', 'Sweden',
  'Switzerland', 'Taiwan', 'Thailand', 'Turkey', 'Ukraine',
  'United Arab Emirates', 'United Kingdom', 'United States', 'Uruguay',
  'Vietnam', 'Other',
];

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
  showClientModal = signal(false);

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
  }

  readonly isSuperAdmin = this.auth.isSuperAdmin;

  showAddForm  = signal(false);
  showPassword = signal(false);
  addForm      = signal<Record<AddField, string>>({
    organization_name: '', address: '', city: '', state_province: '',
    postal_code: '', country: '', company_contact_email: '', company_phone: '',
    admin_name: '', admin_phone: '', admin_email: '', password: '',
  });
  addTouched   = signal<Partial<Record<AddField, boolean>>>({});
  addSaving    = signal(false);
  addError     = signal('');
  addSuccess   = signal<string>('');

  readonly countries = COUNTRIES;

  private readonly EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
  private readonly PHONE_RE = /^\d{10,12}$/;

  private computeAddErrors(f: Record<AddField, string>): Partial<Record<AddField, string>> {
    const e: Partial<Record<AddField, string>> = {};

    const req = (k: AddField, label: string, max = 0) => {
      if (!f[k].trim()) { e[k] = `${label} is required.`; return; }
      if (max && f[k].trim().length > max) e[k] = `${label} must be ${max} characters or fewer.`;
    };

    req('organization_name', 'Organization name', 100);
    req('address', 'Address', 255);
    req('city', 'City', 100);
    req('state_province', 'State / Province', 100);
    req('postal_code', 'Zip / Postal code', 20);
    req('country', 'Country');
    req('admin_name', 'Admin name', 100);

    const cce = f.company_contact_email.trim();
    if (!cce || !this.EMAIL_RE.test(cce)) e.company_contact_email = 'Enter a valid company contact email.';
    const ae = f.admin_email.trim();
    if (!ae || !this.EMAIL_RE.test(ae)) e.admin_email = 'Enter a valid admin login email.';

    const cp = f.company_phone.trim();
    if (!cp) e.company_phone = 'Company phone is required.';
    else if (!this.PHONE_RE.test(cp)) e.company_phone = 'Phone must be 10–12 digits.';
    const ap = f.admin_phone.trim();
    if (!ap) e.admin_phone = 'Admin phone is required.';
    else if (!this.PHONE_RE.test(ap)) e.admin_phone = 'Phone must be 10–12 digits.';

    const p = f.password;
    if (p && p !== p.trim())
      e.password = 'Password cannot start or end with a space.';
    else if (!(p.length >= 8 && /[A-Z]/.test(p) && /[a-z]/.test(p) && /\d/.test(p) && /[^A-Za-z0-9]/.test(p)))
      e.password = 'Min 8 characters with an uppercase letter, a number, and a special character.';
    return e;
  }

  addErrors = computed<Partial<Record<AddField, string>>>(() => {
    const all = this.computeAddErrors(this.addForm());
    const touched = this.addTouched();
    const shown: Partial<Record<AddField, string>> = {};
    (Object.keys(all) as AddField[]).forEach(k => { if (touched[k]) shown[k] = all[k]!; });
    return shown;
  });

  addFormValid = computed(() => Object.keys(this.computeAddErrors(this.addForm())).length === 0);

  deleteConfirmId = signal<string | null>(null);
  deleting        = signal(false);
  deleteError     = signal('');

  reactivateConfirmId = signal<string | null>(null);
  reactivating        = signal(false);
  reactivateError     = signal('');

  overview        = signal<DataOverview | null>(null);
  overviewLoading = signal(false);
  overviewError   = signal('');

  viewTable   = signal<string | null>(null);
  viewLabel   = signal<string>('');
  viewData    = signal<TableDataResponse | null>(null);
  viewLoading = signal(false);
  viewError   = signal('');
  viewOffset  = signal(0);
  readonly viewLimit = 100;

  // ── Pagination computeds ────────────────────────────────────────
  // Derived from viewData so the template never does arithmetic inline.
  viewCurrentPage = computed(() => {
    const d = this.viewData();
    if (!d || d.total === 0) return 1;
    return Math.floor(d.offset / this.viewLimit) + 1;
  });

  viewTotalPages = computed(() => {
    const d = this.viewData();
    if (!d || d.total === 0) return 1;
    return Math.ceil(d.total / this.viewLimit);
  });

  viewHasPrev = computed(() => this.viewOffset() > 0);

  viewHasNext = computed(() => {
    const d = this.viewData();
    return !!d && (this.viewOffset() + this.viewLimit) < d.total;
  });

  ngOnInit() { this.loadClients(); }

  loadClients() {
    this.loading.set(true);
    const qs = this.isSuperAdmin() ? '?includeInactive=true' : '';
    this.api.get<ClientRow[]>(`/clients${qs}`).subscribe({
      next: (data) => { this.clients.set(data); this.loading.set(false); },
      error: () => { this.loading.set(false); }
    });
  }

  selectClient(c: ClientRow) {
    this.selected.set(c);
    this.fetchOverview(c.client_id);
  }

  viewClient(c: ClientRow) {
    this.selectClient(c);
    this.showClientModal.set(true);
  }

  closeClientModal() {
    this.showClientModal.set(false);
    this.closeDataView();
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

  addressLines(c: ClientRow): string[] {
    const lines: string[] = [];
    if (c.address?.trim()) lines.push(c.address.trim());
    const cityZip = [
      c.city?.trim(),
      [c.state_province?.trim(), c.postal_code?.trim()].filter(Boolean).join(' '),
    ].filter(Boolean).join(', ');
    if (cityZip) lines.push(cityZip);
    if (c.country?.trim()) lines.push(c.country.trim());
    return lines;
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
    if (!this.viewHasNext()) return;
    this.viewOffset.set(this.viewOffset() + this.viewLimit);
    this.fetchTableRows();
  }

  prevPage() {
    if (!this.viewHasPrev()) return;
    this.viewOffset.set(Math.max(0, this.viewOffset() - this.viewLimit));
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
    if (typeof value === 'string') {
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(value)) return this.formatTimestamp(value);
      return value;
    }
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  formatColumnName(col: string): string {
    if (!col) return col;
    const acronyms = new Set(['id', 'usd', 'rfm', 'ltv', 'sku', 'api', 'url', 'csv', 'db', 'pv']);
    return col
      .split('_')
      .map(w => !w ? w
        : acronyms.has(w.toLowerCase()) ? w.toUpperCase()
        : w.charAt(0).toUpperCase() + w.slice(1))
      .join(' ');
  }

  openAddForm() {
    this.addForm.set({
      organization_name: '', address: '', city: '', state_province: '',
      postal_code: '', country: '', company_contact_email: '', company_phone: '',
      admin_name: '', admin_phone: '', admin_email: '', password: '',
    });
    this.addTouched.set({});
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

  updateAddField(field: AddField, value: string) {
    this.addForm.update(f => ({ ...f, [field]: value }));
  }

  touchAdd(field: AddField) {
    this.addTouched.update(t => ({ ...t, [field]: true }));
  }

  private touchAllAdd() {
    const all: Partial<Record<AddField, boolean>> = {};
    (Object.keys(this.addForm()) as AddField[]).forEach(k => { all[k] = true; });
    this.addTouched.set(all);
  }

  private extractErrorMessage(err: any): string {
    const detail = err?.error?.detail;
    if (typeof detail === 'string' && detail.trim()) return detail.trim();
    if (Array.isArray(detail) && detail.length > 0) {
      return detail.map((d: any) => d?.msg ?? JSON.stringify(d)).join(' · ');
    }
    const msg = err?.error?.message ?? err?.message;
    if (typeof msg === 'string' && msg.trim()) return msg.trim();
    return 'Could not create client. Please try again.';
  }

  saveNewClient() {
    this.touchAllAdd();
    this.addError.set('');
    if (!this.addFormValid()) return;

    this.addSaving.set(true);
    this.api.post<{ client_id: string; client_name: string; message: string }>(
      '/clients/admin-create', this.addForm(),
    ).subscribe({
      next: (res) => {
        this.addSaving.set(false);
        this.addSuccess.set(
          `✅ Client "${res.client_name}" created! Client ID: ${res.client_id} · Invite email sent to ${this.addForm().admin_email}.`
        );
        this.showAddForm.set(false);
        this.showPassword.set(false);
        this.loadClients();
      },
      error: (err) => {
        this.addSaving.set(false);
        this.addError.set(this.extractErrorMessage(err));
      }
    });
  }

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