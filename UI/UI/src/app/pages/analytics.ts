import { Component, OnInit, inject, computed, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AnalyticsService } from '../services/analytics.service';
import { KpiDrilldown } from '../services/dashboard.service';

const CLIENT_COLORS = [
  'linear-gradient(90deg,#0071CE,#0099FF)',
  'linear-gradient(90deg,#EF4444,#DC2626)',
  'linear-gradient(90deg,#10B981,#059669)',
  'linear-gradient(90deg,#8B5CF6,#6D28D9)',
  'linear-gradient(90deg,#F59E0B,#D97706)',
];

@Component({
  selector: 'app-analytics',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './analytics.html',
  styleUrls: ['./analytics.scss']
})
export class AnalyticsComponent implements OnInit {
  svc = inject(AnalyticsService);

  kpis    = computed(() => this.svc.data()?.platformKpis);
  clients = computed(() =>
    (this.svc.data()?.clientMetrics ?? [])
      .map((c, i) => ({ ...c, color: c.color || CLIENT_COLORS[i % CLIENT_COLORS.length] }))
  );

  // Max values for bar scaling
  maxCustomers = computed(() => Math.max(...this.clients().map(c => c.customers), 1));
  maxOrders    = computed(() => Math.max(...this.clients().map(c => c.orders), 1));
  maxHV        = computed(() => Math.max(...this.clients().map(c => c.highValue), 1));

  custPct  = (c: number) => Math.round((c / this.maxCustomers()) * 100);
  orderPct = (c: number) => Math.round((c / this.maxOrders())    * 100);
  hvPct    = (c: number) => Math.round((c / this.maxHV())        * 100);
  churnPct = (pct: number) => Math.round(pct);

  ngOnInit() {
    this.svc.load().subscribe({ error: () => {} });
  }

  refresh() { this.svc.load().subscribe({ error: () => {} }); }

  // ── Summary-card drill-down modal ──────────────────────────────────────
  // Clicking a platform KPI tile opens a paginated, cross-client list of the
  // records behind it. Mirrors the Dashboard drill-down: generic columns/rows
  // table, 100 rows/page, vertical scroll inside the modal.
  viewCard    = signal<string | null>(null);
  viewLabel   = signal('');
  viewData    = signal<KpiDrilldown | null>(null);
  viewLoading = signal(false);
  viewError   = signal('');
  viewOffset  = signal(0);
  readonly viewLimit = 100;

  viewHasPrev = computed(() => this.viewOffset() > 0);
  viewHasNext = computed(() => {
    const d = this.viewData();
    return !!d && (this.viewOffset() + this.viewLimit) < d.total;
  });

  openCardView(card: string, label: string) {
    this.viewCard.set(card);
    this.viewLabel.set(label);
    this.viewOffset.set(0);
    this.viewData.set(null);
    this.viewError.set('');
    this.fetchCardRows();
  }

  closeCardView() {
    this.viewCard.set(null);
    this.viewLabel.set('');
    this.viewData.set(null);
    this.viewError.set('');
    this.viewOffset.set(0);
    this.viewLoading.set(false);
  }

  fetchCardRows() {
    const card = this.viewCard();
    if (!card) return;
    this.viewLoading.set(true);
    this.viewError.set('');
    this.svc.loadKpiDrilldown(card, this.viewOffset(), this.viewLimit).subscribe({
      next: (data) => { this.viewData.set(data); this.viewLoading.set(false); },
      error: (err) => {
        this.viewLoading.set(false);
        this.viewError.set(err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not load data.');
      }
    });
  }

  cardNextPage() {
    if (!this.viewHasNext()) return;
    this.viewOffset.set(this.viewOffset() + this.viewLimit);
    this.fetchCardRows();
  }

  cardPrevPage() {
    if (!this.viewHasPrev()) return;
    this.viewOffset.set(Math.max(0, this.viewOffset() - this.viewLimit));
    this.fetchCardRows();
  }

  cardPaginationLabel(): string {
    const d = this.viewData();
    if (!d || d.total === 0) return 'No rows';
    const start = d.offset + 1;
    const end   = Math.min(d.offset + d.rows.length, d.total);
    return `${start}–${end} of ${d.total.toLocaleString('en-US')}`;
  }

  renderCell(value: unknown): string {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'boolean') return value ? '✓' : '—';
    if (typeof value === 'number') {
      if (Number.isInteger(value)) return value.toLocaleString('en-US');
      return String(Number(value.toFixed(4)));
    }
    if (typeof value === 'string') {
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(value)) {
        const d = new Date(value);
        return isNaN(d.getTime()) ? value
          : d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
      }
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
}
