import { Component, OnInit, signal, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';
import { TierLabelService } from '../services/tier-label.service';
import { TierLabelPipe } from '../pipes/tier-label.pipe';

interface ChurnScore {
  customer_id: string;
  customer_name: string;
  customer_email: string;
  churn_probability: number;
  risk_tier: string;
  driver_1: string;
  driver_2: string;
  driver_3: string;
  scored_at: string;
  model_version: string;
  total_orders: number;
  total_spend: number;
  avg_order_value: number;
  rfm_recency: number;
  rfm_frequency: number;
  rfm_monetary: number;
  rfm_total: number;
  tier: string;
  avg_rating: number;
  total_tickets: number;
}

interface Summary {
  total_scored: number;
  high_risk: number;
  medium_risk: number;
  low_risk: number;
  avg_probability: number;
}

/**
 * Build a CSV (header + one row per score) from the churn scores shown on the
 * page. Pure + exported so it's unit-tested; the browser download (Blob) lives
 * in the component. Columns mirror what the table displays.
 */
export function toChurnCsv(scores: ChurnScore[]): string {
  const cols: Array<[string, (s: ChurnScore) => unknown]> = [
    ['Customer ID',       s => s.customer_id],
    ['Customer Name',     s => s.customer_name],
    ['Email',             s => s.customer_email],
    ['Tier',              s => s.tier],
    ['Total Orders',      s => s.total_orders],
    ['Total Spend',       s => s.total_spend],
    ['Avg Order Value',   s => s.avg_order_value],
    ['Recency',           s => s.rfm_recency],
    ['Frequency',         s => s.rfm_frequency],
    ['Monetary',          s => s.rfm_monetary],
    ['RFM Total',         s => s.rfm_total],
    ['Churn Probability', s => s.churn_probability],
    ['Risk Tier',         s => s.risk_tier],
    ['Driver 1',          s => s.driver_1],
    ['Driver 2',          s => s.driver_2],
    ['Driver 3',          s => s.driver_3],
    ['Scored At',         s => s.scored_at],
    ['Model Version',     s => s.model_version],
  ];
  const esc = (v: unknown): string => {
    const str = (v === null || v === undefined) ? '' : String(v);
    return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
  };
  const header = cols.map(c => esc(c[0])).join(',');
  const rows = scores.map(s => cols.map(c => esc(c[1](s))).join(','));
  return [header, ...rows].join('\n');
}

@Component({
  selector: 'app-churn-scores',
  standalone: true,
  imports: [CommonModule, FormsModule, TierLabelPipe],
  templateUrl: './churn-scores.html',
  styleUrls: ['./churn-scores.scss']
})
export class ChurnScoresComponent implements OnInit {
  private api  = inject(ApiService);
  auth = inject(AuthService);
  private tierLabels = inject(TierLabelService);
  private clientId = this.auth.getClientId();

  // Data
  scores = signal<ChurnScore[]>([]);
  summary = signal<Summary>({ total_scored: 0, high_risk: 0, medium_risk: 0, low_risk: 0, avg_probability: 0 });

  // Pagination
  // pageSize = 100 per CTO direction: one long page with a vertical
  // scroller inside the table wrapper is easier to scan than clicking
  // through 4×25-row pages. The scroll container is defined in
  // .churn-scroll (churn-scores.scss) with max-height + overflow-y:auto.
  page = signal(1);
  pageSize = signal(100);
  totalRows = signal(0);
  totalPages = signal(1);

  // Filters
  riskFilter = signal<string>('');
  searchQuery = signal('');

  // State
  loading = signal(true);
  error = signal('');
  downloading = signal(false);

  // Selected customer for detail panel
  selectedCustomer = signal<ChurnScore | null>(null);

  ngOnInit() {
    // Pull client's tier labels so {{ s.tier | tierLabel }} shows custom names.
    this.tierLabels.refresh();
    this.loadScores();
  }

  loadScores() {
    this.loading.set(true);
    this.error.set('');

    let url = `/churn-scores?clientId=${this.clientId}&page=${this.page()}&pageSize=${this.pageSize()}`;
    if (this.riskFilter()) url += `&riskTier=${this.riskFilter()}`;
    if (this.searchQuery().trim()) url += `&search=${encodeURIComponent(this.searchQuery().trim())}`;

    this.api.get<any>(url).subscribe({
      next: (res) => {
        this.scores.set(res.scores);
        this.summary.set(res.summary);
        this.totalRows.set(res.totalRows);
        this.totalPages.set(res.totalPages);
        this.loading.set(false);
      },
      error: (err) => {
        this.error.set('Could not load churn scores. Run the pipeline first.');
        this.loading.set(false);
      }
    });
  }

  // Download the churn scores (respecting the active filter + search) as CSV —
  // the only export in the app now that the Downloads page is gone. Fetches all
  // matching rows (not just the visible page) so the file is complete.
  downloadCsv() {
    this.downloading.set(true);
    // The /churn-scores endpoint caps pageSize at 500, so loop pages to collect
    // the full set (e.g. 675 rows = 2 calls) rather than just the visible page.
    const PAGE_SIZE = 500;
    const base = `/churn-scores?clientId=${this.clientId}&pageSize=${PAGE_SIZE}`
      + (this.riskFilter() ? `&riskTier=${this.riskFilter()}` : '')
      + (this.searchQuery().trim() ? `&search=${encodeURIComponent(this.searchQuery().trim())}` : '');
    const all: ChurnScore[] = [];
    const fetchPage = (page: number) => {
      this.api.get<any>(`${base}&page=${page}`).subscribe({
        next: (res) => {
          all.push(...(res.scores ?? []));
          if (page < (res.totalPages ?? 1)) {
            fetchPage(page + 1);
          } else {
            const suffix = this.riskFilter() ? `_${this.riskFilter().toLowerCase()}` : '';
            this.triggerDownload(toChurnCsv(all), `churn_scores_${this.clientId}${suffix}.csv`);
            this.downloading.set(false);
          }
        },
        error: () => {
          this.downloading.set(false);
          this.error.set('Could not export scores. Please try again.');
        }
      });
    };
    fetchPage(1);
  }

  private triggerDownload(csv: string, filename: string) {
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  // Filter handlers
  filterByRisk(tier: string) {
    this.riskFilter.set(this.riskFilter() === tier ? '' : tier);
    this.page.set(1);
    this.loadScores();
  }

  onSearch() {
    this.page.set(1);
    this.loadScores();
  }

  clearSearch() {
    this.searchQuery.set('');
    this.page.set(1);
    this.loadScores();
  }

  // Pagination
  goToPage(p: number) {
    if (p < 1 || p > this.totalPages()) return;
    this.page.set(p);
    this.loadScores();
  }

  // Detail panel
  selectCustomer(c: ChurnScore) {
    this.selectedCustomer.set(this.selectedCustomer()?.customer_id === c.customer_id ? null : c);
  }

  // Formatting helpers
  riskColor(tier: string): string {
    if (tier === 'HIGH') return 'red';
    if (tier === 'MEDIUM') return 'yellow';
    return 'green';
  }

  riskIcon(tier: string): string {
    if (tier === 'HIGH') return '\uD83D\uDD34';
    if (tier === 'MEDIUM') return '\uD83D\uDFE1';
    return '\uD83D\uDFE2';
  }

  probPercent(p: number): string {
    return (p * 100).toFixed(1) + '%';
  }

  probBarWidth(p: number): string {
    return (p * 100).toFixed(0) + '%';
  }

  probBarColor(p: number): string {
    if (p >= 0.65) return '#EF4444';
    if (p >= 0.35) return '#F59E0B';
    return '#10B981';
  }

  formatCurrency(n: number): string {
    return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  formatNumber(n: number): string {
    return n.toLocaleString('en-US');
  }

  tierColor(tier: string): string {
    if (tier === 'Platinum') return 'purple';
    if (tier === 'Gold') return 'yellow';
    if (tier === 'Silver') return 'gray';
    return 'orange';
  }
}
