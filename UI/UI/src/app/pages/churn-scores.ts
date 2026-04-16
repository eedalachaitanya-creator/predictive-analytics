import { Component, OnInit, signal, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';

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

@Component({
  selector: 'app-churn-scores',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './churn-scores.html',
  styleUrls: ['./churn-scores.scss']
})
export class ChurnScoresComponent implements OnInit {
  private api = inject(ApiService);
  auth = inject(AuthService);
  private clientId = this.auth.getClientId();

  // Data
  scores = signal<ChurnScore[]>([]);
  summary = signal<Summary>({ total_scored: 0, high_risk: 0, medium_risk: 0, low_risk: 0, avg_probability: 0 });

  // Pagination
  page = signal(1);
  pageSize = signal(25);
  totalRows = signal(0);
  totalPages = signal(1);

  // Filters
  riskFilter = signal<string>('');
  searchQuery = signal('');

  // State
  loading = signal(true);
  error = signal('');

  // Selected customer for detail panel
  selectedCustomer = signal<ChurnScore | null>(null);

  ngOnInit() {
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
