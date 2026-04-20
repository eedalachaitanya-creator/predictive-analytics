import { Component, inject, signal, computed, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { AuthService } from '../services/auth.service';

interface BucketTotals {
  calls: number;
  cost: number;
  tokens: number;
}

interface PerModelRow {
  model: string;
  calls: number;
  tokens: number;
  cost: number;
}

interface DailyPoint {
  date: string;
  calls: number;
  cost: number;
}

interface RecentCall {
  created_at: string;
  call_type: string;
  model: string;
  tokens: number;
  cost: number;
  over_budget: boolean;
}

interface Aggregates {
  today: BucketTotals;
  week: BucketTotals;
  month: BucketTotals;
  all_time: BucketTotals;
  avg_cost_per_call: number;
  budget_usd_per_call: number;
  over_budget_pct: number;
  per_model: PerModelRow[];
  daily_trend: DailyPoint[];
  recent_calls: RecentCall[];
}

interface CostResponse {
  langfuse_enabled: boolean;
  langfuse_configured: boolean;
  target_per_call: number;
  cost_per_input_token: number;
  cost_per_output_token: number;
  client_id?: string;
  aggregates?: Aggregates | null;
  aggregates_error?: string;
}

@Component({
  selector: 'app-cost-tracking',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './cost-tracking.html',
  styleUrls: ['./cost-tracking.scss'],
})
export class CostTrackingComponent implements OnInit {
  private http = inject(HttpClient);
  private auth = inject(AuthService);
  private base = environment.apiUrl;

  data = signal<CostResponse | null>(null);
  loading = signal(true);
  error = signal<string | null>(null);

  // Computed helpers the template reads
  agg = computed<Aggregates | null>(() => this.data()?.aggregates ?? null);
  hasData = computed(() => (this.agg()?.all_time.calls ?? 0) > 0);

  // Chart dimensions (simple inline SVG)
  chartWidth = 720;
  chartHeight = 180;

  ngOnInit() {
    this.load();
  }

  load() {
    this.loading.set(true);
    this.error.set(null);
    this.http
      .get<CostResponse>(`${this.base}/cost-tracking?clientId=${this.auth.getClientId()}`)
      .subscribe({
        next: (res) => {
          this.data.set(res);
          this.loading.set(false);
        },
        error: () => {
          this.error.set('Could not load cost tracking data. Is the backend running?');
          this.loading.set(false);
        },
      });
  }

  fmtUsd(v: number | undefined | null): string {
    if (v == null) return '$0.00';
    if (v < 0.01 && v > 0) return '$' + v.toFixed(6);
    return '$' + v.toFixed(2);
  }

  fmtTokens(v: number | undefined | null): string {
    if (v == null) return '0';
    if (v >= 1_000_000) return (v / 1_000_000).toFixed(2) + 'M';
    if (v >= 1_000) return (v / 1_000).toFixed(1) + 'K';
    return v.toString();
  }

  fmtDate(iso: string | undefined | null): string {
    if (!iso) return '';
    return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  }

  fmtDateTime(iso: string | undefined | null): string {
    if (!iso) return '';
    return new Date(iso).toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
    });
  }

  // Build an SVG polyline path for the daily trend chart
  trendPoints(): string {
    const pts = this.agg()?.daily_trend ?? [];
    if (pts.length === 0) return '';
    const pad = 20;
    const w = this.chartWidth - pad * 2;
    const h = this.chartHeight - pad * 2;
    const maxCost = Math.max(...pts.map(p => p.cost), 0.0001);
    return pts.map((p, i) => {
      const x = pad + (pts.length === 1 ? w / 2 : (i / (pts.length - 1)) * w);
      const y = pad + h - (p.cost / maxCost) * h;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
  }

  trendLabels(): { x: number; label: string }[] {
    const pts = this.agg()?.daily_trend ?? [];
    if (pts.length === 0) return [];
    const pad = 20;
    const w = this.chartWidth - pad * 2;
    return pts.map((p, i) => ({
      x: pad + (pts.length === 1 ? w / 2 : (i / (pts.length - 1)) * w),
      label: this.fmtDate(p.date),
    }));
  }

  // Budget-status helpers for the top-right pill
  budgetStatus = computed<{ label: string; cls: string }>(() => {
    const a = this.agg();
    if (!a || a.all_time.calls === 0) return { label: 'No data yet', cls: 'muted' };
    if (a.over_budget_pct >= 20) return { label: `${a.over_budget_pct}% over budget`, cls: 'danger' };
    if (a.over_budget_pct > 0)   return { label: `${a.over_budget_pct}% over budget`, cls: 'warn' };
    return { label: 'Within budget', cls: 'ok' };
  });
}
