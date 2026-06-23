import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { AnalyticsData } from '../models';
import { KpiDrilldown } from './dashboard.service';

@Injectable({ providedIn: 'root' })
export class AnalyticsService {
  private api = inject(ApiService);

  readonly data    = signal<AnalyticsData | null>(null);
  readonly loading = signal(false);
  readonly error   = signal<string | null>(null);

  load(): Observable<AnalyticsData> {
    this.loading.set(true);
    this.error.set(null);
    return this.api.get<AnalyticsData>('/analytics').pipe(
      tap({
        next:  d => { this.data.set(d); this.loading.set(false); },
        error: e => { this.error.set(e.message); this.loading.set(false); }
      })
    );
  }

  // Cross-client drill-down behind a clickable Admin Analytics card. `card` is
  // one of: active_clients | total_customers | total_orders | avg_churn_rate.
  // The backend guarantees `total` equals the matching KPI on the card (for the
  // count cards) — see test_analytics_kpi_drilldown.py.
  loadKpiDrilldown(card: string, offset = 0, limit = 100): Observable<KpiDrilldown> {
    return this.api.get<KpiDrilldown>(
      `/analytics/kpi-drilldown?card=${encodeURIComponent(card)}&limit=${limit}&offset=${offset}`
    );
  }
}
