import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { DashboardData } from '../models';

/** Generic paginated table behind a clickable summary card (matches the
 *  clients data-viewer shape: columns + rows + offset pagination). */
export interface KpiDrilldown {
  card: string;
  label: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  offset: number;
  limit: number;
}

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private api = inject(ApiService);

  readonly data     = signal<DashboardData | null>(null);
  readonly loading  = signal(false);
  readonly error    = signal<string | null>(null);

  load(clientId: string): Observable<DashboardData> {
    this.loading.set(true);
    this.error.set(null);
    return this.api.get<DashboardData>(`/dashboard?clientId=${clientId}`).pipe(
      tap({
        next:  d  => { this.data.set(d); this.loading.set(false); },
        error: e  => { this.error.set(e.message); this.loading.set(false); }
      })
    );
  }

  // loadOrders / OrderRow removed 2026-04-29 along with the
  // /dashboard/orders endpoint and the dashboard's Detail Data Tabs
  // section. Restore from git history if those drilldowns return.

  // pageSize default bumped 10 → 100 per CTO direction: the segment
  // drill-down on the Dashboard now shows 100 customers per page and
  // scrolls vertically inside .drilldown-scroll (see dashboard.scss)
  // instead of paginating in 10-row chunks.
  loadSegmentCustomers(clientId: string, segment: string, page: number = 1, pageSize: number = 100): Observable<any> {
    return this.api.get(`/dashboard/segment-customers?clientId=${clientId}&segment=${encodeURIComponent(segment)}&page=${page}&pageSize=${pageSize}`);
  }

  // Drill-down behind a clickable summary card. `card` is one of:
  // total_customers | total_orders | repeat_customers | high_value | lapsed_customers.
  // The backend guarantees `total` equals the matching KPI on the card.
  loadKpiDrilldown(clientId: string, card: string, offset = 0, limit = 100): Observable<KpiDrilldown> {
    return this.api.get<KpiDrilldown>(
      `/dashboard/kpi-drilldown?clientId=${clientId}&card=${encodeURIComponent(card)}&limit=${limit}&offset=${offset}`
    );
  }

  refresh(clientId: string): Observable<DashboardData> {
    return this.load(clientId);
  }
}
