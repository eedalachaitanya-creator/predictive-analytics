import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { DashboardData } from '../models';

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

  refresh(clientId: string): Observable<DashboardData> {
    return this.load(clientId);
  }
}
