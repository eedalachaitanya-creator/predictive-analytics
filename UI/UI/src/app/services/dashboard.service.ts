import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { DashboardData, OrderRow } from '../models';

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

  loadOrders(clientId: string, page: number, tab: string): Observable<{ orders: OrderRow[]; total: number; pages: number }> {
    return this.api.get(`/dashboard/orders?clientId=${clientId}&page=${page}&tab=${tab}`);
  }

  loadSegmentCustomers(clientId: string, segment: string, page: number = 1, pageSize: number = 10): Observable<any> {
    return this.api.get(`/dashboard/segment-customers?clientId=${clientId}&segment=${encodeURIComponent(segment)}&page=${page}&pageSize=${pageSize}`);
  }

  refresh(clientId: string): Observable<DashboardData> {
    return this.load(clientId);
  }
}
