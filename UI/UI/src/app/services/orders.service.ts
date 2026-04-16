import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { Order, LineItem } from '../models';

@Injectable({ providedIn: 'root' })
export class OrdersService {
  private api = inject(ApiService);

  readonly orders  = signal<Order[]>([]);
  readonly loading = signal(false);
  readonly error   = signal<string | null>(null);

  /** Load paginated orders for a client */
  load(clientId: string, page = 1, pageSize = 50): Observable<{ data: Order[]; total: number; pages: number }> {
    this.loading.set(true);
    this.error.set(null);
    return this.api.get<{ data: Order[]; total: number; pages: number }>(
      `/orders?clientId=${clientId}&page=${page}&pageSize=${pageSize}`
    ).pipe(
      tap({
        next:  r => { this.orders.set(r.data); this.loading.set(false); },
        error: e => { this.error.set(e.message);  this.loading.set(false); }
      })
    );
  }

  /** Get all line items for a specific order */
  getLineItems(clientId: string, orderId: string): Observable<LineItem[]> {
    return this.api.get<LineItem[]>(`/orders/${orderId}/line-items?clientId=${clientId}`);
  }

  /** Filter orders by status */
  filterByStatus(clientId: string, status: string, page = 1): Observable<{ data: Order[]; total: number; pages: number }> {
    return this.api.get(`/orders?clientId=${clientId}&status=${status}&page=${page}`);
  }

  // ── Future methods to add ──────────────────────────────────
  // filterByDateRange(clientId, from, to): Observable<...> { ... }
  // filterByCustomer(clientId, customerId): Observable<...> { ... }
  // getSummaryStats(clientId): Observable<OrderSummary> { ... }
}
