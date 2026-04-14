import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { Customer } from '../models';

@Injectable({ providedIn: 'root' })
export class CustomersService {
  private api = inject(ApiService);

  readonly customers = signal<Customer[]>([]);
  readonly loading   = signal(false);
  readonly error     = signal<string | null>(null);

  /** Load paginated customers for a client */
  load(clientId: string, page = 1, pageSize = 50): Observable<{ data: Customer[]; total: number; pages: number }> {
    this.loading.set(true);
    this.error.set(null);
    return this.api.get<{ data: Customer[]; total: number; pages: number }>(
      `/customers?clientId=${clientId}&page=${page}&pageSize=${pageSize}`
    ).pipe(
      tap({
        next:  r => { this.customers.set(r.data); this.loading.set(false); },
        error: e => { this.error.set(e.message);   this.loading.set(false); }
      })
    );
  }

  /** Get a single customer by ID */
  getById(clientId: string, customerId: string): Observable<Customer> {
    return this.api.get<Customer>(`/customers/${customerId}?clientId=${clientId}`);
  }

  /** Search customers by name or email */
  search(clientId: string, query: string): Observable<Customer[]> {
    return this.api.get<Customer[]>(`/customers/search?clientId=${clientId}&q=${encodeURIComponent(query)}`);
  }

  // ── Future methods to add ──────────────────────────────────
  // export(clientId: string): Observable<Blob> { ... }
  // getChurnRisk(clientId: string): Observable<Customer[]> { ... }
  // getHighValue(clientId: string): Observable<Customer[]> { ... }
}
