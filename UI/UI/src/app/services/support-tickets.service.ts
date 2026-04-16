import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { SupportTicket, TicketStatus, TicketPriority } from '../models';

/**
 * SupportTicketsService — TABLE: support_tickets
 */
@Injectable({ providedIn: 'root' })
export class SupportTicketsService {
  private api = inject(ApiService);

  readonly tickets = signal<SupportTicket[]>([]);
  readonly loading = signal(false);
  readonly error   = signal<string | null>(null);

  load(clientId: string, page = 1, pageSize = 50): Observable<{ data: SupportTicket[]; total: number; pages: number }> {
    this.loading.set(true);
    this.error.set(null);
    return this.api.get<{ data: SupportTicket[]; total: number; pages: number }>(
      `/support-tickets?clientId=${clientId}&page=${page}&pageSize=${pageSize}`
    ).pipe(
      tap({
        next:  r => { this.tickets.set(r.data); this.loading.set(false); },
        error: e => { this.error.set(e.message);  this.loading.set(false); }
      })
    );
  }

  filterByStatus(clientId: string, status: TicketStatus): Observable<SupportTicket[]> {
    return this.api.get<SupportTicket[]>(`/support-tickets?clientId=${clientId}&status=${status}`);
  }

  filterByPriority(clientId: string, priority: TicketPriority): Observable<SupportTicket[]> {
    return this.api.get<SupportTicket[]>(`/support-tickets?clientId=${clientId}&priority=${priority}`);
  }

  getByCustomer(clientId: string, customerId: string): Observable<SupportTicket[]> {
    return this.api.get<SupportTicket[]>(`/support-tickets?clientId=${clientId}&customerId=${customerId}`);
  }

  updateStatus(ticketId: string, status: TicketStatus): Observable<SupportTicket> {
    return this.api.put<SupportTicket>(`/support-tickets/${ticketId}`, { status }).pipe(
      tap(updated => this.tickets.update(list => list.map(t => t.ticket_id === ticketId ? updated : t)))
    );
  }

  // ── Future methods to add ──────────────────────────────────
  // getResolutionStats(clientId): Observable<TicketStats> { ... }
  // getOpenByPriority(clientId): Observable<Record<TicketPriority, number>> { ... }
  // bulkClose(ticketIds): Observable<void> { ... }
}
