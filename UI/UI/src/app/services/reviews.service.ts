import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { CustomerReview, ReviewSentiment } from '../models';

/**
 * ReviewsService — TABLE: customer_reviews
 */
@Injectable({ providedIn: 'root' })
export class ReviewsService {
  private api = inject(ApiService);

  readonly reviews = signal<CustomerReview[]>([]);
  readonly loading = signal(false);
  readonly error   = signal<string | null>(null);

  // pageSize default bumped 50 → 100 per CTO direction so any future
  // consumer of this service matches the rest of the UI's page size.
  load(clientId: string, page = 1, pageSize = 100): Observable<{ data: CustomerReview[]; total: number; pages: number }> {
    this.loading.set(true);
    this.error.set(null);
    return this.api.get<{ data: CustomerReview[]; total: number; pages: number }>(
      `/reviews?clientId=${clientId}&page=${page}&pageSize=${pageSize}`
    ).pipe(
      tap({
        next:  r => { this.reviews.set(r.data); this.loading.set(false); },
        error: e => { this.error.set(e.message);  this.loading.set(false); }
      })
    );
  }

  filterBySentiment(clientId: string, sentiment: ReviewSentiment): Observable<CustomerReview[]> {
    return this.api.get<CustomerReview[]>(`/reviews?clientId=${clientId}&sentiment=${sentiment}`);
  }

  getByProduct(clientId: string, productId: number): Observable<CustomerReview[]> {
    return this.api.get<CustomerReview[]>(`/reviews?clientId=${clientId}&productId=${productId}`);
  }

  getByCustomer(clientId: string, customerId: string): Observable<CustomerReview[]> {
    return this.api.get<CustomerReview[]>(`/reviews?clientId=${clientId}&customerId=${customerId}`);
  }

  // ── Future methods to add ──────────────────────────────────
  // getSentimentSummary(clientId): Observable<SentimentSummary> { ... }
  // getTopRated(clientId, limit): Observable<CustomerReview[]> { ... }
  // flagReview(reviewId, reason): Observable<void> { ... }
}
