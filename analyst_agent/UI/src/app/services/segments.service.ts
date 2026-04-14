import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { BusinessSegment, ValueTier, ValueProposition, TierName, DbRiskLevel } from '../models';

/**
 * SegmentsService
 * Covers: business_segments, value_tiers, value_propositions.
 */
@Injectable({ providedIn: 'root' })
export class SegmentsService {
  private api = inject(ApiService);

  readonly segments         = signal<BusinessSegment[]>([]);
  readonly tiers            = signal<ValueTier[]>([]);
  readonly valuePropositions = signal<ValueProposition[]>([]);
  readonly loading          = signal(false);
  readonly error            = signal<string | null>(null);

  loadSegments(): Observable<BusinessSegment[]> {
    this.loading.set(true);
    return this.api.get<BusinessSegment[]>('/segments').pipe(
      tap({
        next:  s => { this.segments.set(s); this.loading.set(false); },
        error: e => { this.error.set(e.message); this.loading.set(false); }
      })
    );
  }

  loadTiers(): Observable<ValueTier[]> {
    this.loading.set(true);
    return this.api.get<ValueTier[]>('/tiers').pipe(
      tap({
        next:  t => { this.tiers.set(t); this.loading.set(false); },
        error: e => { this.error.set(e.message); this.loading.set(false); }
      })
    );
  }

  loadValuePropositions(clientId: string): Observable<ValueProposition[]> {
    this.loading.set(true);
    return this.api.get<ValueProposition[]>(`/value-propositions?clientId=${clientId}`).pipe(
      tap({
        next:  v => { this.valuePropositions.set(v); this.loading.set(false); },
        error: e => { this.error.set(e.message);     this.loading.set(false); }
      })
    );
  }

  /** Lookup a value proposition by tier + risk level */
  getProposition(tier: TierName, risk: DbRiskLevel): ValueProposition | undefined {
    return this.valuePropositions().find(
      v => v.tier_name === tier && v.risk_level === risk
    );
  }

  /** Get tier config by name */
  getTierByName(name: TierName): ValueTier | undefined {
    return this.tiers().find(t => t.tier_name === name);
  }

  // ── Future methods to add ──────────────────────────────────
  // updateTierThreshold(tierId, threshold): Observable<ValueTier> { ... }
  // updateValueProposition(vp): Observable<ValueProposition> { ... }
  // getSegmentCustomers(clientId, segmentId): Observable<Customer[]> { ... }
}
