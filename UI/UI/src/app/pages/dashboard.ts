import { Component, OnInit, signal, inject, computed } from '@angular/core';
import { CommonModule, DecimalPipe, PercentPipe } from '@angular/common';
import { DashboardService } from '../services/dashboard.service';
import { AuthService } from '../services/auth.service';
import { TierLabelService } from '../services/tier-label.service';
import { TierLabelPipe } from '../pipes/tier-label.pipe';

const SEGMENT_COLORS: Record<string, string> = {
  'Good':     'linear-gradient(90deg,#10B981,#059669)',
  'At-Risk':  'linear-gradient(90deg,#F59E0B,#D97706)',
  'Churned':  'linear-gradient(90deg,#EF4444,#B91C1C)',
};

const CHURN_COLORS: Record<string, string> = {
  'Churned':    'linear-gradient(90deg,#EF4444,#B91C1C)',
  'At-Risk':    'linear-gradient(90deg,#F59E0B,#D97706)',
  'Active':     'linear-gradient(90deg,#3B82F6,#2563EB)',
  'Returning':  'linear-gradient(90deg,#10B981,#059669)',
  'Active / New':'linear-gradient(90deg,#3B82F6,#2563EB)',
};

// Friendly display names for the Purchase-Recency card so it reads as an
// order-recency view, clearly distinct from the ML risk tiers on the Churn
// Scores page. Keyed by the backend churnBreakdown label.
const RECENCY_LABELS: Record<string, string> = {
  'Active':  'Recently Purchased',
  'At-Risk': 'Slowing Down',
  'Churned': 'Lapsed',
};

const TIER_COLORS: Record<string, string> = {
  'Platinum': 'linear-gradient(90deg,#C0C0C0,#9CA3AF)',
  'Gold':     'linear-gradient(90deg,#F59E0B,#D97706)',
  'Silver':   'linear-gradient(90deg,#94A3B8,#64748B)',
  'Bronze':   'linear-gradient(90deg,#B45309,#92400E)',
};

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule, TierLabelPipe],
  templateUrl: './dashboard.html',
  styleUrls: ['./dashboard.scss']
})
export class DashboardComponent implements OnInit {
  svc  = inject(DashboardService);
  auth = inject(AuthService);
  private tierLabels = inject(TierLabelService);
  private clientId = this.auth.getClientId();

  // Derived from service signal — add display colors
  segments = computed(() =>
    (this.svc.data()?.segments ?? []).map(s => ({
      ...s, color: SEGMENT_COLORS[s.label] ?? 'linear-gradient(90deg,#475569,#334155)'
    }))
  );

  churnBreakdown = computed(() =>
    (this.svc.data()?.churnBreakdown ?? []).map(s => ({
      ...s,
      color: CHURN_COLORS[s.label] ?? 'linear-gradient(90deg,#64748B,#475569)',
      label: RECENCY_LABELS[s.label] ?? s.label,   // friendly recency name
    }))
  );

  // Total customers in the recency view — shown on the card so it's clear this
  // covers ALL customers, unlike the ML-scored count on the Churn Scores page.
  activityTotal = computed(() => this.churnBreakdown().reduce((n, c) => n + c.count, 0));

  tiers = computed(() =>
    (this.svc.data()?.tiers ?? []).map(t => ({
      ...t, color: TIER_COLORS[t.label]?? 'linear-gradient(90deg,#64748B,#475569)'
    }))
  );

  // ── Segment drill-down signals ─────────────────────────────────
  expandedSegment     = signal<string | null>(null);
  segmentCustomers    = signal<any[]>([]);
  segmentLoading      = signal(false);
  segmentPage         = signal(1);
  segmentTotalPages   = signal(1);
  segmentTotal        = signal(0);

  kpis        = computed(() => this.svc.data()?.kpis);
  repeatVsOne = computed(() => this.svc.data()?.repeatVsOneTime);

  ngOnInit() {
    // Pull the client's latest tier labels so {{ x | tierLabel }} renders custom names.
    this.tierLabels.refresh();

    this.svc.load(this.clientId).subscribe({ error: () => {} });
  }

  toggleSegment(segmentLabel: string) {
    if (this.expandedSegment() === segmentLabel) {
      // Collapse if already open
      this.expandedSegment.set(null);
      this.segmentCustomers.set([]);
      return;
    }
    this.expandedSegment.set(segmentLabel);
    this.segmentPage.set(1);
    this.loadSegmentPage(segmentLabel, 1);
  }

  loadSegmentPage(segment: string, page: number) {
    this.segmentLoading.set(true);
    this.svc.loadSegmentCustomers(this.clientId, segment, page).subscribe({
      next: (res) => {
        this.segmentCustomers.set(res.customers);
        this.segmentTotalPages.set(res.pages);
        this.segmentTotal.set(res.total);
        this.segmentPage.set(page);
        this.segmentLoading.set(false);
      },
      error: () => { this.segmentLoading.set(false); }
    });
  }

  segmentPrev() {
    const p = this.segmentPage();
    if (p > 1 && this.expandedSegment()) {
      this.loadSegmentPage(this.expandedSegment()!, p - 1);
    }
  }

  segmentNext() {
    const p = this.segmentPage();
    if (p < this.segmentTotalPages() && this.expandedSegment()) {
      this.loadSegmentPage(this.expandedSegment()!, p + 1);
    }
  }

  riskClass(tier: string): string {
    if (tier === 'HIGH') return 'red';
    if (tier === 'MEDIUM') return 'yellow';
    return 'green';
  }

  refresh() {
    this.svc.refresh(this.clientId).subscribe({ error: () => {} });
  }

  formatCurrency(n: number): string {
    return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  /** Returns 0 instead of NaN/Infinity when denominator is 0 or falsy. */
  safePct(numerator: number, denominator: number): number {
    return denominator > 0 ? (numerator / denominator) * 100 : 0;
  }
}