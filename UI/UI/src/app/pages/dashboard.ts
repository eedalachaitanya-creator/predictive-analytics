import { Component, OnInit, signal, inject, computed } from '@angular/core';
import { CommonModule, DecimalPipe, PercentPipe } from '@angular/common';
import { DashboardService } from '../services/dashboard.service';
import { AuthService } from '../services/auth.service';
import { TierLabelService } from '../services/tier-label.service';
import { TierLabelPipe } from '../pipes/tier-label.pipe';

const SEGMENT_COLORS: Record<string, string> = {
  'Champions':          'linear-gradient(90deg,#8B5CF6,#6D28D9)',
  'Loyal Customers':    'linear-gradient(90deg,#3B82F6,#2563EB)',
  "Can't Lose Them":    'linear-gradient(90deg,#EF4444,#DC2626)',
  'At Risk':            'linear-gradient(90deg,#F59E0B,#D97706)',
  'New Customers':      'linear-gradient(90deg,#10B981,#059669)',
  'Potential Loyalists': 'linear-gradient(90deg,#06B6D4,#0891B2)',
  'Hibernating':        'linear-gradient(90deg,#64748B,#475569)',
  'Needs Attention':    'linear-gradient(90deg,#F97316,#EA580C)',
};

const CHURN_COLORS: Record<string, string> = {
  'Churned':    'linear-gradient(90deg,#EF4444,#B91C1C)',
  'At-Risk':    'linear-gradient(90deg,#F59E0B,#D97706)',
  'Returning':  'linear-gradient(90deg,#10B981,#059669)',
  'Active / New':'linear-gradient(90deg,#3B82F6,#2563EB)',
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
      ...s, color: CHURN_COLORS[s.label] ?? 'linear-gradient(90deg,#64748B,#475569)'
    }))
  );

  tiers = computed(() =>
    (this.svc.data()?.tiers ?? []).map(t => ({
      ...t, color: TIER_COLORS[Object.keys(TIER_COLORS).find(k => t.label.includes(k)) ?? ''] ?? 'linear-gradient(90deg,#64748B,#475569)'
    }))
  );

  // ── Segment drill-down signals ─────────────────────────────────
  expandedSegment     = signal<string | null>(null);
  segmentCustomers    = signal<any[]>([]);
  segmentLoading      = signal(false);
  segmentPage         = signal(1);
  segmentTotalPages   = signal(1);
  segmentTotal        = signal(0);

  kpis     = computed(() => this.svc.data()?.kpis);
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
}
