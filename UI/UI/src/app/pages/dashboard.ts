import { Component, OnInit, signal, inject, computed } from '@angular/core';
import { CommonModule, DecimalPipe, PercentPipe } from '@angular/common';
import { DashboardService } from '../services/dashboard.service';
import { AuthService } from '../services/auth.service';

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
  imports: [CommonModule],
  templateUrl: './dashboard.html',
  styleUrls: ['./dashboard.scss']
})
export class DashboardComponent implements OnInit {
  svc = inject(DashboardService);
  auth = inject(AuthService);
  private clientId = this.auth.getClientId();

  activeTab   = signal('Clean Orders');
  currentPage = signal(1);

  detailTabs = ['Clean Orders','Repeat Analysis','RFM','High Value',
                'Product Affinity','ML Features','Vendor Analysis','Audit Log','Quarantine'];

  // ── Tab-specific data signals ──────────────────────────────────
  tabRows       = signal<any[]>([]);
  tabTotalPages = signal(1);
  tabLoading    = signal(false);

  readonly TAB_HEADERS: Record<string, string[]> = {
    'Clean Orders':     ['Order ID','Customer','Date','Items','Gross','Discount','Net','Status'],
    'RFM':              ['Customer ID','Name','RFM Scores','Total Score','Spend','Days Since','Orders','Segment'],
    'High Value':       ['Customer ID','Name','Orders','RFM Score','Spend','Avg Order','Days Since','Tier'],
    'Repeat Analysis':  ['Customer ID','Name','Orders','Products','Spend','Avg Order','Avg Between','Status'],
    'Product Affinity': ['Product ID','Product','Category','Qty Sold','Revenue','Avg Price','Orders','Brand'],
    'ML Features':      ['Customer ID','Name','Account Age','Orders','Spend','Discount %','Return %','Status'],
    'Vendor Analysis':  ['Vendor ID','Vendor','Products','Qty Sold','Revenue','Avg Price','Orders','Reach'],
    'Audit Log':        ['ID','Customer','Date','—','Churn Prob','Discount %','—','Risk Tier'],
    'Quarantine':       ['ID','Customer','Scored At','—','Churn Prob','—','—','Risk Tier'],
  };

  activeHeaders = computed(() => this.TAB_HEADERS[this.activeTab()] ?? this.TAB_HEADERS['Clean Orders']);

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
  orders   = computed(() => this.svc.data()?.recentOrders ?? []);
  repeatVsOne = computed(() => this.svc.data()?.repeatVsOneTime);
  totalPages  = computed(() => this.svc.data()?.totalOrderPages ?? 1);

  ngOnInit() {
    this.svc.load(this.clientId).subscribe({
      next: () => {
        this.tabRows.set(this.svc.data()?.recentOrders ?? []);
        this.tabTotalPages.set(this.svc.data()?.totalOrderPages ?? 1);
      },
      error: () => {}
    });
  }

  switchTab(tab: string) {
    this.activeTab.set(tab);
    this.currentPage.set(1);
    this.tabLoading.set(true);
    this.tabRows.set([]);
    this.svc.loadOrders(this.clientId, 1, tab).subscribe({
      next: (res) => {
        this.tabRows.set(res.orders);
        this.tabTotalPages.set(res.pages);
        this.tabLoading.set(false);
      },
      error: () => { this.tabLoading.set(false); }
    });
  }

  loadPage(p: number) {
    if (p < 1 || p > this.tabTotalPages()) return;
    this.currentPage.set(p);
    this.svc.loadOrders(this.clientId, p, this.activeTab()).subscribe({
      next: (res) => {
        this.tabRows.set(res.orders);
        this.tabTotalPages.set(res.pages);
      },
      error: () => {}
    });
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

  orderStatusClass(s: string): string {
    if (s === 'completed') return 'green';
    if (s === 'returned')  return 'yellow';
    if (s === 'cancelled') return 'red';
    return 'gray';
  }

  orderStatusLabel(s: string): string {
    const map: Record<string,string> = { completed:'✅ Completed', returned:'🔄 Returned', cancelled:'❌ Cancelled', pending:'🕐 Pending' };
    return map[s] ?? s;
  }

  formatCurrency(n: number): string {
    return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  formatNumber(n: any): string {
    if (n == null) return '—';
    if (typeof n === 'string') return n;
    if (Number.isInteger(n)) return n.toLocaleString('en-US');
    return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
}
