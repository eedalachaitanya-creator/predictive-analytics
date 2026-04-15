import { Component, OnInit, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AnalyticsService } from '../services/analytics.service';

const CLIENT_COLORS = ['linear-gradient(90deg,#0071CE,#0099FF)', 'linear-gradient(90deg,#EF4444,#DC2626)', 'linear-gradient(90deg,#10B981,#059669)', 'linear-gradient(90deg,#8B5CF6,#6D28D9)', 'linear-gradient(90deg,#F59E0B,#D97706)'];

@Component({
  selector: 'app-analytics',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './analytics.html',
  styleUrls: ['./analytics.scss']
})
export class AnalyticsComponent implements OnInit {
  svc = inject(AnalyticsService);

  kpis    = computed(() => this.svc.data()?.platformKpis);
  clients = computed(() =>
    (this.svc.data()?.clientMetrics ?? []).map((c, i) => ({ ...c, color: c.color || CLIENT_COLORS[i % CLIENT_COLORS.length] }))
  );
  trend   = computed(() => this.svc.data()?.monthlyTrend ?? []);

  // Max values for bar scaling
  maxCustomers = computed(() => Math.max(...this.clients().map(c => c.customers), 1));
  maxOrders    = computed(() => Math.max(...this.clients().map(c => c.orders), 1));
  maxHV        = computed(() => Math.max(...this.clients().map(c => c.highValue), 1));

  custPct  = (c: number) => Math.round((c / this.maxCustomers()) * 100);
  orderPct = (c: number) => Math.round((c / this.maxOrders())    * 100);
  hvPct    = (c: number) => Math.round((c / this.maxHV())        * 100);
  churnPct = (pct: number) => Math.round(pct);

  // Trend client keys (dynamic from data)
  trendClients = computed(() => {
    const first = this.trend()[0];
    if (!first) return [];
    return Object.keys(first.runsByClient);
  });

  ngOnInit() {
    this.svc.load().subscribe({ error: () => {} });
  }

  refresh() { this.svc.load().subscribe({ error: () => {} }); }
}
