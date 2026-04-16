import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ScoutService } from '../../../services/scout.service';

@Component({
  selector: 'scout-monitor',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './monitor.html',
  styleUrls: ['./monitor.scss']
})
export class ScoutMonitorTab implements OnInit {
  private svc = inject(ScoutService);

  alerts          = signal<any[]>([]);
  products        = signal<any[]>([]);
  platforms       = signal<string[]>([]);
  loading         = signal(true);
  monitoring      = signal(false);
  monitorResult   = signal('');
  selectedProduct = signal<string | null>(null);
  priceHistory    = signal<any[]>([]);
  historyLoading  = signal(false);

  totalProducts  = signal(0);
  totalPlatforms = signal(0);
  alertCount     = signal(0);
  avgChange      = signal(0);

  ngOnInit() { this.loadData(); }

  loadData() {
    this.loading.set(true);

    this.svc.getAllProducts().subscribe({
      next: (res: any) => {
        const rawProducts = res.data || [];
        const plats = res.platforms || [];
        this.platforms.set(plats);
        this.totalPlatforms.set(plats.length);

        const rows = rawProducts.map((p: any) => {
          const row: any = { product_name: p.name };
          for (const listing of (p.listings || [])) {
            row[listing.platform] = {
              price: listing.price?.value ?? listing.price ?? 0,
              currency: listing.price?.currency ?? 'INR',
              url: listing.url || listing.product_url || '',
            };
          }
          return row;
        });
        this.products.set(rows);
        this.totalProducts.set(rows.length);
        this.loading.set(false);
      },
      error: () => this.loading.set(false)
    });

    this.svc.getAlerts().subscribe({
      next: (res: any) => {
        this.alerts.set(res.alerts || []);
        this.alertCount.set(res.unread_count ?? (res.alerts || []).length);
        const changes = (res.alerts || [])
          .filter((a: any) => a.change_percent != null)
          .map((a: any) => Math.abs(a.change_percent));
        this.avgChange.set(changes.length ? changes.reduce((a: number, b: number) => a + b, 0) / changes.length : 0);
      }
    });
  }

  runMonitor() {
    this.monitoring.set(true);
    this.monitorResult.set('');
    this.svc.runMonitor().subscribe({
      next: (res: any) => {
        this.monitorResult.set(`Checked ${res.products_checked} products, ${res.alerts_generated} alerts`);
        this.monitoring.set(false);
        this.loadData();
      },
      error: (err: any) => { this.monitorResult.set(err.message || 'Failed'); this.monitoring.set(false); }
    });
  }

  loadHistory(name: string) {
    if (this.selectedProduct() === name) { this.selectedProduct.set(null); return; }
    this.selectedProduct.set(name);
    this.historyLoading.set(true);
    this.priceHistory.set([]);
    this.svc.getPriceHistory(name).subscribe({
      next: (res: any) => { this.priceHistory.set(res.history || []); this.historyLoading.set(false); },
      error: () => this.historyLoading.set(false)
    });
  }

  fmt(val: number, cur: string = 'INR'): string {
    if (!val || val <= 0) return '—';
    return (cur === 'INR' ? '₹' : cur === 'USD' ? '$' : cur) + val.toLocaleString();
  }

  changeIcon(a: any): string { return a.direction === 'down' ? '📉' : a.direction === 'up' ? '📈' : '🆕'; }
  changeClass(a: any): string { return a.direction === 'down' ? 'text-green' : a.direction === 'up' ? 'text-red' : 'text-blue'; }
  fmtChange(a: any): string {
    if (a.direction === 'new') return 'New';
    return a.change_percent != null ? `${a.change_percent > 0 ? '+' : ''}${a.change_percent.toFixed(1)}%` : '—';
  }
  timeAgo(d: string): string {
    if (!d) return '—';
    const m = Math.floor((Date.now() - new Date(d).getTime()) / 60000);
    if (m < 1) return 'just now'; if (m < 60) return m + 'm ago';
    const h = Math.floor(m / 60); if (h < 24) return h + 'h ago';
    return Math.floor(h / 24) + 'd ago';
  }
}