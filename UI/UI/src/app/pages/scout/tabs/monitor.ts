import { Component, computed, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ScoutService } from '../../../services/scout.service';

// Page sizes for the two tables. Defined as constants rather than magic
// numbers so they're easy to change in one place.
const ALERTS_PAGE_SIZE   = 50;
const PRODUCTS_PAGE_SIZE = 20;

@Component({
  selector: 'scout-monitor',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './monitor.html',
  styleUrls: ['./monitor.scss']
})
export class ScoutMonitorTab implements OnInit {
  private svc = inject(ScoutService);

  // ── Tables (paged) ──────────────────────────────────────────────
  alerts        = signal<any[]>([]);
  alertsTotal   = signal(0);
  alertsPage    = signal(1);              // 1-based page index

  products      = signal<any[]>([]);
  productsTotal = signal(0);
  productsPage  = signal(1);

  platforms     = signal<string[]>([]);

  // ── Other state (unchanged) ─────────────────────────────────────
  loading         = signal(true);
  monitoring      = signal(false);
  monitorResult   = signal('');
  selectedProduct = signal<string | null>(null);
  priceHistory    = signal<any[]>([]);
  historyLoading  = signal(false);

  // ── KPI tiles ───────────────────────────────────────────────────
  // These now read from totals (not current page) so the tiles always
  // show the true count even when paging.
  totalProducts  = computed(() => this.productsTotal());
  alertCount     = signal(0);        // unread_count from /alerts response
  totalPlatforms = computed(() => this.svc.activePlatformNames().length);

  // Page sizes (exposed to template for "Showing X-Y of N")
  readonly alertsPageSize   = ALERTS_PAGE_SIZE;
  readonly productsPageSize = PRODUCTS_PAGE_SIZE;

  // Ranges for the "Showing X-Y of N" label.
  alertsRange = computed(() => this.rangeLabel(
    this.alertsPage(), ALERTS_PAGE_SIZE, this.alertsTotal()
  ));
  productsRange = computed(() => this.rangeLabel(
    this.productsPage(), PRODUCTS_PAGE_SIZE, this.productsTotal()
  ));

  alertsHasNext   = computed(() => this.alertsPage() * ALERTS_PAGE_SIZE < this.alertsTotal());
  productsHasNext = computed(() => this.productsPage() * PRODUCTS_PAGE_SIZE < this.productsTotal());

  ngOnInit() {
    // Trigger shared platforms refresh so the computed totalPlatforms updates.
    this.svc.refreshPlatforms();
    this.loadData();
  }

  loadData() {
    this.loadProducts();
    this.loadAlerts();
  }

  // ── Alerts page loading ─────────────────────────────────────────

  loadAlerts() {
    const page = this.alertsPage();
    const offset = (page - 1) * ALERTS_PAGE_SIZE;
    this.svc.getAlerts({ limit: ALERTS_PAGE_SIZE, offset }).subscribe({
      next: (res: any) => {
        this.alerts.set(res.alerts || []);
        this.alertsTotal.set(res.total ?? (res.alerts || []).length);
        this.alertCount.set(res.unread_count ?? 0);
      }
    });
  }

  nextAlertsPage() {
    if (!this.alertsHasNext()) return;
    this.alertsPage.update(p => p + 1);
    this.loadAlerts();
  }

  prevAlertsPage() {
    if (this.alertsPage() <= 1) return;
    this.alertsPage.update(p => p - 1);
    this.loadAlerts();
  }

  // ── Products page loading ───────────────────────────────────────

  loadProducts() {
    this.loading.set(true);
    const page = this.productsPage();
    const offset = (page - 1) * PRODUCTS_PAGE_SIZE;
    this.svc.getAllProducts({ limit: PRODUCTS_PAGE_SIZE, offset }).subscribe({
      next: (res: any) => {
        const rawProducts = res.data || [];
        const plats = res.platforms || [];
        this.platforms.set(plats);

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
        this.productsTotal.set(res.total ?? rows.length);
        this.loading.set(false);
      },
      error: () => this.loading.set(false)
    });
  }

  nextProductsPage() {
    if (!this.productsHasNext()) return;
    this.productsPage.update(p => p + 1);
    this.loadProducts();
  }

  prevProductsPage() {
    if (this.productsPage() <= 1) return;
    this.productsPage.update(p => p - 1);
    this.loadProducts();
  }

  // ── "Showing X-Y of N" helper ───────────────────────────────────
  // Returns "0 of 0" when empty rather than crashing on NaN.
  private rangeLabel(page: number, size: number, total: number): string {
    if (total === 0) return '0 of 0';
    const start = (page - 1) * size + 1;
    const end   = Math.min(page * size, total);
    return `${start}–${end} of ${total}`;
  }

  // ── Monitor action (unchanged flow, just uses new load methods) ──

  runMonitor() {
    this.monitoring.set(true);
    this.monitorResult.set('');
    this.svc.runMonitor().subscribe({
      next: (res: any) => {
        this.monitorResult.set(`Checked ${res.products_checked} products, ${res.alerts_generated} alerts`);
        this.monitoring.set(false);
        // Reset to page 1 after a monitor run so the newest alerts are visible.
        this.alertsPage.set(1);
        this.productsPage.set(1);
        this.loadData();
      },
      error: (err: any) => { this.monitorResult.set(err.message || 'Failed'); this.monitoring.set(false); }
    });
  }

  // ── Unchanged below ─────────────────────────────────────────────

  loadHistory(name: string) {
    if (this.selectedProduct() === name) { this.selectedProduct.set(null); return; }
    this.selectedProduct.set(name);
    this.historyLoading.set(true);
    this.priceHistory.set([]);
    this.svc.getPriceHistory(name).subscribe({
      next: (res: any) => {
        const platforms = res.platforms || {};
        const flat: any[] = [];
        for (const [platform, points] of Object.entries(platforms)) {
          for (const point of (points as any[])) {
            flat.push({ ...point, platform });
          }
        }
        flat.sort((a, b) => new Date(b.scraped_at).getTime() - new Date(a.scraped_at).getTime());
        this.priceHistory.set(flat);
        this.historyLoading.set(false);
      },
      error: () => this.historyLoading.set(false)
    });
  }

  fmt(val: number, cur: string = 'INR'): string {
    if (!val || val <= 0) return '—';
    const symbols: Record<string, string> = {
      INR: '₹', USD: '$', EUR: '€', GBP: '£', JPY: '¥',
      AUD: 'A$', CAD: 'C$', SGD: 'S$', AED: 'AED ',
    };
    return (symbols[cur] || cur + ' ') + val.toLocaleString();
  }

  currencyFor(platform: string): string {
    const p = platform.toLowerCase();
    if (p.endsWith('.in') || ['flipkart', 'myntra', 'nykaa', 'beato', 'fastandup.in', 'ikea'].includes(p)) return 'INR';
    if (['amazon', 'walmart', 'target', 'ebay'].includes(p)) return 'USD';
    if (p === 'fast and up') return 'EUR';
    return 'INR';
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