import { Component, computed, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ScoutService } from '../../../services/scout.service';

// Page sizes for the two tables. Defined as constants rather than magic
// numbers so they're easy to change in one place.
const ALERTS_PAGE_SIZE   = 10;
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
  allAlerts     = signal<any[]>([]);  // full deduped list for frontend pagination
  alertsTotal   = signal(0);
  alertsPage    = signal(1);              // 1-based page index
  alertsLoading = signal(false);          // independent of products loading
  alertsError   = signal('');

  products      = signal<any[]>([]);
  productsTotal = signal(0);
  productsPage  = signal(1);

  platforms     = signal<string[]>([]);

  // ── Other state ─────────────────────────────────────────────────
  loading         = signal(true);         // products table only
  monitoring      = signal(false);
  monitorResult   = signal('');
  selectedProduct = signal<string | null>(null);
  priceHistory    = signal<any[]>([]);
  historyLoading  = signal(false);

  // ── KPI tiles ───────────────────────────────────────────────────
  // These read from totals (not current page) so tiles always show
  // the true count regardless of which page is active.
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
    this.alertsLoading.set(true);
    this.alertsError.set('');
    // Fetch ALL alerts at once, deduplicate on frontend, then paginate.
    // This gives accurate total count and correct LIFO order.
    this.svc.getAlerts({ limit: 500, offset: 0 }).subscribe({
      next: (res: any) => {
        // Deduplicate: keep only the latest alert per product+platform
        const allAlerts: any[] = res.alerts || [];
        const seen = new Map<string, any>();
        for (const a of allAlerts) {
          // Normalize: use first 30 chars of title to group same product
          // with different scraped titles (e.g. search query vs full title)
          const normTitle = (a.title || a.product_name || '').toLowerCase().slice(0, 30);
          const key = `${normTitle}__${a.platform}`;
          if (!seen.has(key) || new Date(a.detected_at) > new Date(seen.get(key).detected_at)) {
            seen.set(key, a);
          }
        }
        const deduped = Array.from(seen.values())
          .sort((a, b) => new Date(b.detected_at).getTime() - new Date(a.detected_at).getTime());
        // Store ALL deduped alerts, paginate in template via slice
        this.allAlerts.set(deduped);
        this.alertsTotal.set(deduped.length);
        // Show current page slice
        const page = this.alertsPage();
        const start = (page - 1) * ALERTS_PAGE_SIZE;
        this.alerts.set(deduped.slice(start, start + ALERTS_PAGE_SIZE));
        this.alertCount.set(res.unread_count ?? 0);
        this.alertsLoading.set(false);
      },
      error: (err: any) => {
        this.alertsError.set(err?.error?.detail ?? err?.message ?? 'Could not load alerts.');
        this.alertsLoading.set(false);
      }
    });
  }

  nextAlertsPage() {
    if (!this.alertsHasNext()) return;
    this.alertsPage.update(p => p + 1);
    const page = this.alertsPage();
    const start = (page - 1) * ALERTS_PAGE_SIZE;
    this.alerts.set(this.allAlerts().slice(start, start + ALERTS_PAGE_SIZE));
  }

  prevAlertsPage() {
    if (this.alertsPage() <= 1) return;
    this.alertsPage.update(p => p - 1);
    const page = this.alertsPage();
    const start = (page - 1) * ALERTS_PAGE_SIZE;
    this.alerts.set(this.allAlerts().slice(start, start + ALERTS_PAGE_SIZE));
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
              // Capture the scraped technical title (e.g., "Dyson Supersonic
              // HD08 Hair Dryer Vinca Blue/Rosé"). The row's product_name is
              // the user's search query (canonical anchor); per-platform
              // titles are surfaced via tooltips on the price cells so users
              // can see exactly which SKU each platform listed.
              title: listing.title || '',
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

  // ── Monitor action ───────────────────────────────────────────────

  runMonitor() {
    this.monitoring.set(true);
    this.monitorResult.set('');
    this.svc.runMonitor().subscribe({
      next: (res: any) => {
        this.monitorResult.set('');
        this.monitoring.set(false);
        // Reset to page 1 after a monitor run so the newest alerts are visible.
        this.alertsPage.set(1);
        this.productsPage.set(1);
        this.loadData();
      },
      error: (err: any) => { this.monitorResult.set(err.message || 'Failed'); this.monitoring.set(false); }
    });
  }

  // ── Price history ────────────────────────────────────────────────

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

  fmt(val: number, cur: string = ''): string {
    if (!val || val <= 0) return '—';
    if (!cur) return val.toLocaleString();  // no currency info — show raw number
    const symbols: Record<string, string> = {
        INR: '₹', USD: '$', EUR: '€', GBP: '£', JPY: '¥',
        AUD: 'A$', CAD: 'C$', SGD: 'S$', AED: 'AED ',
    };
    return (symbols[cur] || cur + ' ') + val.toLocaleString();
}

  /**
   * Derive a short display name for a platform column header.
   *
   * When a platform's name was saved as a full URL (e.g. the Myntra UTM URL
   * visible in the bug screenshot), the raw value is unusable as a column
   * header — it blows the table layout even with CSS truncation because the
   * browser allocates minimum content width before overflow kicks in.
   *
   * Strategy (mirrors _canonical_platform_name on the backend):
   *   1. If it looks like a URL, extract just the hostname.
   *   2. Strip www. prefix.
   *   3. Strip common TLD suffixes (.com, .in, etc.).
   *   4. Title-case the result.
   *   5. Cap at 20 chars with ellipsis for anything still too long.
   *
   * The full raw value is always shown via the [title] tooltip on hover.
   */
  displayName(platform: string): string {
    let name = platform.trim();

    // If it looks like a URL, extract just the hostname
    if (name.startsWith('http://') || name.startsWith('https://') || name.includes('://')) {
      try {
        name = new URL(name).hostname;
      } catch {
        // Not a valid URL — fall through with original value
      }
    }

    // Strip www.
    name = name.replace(/^www\./, '');

    // Strip common TLD suffixes
    name = name.replace(/\.(com\.au|co\.uk|co\.in|com|net|org|in|io|co|store|shop|app)$/, '');

    // Title-case (flipkart → Flipkart)
    name = name.charAt(0).toUpperCase() + name.slice(1);

    // Hard cap at 20 chars — anything still longer gets ellipsis
    if (name.length > 20) {
      name = name.slice(0, 18) + '…';
    }

    return name;
  }

  allTitlesFor(row: any): string {
    const lines: string[] = [];
    for (const p of this.platforms()) {
      const cell = row[p];
      if (cell?.title) lines.push(`${p}: ${cell.title}`);
    }
    return lines.length ? lines.join('\n') : '(no titles available)';
  }

  currencyFor(platform: string): string {
    const p = platform.toLowerCase();
    if (p.endsWith('.in') || ['flipkart', 'myntra', 'nykaa', 'beato', 'fastandup.in', 'ikea'].includes(p)) return 'INR';
    if (['amazon', 'walmart', 'target', 'ebay'].includes(p)) return 'USD';
    if (p === 'fast and up') return 'EUR';
    return 'INR';
  }

  changeIcon(a: any): string {
    if (a.direction === 'down')   return '📉';
    if (a.direction === 'up')     return '📈';
    if (a.direction === 'stable') return '➡️';
    return '🆕';
  }
  changeClass(a: any): string {
    if (a.direction === 'down')   return 'text-green';
    if (a.direction === 'up')     return 'text-red';
    if (a.direction === 'stable') return 'text-muted';
    return 'text-blue';
  }
  fmtChange(a: any): string {
    if (a.direction === 'new')    return 'New';
    if (a.direction === 'stable') return 'Stable';
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