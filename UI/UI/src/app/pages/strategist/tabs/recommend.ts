import { Component, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { StrategistService, StrategistRequest, PricingRecommendation, ScoutProduct } from '../../../services/strategist.service';
import { AuthService } from '../../../services/auth.service';

interface ProductRow {
  name:     string;
  cost:     string;
  listings: string;            // raw JSON kept internally — never shown to client
  platforms: { name: string; price: number }[];  // parsed for display only
}

@Component({
  selector: 'strategist-recommend',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './recommend.html',
  styleUrls: ['./recommend.scss']
})
export class StrategistRecommendTab {
  private svc  = inject(StrategistService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  constructor() {
    // Pre-fill currency dropdown from client_config (silent fallback to INR)
    this.svc.getClientConfig(this.clientId).subscribe({
      next: (cfg: any) => {
        if (cfg?.currency) this.currency.set(cfg.currency);
      },
      error: () => { /* keep INR default */ }
    });
  }

  // products      = signal<ProductRow[]>([]);
  products      = signal<ProductRow[]>([{ name: '', cost: '', listings: '', platforms: [] }]);
  savedCosts    = signal<Record<string, number>>({});
  useChurn      = signal(false);
  churnJson     = signal('');
  targetMargin  = signal(20);
  minMargin     = signal(8);
  undercutPct   = signal(2);

  loading       = signal(false);
  loadingSample = signal(false);
  error         = signal('');
  results       = signal<PricingRecommendation[]>([]);
  runMeta       = signal<{ run_id: string; elapsed: number; retention_count: number } | null>(null);

  // Currency — prefilled from client_config.currency, user can override per request
  currency        = signal('INR');
  currencyOptions = ['INR', 'USD', 'EUR', 'GBP', 'AED', 'SGD'];

  // Autocomplete state — suggestions[i] = list for product row i
  suggestions     = signal<Record<number, { name: string; sku: string; saved_cost: number }[]>>({});
  suggestionsOpen = signal<number | null>(null);   // which row's dropdown is visible
  private searchTimeouts: Record<number, any> = {};
  // ngOnInit() { this.loadSample(); }

  loadSample() {
    this.loadingSample.set(true);
    this.svc.getSampleRequest(this.clientId).subscribe({
      next: (res: any) => {
        const mapped = (res.scout_output?.products || []).map((p: ScoutProduct) => {
          const listings = p.listings || [];
          const platforms = listings.map((l: any) => ({
            name:  l.platform,
            price: l.price?.value || 0
          })).filter((l: any) => l.price > 0);
          return {
            name:     p.name,
            cost:     '',
            listings: JSON.stringify(listings),   // kept internally for API call
            platforms,
          };
        });
        this.products.set(mapped.length ? mapped : [{ name: '', cost: '', listings: '', platforms: [] }]);

        this.svc.getCosts(this.clientId).subscribe({
          next: (costsRes: any) => {
            const costs: Record<string, number> = {};
            for (const c of (costsRes.costs || [])) costs[c.product_name] = c.cost_usd;
            this.savedCosts.set(costs);
          },
          error: () => {}
        });
        this.loadingSample.set(false);
      },
      error: () => {
        this.products.set([{ name: '', cost: '', listings: '', platforms: [] }]);
        this.loadingSample.set(false);
      }
    });
  }

  addProduct() {
    this.products.update(p => [...p, { name: '', cost: '', listings: '', platforms: [] }]);
  }

  removeProduct(i: number) {
    this.products.update(p => p.filter((_, idx) => idx !== i));
  }

  updateProduct(i: number, field: 'name' | 'cost', value: string) {
    this.products.update(p => {
      const updated = [...p];
      updated[i] = { ...updated[i], [field]: value };
      return updated;
    });

    // Trigger autocomplete only on name field changes — debounced 250ms
    if (field === 'name') {
      this.debouncedSearch(i, value);
    }
  }

  private debouncedSearch(i: number, q: string) {
    // Cancel any pending search for this row
    if (this.searchTimeouts[i]) clearTimeout(this.searchTimeouts[i]);

    // Empty query → hide dropdown
    if (!q || q.trim().length < 2) {
      this.suggestionsOpen.set(null);
      return;
    }

    // 250ms debounce — avoid hitting backend on every keystroke
    this.searchTimeouts[i] = setTimeout(() => {
      this.svc.searchProducts(this.clientId, q.trim()).subscribe(res => {
        this.suggestions.update(s => ({ ...s, [i]: res.products || [] }));
        this.suggestionsOpen.set(res.products?.length ? i : null);
      });
    }, 250);
  }

  /** User clicked a suggestion — fill the product name (and saved cost if any) */
  pickSuggestion(i: number, name: string, savedCost?: number) {
    this.products.update(p => {
      const updated = [...p];
      // Pre-fill saved cost if available and the user hasn't typed one
      const cost = (savedCost && savedCost > 0 && !updated[i].cost) ? String(savedCost) : updated[i].cost;
      updated[i] = { ...updated[i], name, cost };
      return updated;
    });
    this.suggestionsOpen.set(null);
  }

  /** Close dropdown when user tabs/clicks away */
  closeSuggestions() {
    // Small timeout so click on suggestion fires BEFORE blur hides it
    setTimeout(() => this.suggestionsOpen.set(null), 150);
  }

  run() {
    this.error.set('');
    const prods = this.products();
    const scoutProducts = prods.map(p => {
      let listings = [];
      try { listings = JSON.parse(p.listings || '[]'); } catch { listings = []; }
      return { name: p.name.trim(), listings };
    }).filter(p => p.name);

    if (!scoutProducts.length) { this.error.set('Add at least one product.'); return; }

    const ourCosts: Record<string, number> = {};
    prods.forEach(p => {
      if (p.name.trim() && p.cost) ourCosts[p.name.trim()] = parseFloat(p.cost);
    });

    const req: StrategistRequest = {
      client_id:         this.clientId,
      scout_output:      { status: 'ok', products: scoutProducts },
      our_costs:         ourCosts,
      target_margin_pct: this.targetMargin(),
      min_margin_pct:    this.minMargin(),
      undercut_pct:      this.undercutPct(),
      currency:          this.currency(),
    };

    if (this.useChurn() && this.churnJson().trim()) {
      try { req.churn_batch = JSON.parse(this.churnJson()); }
      catch { this.error.set('Invalid churn data format.'); return; }
    }

    this.loading.set(true);
    this.results.set([]);
    this.runMeta.set(null);

    this.svc.recommend(req).subscribe({
      next: (res) => {
        this.results.set(res.recommendations || []);
        this.runMeta.set({ run_id: res.run_id, elapsed: res.elapsed_seconds, retention_count: res.retention_count });
        this.loading.set(false);
      },
      error: (err) => {
        this.error.set(err?.error?.detail || err?.message || 'Something went wrong. Please try again.');
        this.loading.set(false);
      }
    });
  }

  getCostPrice(productName: string): number | null {
    return this.savedCosts()[productName] || null;
  }

  strategyColor(s: string) {
    if (s === 'retention') return 'purple';
    if (s === 'undercut')  return 'green';
    if (s === 'match')     return 'blue';
    if (s === 'premium')   return 'yellow';
    return 'gray';
  }

  trendIcon(t: string) { return t === 'rising' ? '📈' : t === 'falling' ? '📉' : '➡️'; }
  fmtPrice(n: number)  { return n ? '₹' + n.toFixed(2) : '—'; }
  fmtPct(n: number)    { return (n || 0).toFixed(1) + '%'; }
  fmtProb(n: number)   { return ((n || 0) * 100).toFixed(1) + '%'; }
  platformIcon(p: string) {
    const icons: Record<string, string> = {
      amazon: '🛒', flipkart: '🏪', meesho: '🛍', myntra: '👗', snapdeal: '🏷'
    };
    return icons[p?.toLowerCase()] || '🌐';
  }
}