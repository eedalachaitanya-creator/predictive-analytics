import { Component, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { StrategistService, StrategistRequest, PricingRecommendation } from '../../../services/strategist.service';
import { ScoutService } from '../../../services/scout.service';
import { AuthService } from '../../../services/auth.service';

interface ProductRow {
  name:        string;   // query sent to backend (Scout search term)
  displayName: string;   // canonical name shown to user in input field
  cost:        string;
  currency:    string;   // currency the cost is entered in (drives market currency)
  listings:    string;
  platforms:   { name: string; price: number }[];
}

@Component({
  selector: 'strategist-recommend',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './recommend.html',
  styleUrls: ['./recommend.scss']
})
export class StrategistRecommendTab {
  private svc   = inject(StrategistService);
  private scout = inject(ScoutService);
  private auth  = inject(AuthService);
  clientId      = this.auth.getClientId();

  constructor() {
    // Pre-fill cost currency per product from client_config default.
    this.svc.getClientConfig(this.clientId).subscribe({
      next: (cfg: any) => {
        const cur = cfg?.currency || 'INR';
        this.products.update(rows => rows.map(r => ({ ...r, currency: cur })));
      },
      error: () => { /* keep INR default on each product row */ }
    });
    // Load websites so platformIcon() can read icons from DB via the signal.
    // subscribe() is called for the side-effect only; the signal update
    // inside ScoutService is what matters.
    this.scout.loadWebsites().subscribe();

    // Load pricing parameter defaults from backend schema.
    // Keeps frontend in sync with StrategistRequest field definitions.
    this.svc.getDefaults().subscribe({
      next: (d) => {
        if (d['target_margin_pct'] != null) this.targetMargin.set(d['target_margin_pct']);
        if (d['min_margin_pct']    != null) this.minMargin.set(d['min_margin_pct']);
        if (d['undercut_pct']      != null) this.undercutPct.set(d['undercut_pct']);
      },
      error: () => { /* keep placeholder defaults */ }
    });
  }

  products      = signal<ProductRow[]>([{ name: '', displayName: '', cost: '', currency: 'INR', listings: '', platforms: [] }]);
  savedCosts    = signal<Record<string, number>>({});
  // Defaults loaded from GET /api/strategist/defaults on init.
  // Signal values below are placeholders only, overwritten in constructor.
  targetMargin  = signal(20);
  minMargin     = signal(8);
  undercutPct   = signal(2);

  loading       = signal(false);
  error         = signal('');
  results       = signal<PricingRecommendation[]>([]);
  runMeta       = signal<{ run_id: string; elapsed: number; retention_count: number } | null>(null);
  savedMsg      = signal('');

  // responseCurrency — set from res.currency after a successful API call.
  // This is what fmtPrice() uses to display symbols, so the symbol always
  // matches the actual currency the backend computed prices in, not just
  // whatever the user has currently selected in the dropdown.
  responseCurrency = signal('INR');

  // Single source of truth for currency metadata.
  // To add a new currency: add one entry here. The dropdown and symbol
  // formatter both read from this — no other changes needed.
  readonly CURRENCIES: { code: string; symbol: string }[] = [
    { code: 'INR', symbol: '₹'    },
    { code: 'USD', symbol: '$'    },
    { code: 'EUR', symbol: '€'    },
    { code: 'GBP', symbol: '£'    },
    { code: 'AED', symbol: 'AED ' },
    { code: 'SGD', symbol: 'S$'   },
  ];
  get currencyOptions(): string[] { return this.CURRENCIES.map(c => c.code); }

  // Autocomplete state — suggestions[i] = list for product row i
  suggestions     = signal<Record<number, { name: string; sku: string; saved_cost: number }[]>>({});
  suggestionsOpen = signal<number | null>(null);
  private searchTimeouts: Record<number, any> = {};

  loadSample() {
    const cur = this.products()[0]?.currency || 'INR';
    this.products.set([{ name: '', displayName: '', cost: '', currency: cur, listings: '', platforms: [] }]);
    this.results.set([]);
    this.runMeta.set(null);
    this.error.set('');
    this.savedMsg.set('');
    this.savedCosts.set({});
  }

  addProduct() {
    // New rows inherit the currency of the first existing row
    const cur = this.products()[0]?.currency || 'INR';
    this.products.update(p => [...p, { name: '', displayName: '', cost: '', currency: cur, listings: '', platforms: [] }]);
  }

  removeProduct(i: number) {
    this.products.update(p => p.filter((_, idx) => idx !== i));
  }

  updateProduct(i: number, field: 'name' | 'cost' | 'currency', value: string) {
    this.products.update(p => {
      const updated = [...p];
      if (field === 'name') {
        // When user types manually, keep displayName in sync
        updated[i] = { ...updated[i], name: value, displayName: value };
      } else {
        updated[i] = { ...updated[i], [field]: value };
      }
      return updated;
    });

    if (field === 'name') {
      this.debouncedSearch(i, value);
    }
  }

  private debouncedSearch(i: number, q: string) {
    if (this.searchTimeouts[i]) clearTimeout(this.searchTimeouts[i]);

    if (!q || q.trim().length < 2) {
      this.suggestionsOpen.set(null);
      return;
    }

    this.searchTimeouts[i] = setTimeout(() => {
      this.svc.searchProducts(this.clientId, q.trim()).subscribe(res => {
        this.suggestions.update(s => ({ ...s, [i]: res.products || [] }));
        this.suggestionsOpen.set(res.products?.length ? i : null);
      });
    }, 250);
  }

  /** User clicked a suggestion:
   *  - displayName = canonical name shown in input (readable)
   *  - name = Scout query term sent to backend (matches more entities via trigram)
   */
  pickSuggestion(i: number, name: string, savedCost?: number, query?: string) {
    this.products.update(p => {
      const updated = [...p];
      const cost = (savedCost && savedCost > 0 && !updated[i].cost) ? String(savedCost) : updated[i].cost;
      updated[i] = {
        ...updated[i],
        name:        query || name,   // query used for DB matching
        displayName: name,            // canonical name shown to user
        cost,
      };
      return updated;
    });
    this.suggestionsOpen.set(null);
  }

  closeSuggestions() { this.suggestionsOpen.set(null); }

  onNameBlur() {
    setTimeout(() => this.suggestionsOpen.set(null), 200);
  }

  run() {
    this.error.set('');
    // Reset responseCurrency to what the user has selected right now.
    // This ensures noDataMessage() shows the correct currency even before
    // the API responds, and is overwritten by res.currency on success.
    this.responseCurrency.set(this.products()[0]?.currency || 'INR');
    if (!this.clientId) {
      this.error.set('No client selected. Please select a client from the top menu.');
      return;
    }
    const prods = this.products();
    const scoutProducts = prods.map(p => {
      let listings = [];
      try { listings = JSON.parse(p.listings || '[]'); } catch { listings = []; }
      return { name: p.name.trim(), listings };
    }).filter(p => p.name);

    if (!scoutProducts.length) { this.error.set('Add at least one product.'); return; }

    const ourCosts: Record<string, number> = {};
    for (const p of prods) {
      if (!p.name.trim() || !p.cost) continue;
      const v = parseFloat(p.cost);
      if (isNaN(v) || v <= 0) {
        this.error.set(`Cost for "${p.displayName || p.name}" must be a valid number greater than zero.`);
        return;
      }
      ourCosts[p.name.trim()] = v;
    }

    if (this.minMargin() < 0 || this.minMargin() > 200) {
      this.error.set('Minimum margin must be between 0% and 200%.');
      return;
    }

    if (this.targetMargin() < 0 || this.targetMargin() > 200) {
      this.error.set('Target margin must be between 0% and 200%.');
      return;
    }

    if (this.minMargin() >= this.targetMargin()) {
      this.error.set('Minimum margin must be less than target margin.');
      return;
    }

    const req: StrategistRequest = {
      client_id:         this.clientId,
      scout_output:      { status: 'ok', products: scoutProducts },
      our_costs:         ourCosts,
      target_margin_pct: this.targetMargin(),
      min_margin_pct:    this.minMargin(),
      undercut_pct:      this.undercutPct(),
      // Currency comes from the product's cost currency.
      // All products in one run must share the same currency.
      currency:          this.products()[0]?.currency || 'INR',
      skip_churn:        true,
      client_priority:   null,   // always default
      customer_segment:  null,   // always default
    };

    this.loading.set(true);
    this.results.set([]);
    this.runMeta.set(null);

    this.svc.recommend(req).subscribe({
      next: (res) => {
        // Lock the displayed currency to what the backend actually used.
        // This is the source of truth — not the dropdown signal.
        this.responseCurrency.set(res.currency);
        // Map product_name back to displayName for UI
        const displayMap: Record<string, string> = {};
        this.products().forEach(p => {
          if (p.name) displayMap[p.name.trim()] = p.displayName || p.name;
        });
        const recs = (res.recommendations || []).map(r => ({
          ...r,
          product_name: displayMap[r.product_name] || r.product_name
        }));
        this.results.set(recs);
        this.runMeta.set({ run_id: res.run_id, elapsed: res.elapsed_seconds, retention_count: res.retention_count });
        this.loading.set(false);
      },
      error: (err) => {
        this.error.set(this.extractErrorMessage(err));
        this.loading.set(false);
      }
    });
  }

  /**
   * Turn an HttpErrorResponse into a human-readable string.
   *
   * FastAPI validation errors (HTTP 422) return `detail` as an ARRAY of
   * Pydantic error objects (e.g. [{ loc, msg, type, input }]), not a string.
   * Binding that array directly to the template (`{{ error() }}`) makes
   * Angular call Array.prototype.toString(), which renders each element via
   * Object.prototype.toString() — producing the literal text "[object
   * Object]". This helper extracts the actual `msg` field(s) so the user
   * sees the real validation message (e.g. "Input should be greater than or
   * equal to 0") instead.
   */
  private extractErrorMessage(err: any): string {
    const detail = err?.error?.detail;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
      const msgs = detail.map((d: any) => d?.msg || JSON.stringify(d)).filter(Boolean);
      if (msgs.length) return msgs.join(' ');
    }
    if (detail && typeof detail === 'object' && detail.msg) return detail.msg;
    return err?.message || 'Something went wrong. Please try again.';
  }

  saveCosts() {
    const costs: Record<string, number> = {};
    for (const p of this.products()) {
      if (!p.name.trim() || !p.cost) continue;
      const v = parseFloat(p.cost);
      if (!isNaN(v) && v > 0) costs[p.name.trim()] = v;
    }
    if (!Object.keys(costs).length) {
      this.savedMsg.set('❌ No valid costs to save.');
      return;
    }
    this.svc.saveCosts(this.clientId, costs).subscribe({
      next: () => {
        this.savedMsg.set('✅ Costs saved successfully.');
        setTimeout(() => this.savedMsg.set(''), 3000);
      },
      error: () => {
        this.savedMsg.set('❌ Failed to save costs.');
        setTimeout(() => this.savedMsg.set(''), 3000);
      }
    });
  }

  getCostPrice(productName: string): number | null {
    return this.savedCosts()[productName] || null;
  }

  strategyColor(s: string) {
    if (s === 'retention')  return 'purple';
    if (s === 'undercut')   return 'green';
    if (s === 'match')      return 'blue';
    if (s === 'premium')    return 'yellow';
    if (s === 'floor_only') return 'orange';
    return 'gray';
  }

  /**
   * Returns true when the recommended price is driven by competitor market
   * data rather than the user's margin settings — so the banner explaining
   * "adjust Undercut By %" is relevant to show.
   *
   * Two cases where this is true:
   *  1. strategy = 'undercut'  — price is always comp_min × (1 - undercut%).
   *     Target/min margin had no effect.
   *  2. strategy = 'match' AND suggested === competitor_min — our target margin
   *     was above the market floor so we capped to it. Margin settings still
   *     had no effect on the final number.
   *
   * All other strategies (floor_only, premium, retention, no_data) are driven
   * by cost, positioning, or churn — not competitor anchoring — so no hint.
   */
  showMarketHint(rec: PricingRecommendation): boolean {
    if (rec.strategy === 'undercut') return true;
    if (rec.strategy === 'match' && rec.suggested_price === rec.competitor_min) return true;
    return false;
  }

  trendIcon(t: string) { return t === 'rising' ? '📈' : t === 'falling' ? '📉' : '➡️'; }

  noDataMessage(rec: PricingRecommendation): string {
    // responseCurrency is reset to the selected currency at the start of each
    // run(), so it always reflects what the current results were computed in.
    const cur = this.responseCurrency();
    switch (rec.flag) {
      case 'no_price_data':
        return `No ${cur}-priced competitor data found for this product. `
             + `This product may only be available on platforms that price in a different currency. `
             + `Try switching to INR if it is sold on Indian platforms.`;
      case 'no_cost_data':
        return 'No cost entered for this product. Enter your cost in the field on the left to get a recommendation.';
      case 'invalid_cost_data':
        return 'The cost entered looks too low (less than 1). Please check and re-enter your cost.';
      default:
        return 'Not enough data to generate a recommendation for this product.';
    }
  }

  fmtPrice(n: number) {
    const entry = this.CURRENCIES.find(c => c.code === this.responseCurrency());
    const sym = entry ? entry.symbol : this.responseCurrency();
    return n ? sym + n.toFixed(2) : '—';
  }

  fmtPct(n: number)  { return (n || 0).toFixed(1) + '%'; }
  fmtProb(n: number) { return ((n || 0) * 100).toFixed(1) + '%'; }

  platformIcon(name: string): string {
    // Icon is a property of the platform stored in the DB (websites.icon).
    // DB column DEFAULT 🌐 so every platform always has one.
    // The ?? fallback here only fires if the websites signal hasn't loaded yet.
    const site = this.scout.websites().find(
      w => w.name.toLowerCase() === name?.toLowerCase()
    );
    return site?.icon ?? '🌐';
  }
}