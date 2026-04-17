import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { StrategistService, StrategistRequest, PricingRecommendation, ScoutProduct } from '../../../services/strategist.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'strategist-recommend',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './recommend.html',
  styleUrls: ['./recommend.scss']
})
export class StrategistRecommendTab implements OnInit {
  private svc  = inject(StrategistService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  products      = signal<{ name: string; cost: string; listings: string }[]>([]);
  savedCosts    = signal<Record<string, number>>({});
  useChurn      = signal(false);
  churnJson     = signal('');
  targetMargin  = signal(20);
  minMargin     = signal(8);
  undercutPct   = signal(2);

  loading       = signal(false);
  loadingSample = signal(true);
  error         = signal('');
  results       = signal<PricingRecommendation[]>([]);
  runMeta       = signal<{ run_id: string; elapsed: number; retention_count: number } | null>(null);

  ngOnInit() { this.loadSample(); }

  loadSample() {
    this.loadingSample.set(true);
    // Load Scout products and saved costs in parallel
    this.svc.getSampleRequest(this.clientId).subscribe({
      next: (res: any) => {
        const mapped = (res.scout_output?.products || []).map((p: ScoutProduct) => ({
          name: p.name, cost: '', listings: JSON.stringify(p.listings)
        }));
        this.products.set(mapped.length ? mapped : [{ name: '', cost: '', listings: '' }]);

        // Load saved cost prices to show in results
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
        this.products.set([{ name: '', cost: '', listings: '' }]);
        this.loadingSample.set(false);
      }
    });
  }

  addProduct()             { this.products.update(p => [...p, { name: '', cost: '', listings: '' }]); }
  removeProduct(i: number) { this.products.update(p => p.filter((_, idx) => idx !== i)); }

  updateProduct(i: number, field: 'name' | 'cost' | 'listings', value: string) {
    this.products.update(p => {
      const updated = [...p];
      updated[i] = { ...updated[i], [field]: value };
      return updated;
    });
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
      client_id: this.clientId,
      scout_output: { status: 'ok', products: scoutProducts },
      our_costs: ourCosts,
      target_margin_pct: this.targetMargin(),
      min_margin_pct: this.minMargin(),
      undercut_pct: this.undercutPct(),
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

  // Get saved cost price for a product from DB (product_costs table)
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

  trendIcon(t: string)  { return t === 'rising' ? '📈' : t === 'falling' ? '📉' : '➡️'; }
  fmtPrice(n: number)   { return n ? '₹' + n.toFixed(2) : '—'; }
  fmtPct(n: number)     { return (n || 0).toFixed(1) + '%'; }
}