import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { StrategistService, MarketTrend } from '../../../services/strategist.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'strategist-trend',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './trend.html',
  styleUrls: ['./trend.scss']
})
export class StrategistTrendTab implements OnInit {
  private svc  = inject(StrategistService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  query    = signal('');
  loading  = signal(false);
  error    = signal('');
  result   = signal<MarketTrend | null>(null);
  history  = signal<MarketTrend[]>([]);
  products = signal<string[]>([]);

  // Autocomplete state
  suggestions     = signal<{ name: string; sku: string; saved_cost: number }[]>([]);
  suggestionsOpen = signal(false);
  private searchTimeout: any;

  ngOnInit() { this.loadProducts(); }

  loadProducts() {
    this.svc.getSampleRequest(this.clientId).subscribe({
      next: (res: any) => {
        const names = (res?.scout_output?.products || []).map((p: any) => p.name);
        this.products.set(names);
      },
      error: () => { this.products.set([]); }
    });
  }

  onInput(value: string) {
    this.query.set(value);

    if (this.searchTimeout) clearTimeout(this.searchTimeout);

    if (!value || value.trim().length < 2) {
      this.suggestions.set([]);
      this.suggestionsOpen.set(false);
      return;
    }

    this.searchTimeout = setTimeout(() => {
      this.svc.getPriceHistoryProducts(value.trim()).subscribe({
        next: (res: any) => {
          const names = (res.products || []).map((p: any) => ({ name: p, sku: p, saved_cost: 0 }));
          this.suggestions.set(names);
          this.suggestionsOpen.set(names.length > 0);
        },
        error: () => {
          this.suggestions.set([]);
          this.suggestionsOpen.set(false);
        }
      });
    }, 250);
  }

  pickSuggestion(name: string) {
    this.query.set(name);
    this.suggestions.set([]);
    this.suggestionsOpen.set(false);
    this.lookup(name);
  }

  onBlur() {
    setTimeout(() => this.suggestionsOpen.set(false), 200);
  }

  lookup(name?: string) {
    const q = name || this.query().trim();
    if (!q) return;
    this.query.set(q);
    this.suggestions.set([]);
    this.suggestionsOpen.set(false);
    this.loading.set(true);
    this.error.set('');
    this.result.set(null);

    this.svc.getMarketTrend(q).subscribe({
      next: (res) => {
        this.result.set(res);
        this.history.update(h => [res, ...h.filter(x => x.product_name !== res.product_name)].slice(0, 10));
        this.loading.set(false);
      },
      error: () => {
        this.error.set('Product not found in price history. Run the Price Monitor first to collect data.');
        this.loading.set(false);
      }
    });
  }

  trendIcon(t: string)  { return t === 'rising' ? '📈' : t === 'falling' ? '📉' : '➡️'; }
  trendColor(t: string) { return t === 'rising' ? 'green' : t === 'falling' ? 'red' : 'blue'; }
  trendDesc(t: string)  {
    if (t === 'rising')  return 'Competitor prices are rising — you can hold or increase your price.';
    if (t === 'falling') return 'Competitor prices are falling — consider reducing your price to stay competitive.';
    return 'Competitor prices are stable — standard pricing strategy applies.';
  }
}