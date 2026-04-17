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

  ngOnInit() { this.loadProducts(); }

  loadProducts() {
    // Load product names from entity_listings via sample-request endpoint
    this.svc.getSampleRequest(this.clientId).subscribe({
      next: (res: any) => {
        const names = (res?.scout_output?.products || []).map((p: any) => p.name);
        this.products.set(names);
      },
      error: () => { this.products.set([]); }
    });
  }

  lookup(name?: string) {
    const q = name || this.query().trim();
    if (!q) return;
    this.query.set(q);
    this.loading.set(true);
    this.error.set('');
    this.result.set(null);

    this.svc.getMarketTrend(q).subscribe({
      next: (res) => {
        this.result.set(res);
        this.history.update(h => [res, ...h.filter(x => x.product_name !== res.product_name)].slice(0, 10));
        this.loading.set(false);
      },
      error: (err) => {
        this.error.set('Product not found in price history. Please check the product name.');
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