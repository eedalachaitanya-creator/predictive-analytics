import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ScoutService, CompareResult, ComparableEntity, ComparePlatformEntry } from '../../../services/scout.service';

@Component({
  selector: 'scout-compare',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './compare.html',
  styleUrls: ['./compare.scss']
})
export class ScoutCompareTab {
  private svc = inject(ScoutService);

  query     = signal('');
  loading   = signal(false);
  result    = signal<CompareResult | null>(null);
  error     = signal('');

  compare() {
    const q = this.query().trim();
    if (!q || this.loading()) return;

    this.loading.set(true);
    this.error.set('');
    this.result.set(null);

    this.svc.compareProducts(q).subscribe({
      next: res => {
        this.result.set(res);
        this.loading.set(false);
      },
      error: err => {
        this.error.set(err.message || 'Compare failed');
        this.loading.set(false);
      }
    });
  }

  onKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') this.compare();
  }

  savings(entity: ComparableEntity): { amount: number; pct: number } | null {
    if (!entity.price_spread) return null;
    return { amount: entity.price_spread.savings, pct: entity.price_spread.diff_percent };
  }

  isCheapest(entry: ComparePlatformEntry, entity: ComparableEntity): boolean {
    return entity.cheapest !== null && entry.price === entity.cheapest.price;
  }

  formatPrice(val: number, currency: string = 'USD'): string {
    if (!val || val <= 0) return '—';
    const symbols: Record<string, string> = {
      INR: '₹', USD: '$', EUR: '€', GBP: '£', JPY: '¥',
      AUD: 'A$', CAD: 'C$', SGD: 'S$', AED: 'AED ',
    };
    return `${symbols[currency] || currency + ' '}${val.toLocaleString()}`;
  }
}