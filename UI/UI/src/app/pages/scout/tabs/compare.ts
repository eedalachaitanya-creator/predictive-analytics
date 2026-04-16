import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ScoutService, CompareResult, Listing, EntityGroup } from '../../../services/scout.service';

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

  bestInGroup(listings: Listing[]): Listing | null {
    const valid = listings.filter(l => l.price.value > 0);
    if (!valid.length) return null;
    return valid.reduce((min, l) => l.price.value < min.price.value ? l : min);
  }

  savings(group: Listing[]): { amount: number; pct: number } | null {
    const valid = group.filter(l => l.price.value > 0);
    if (valid.length < 2) return null;
    const prices = valid.map(l => l.price.value);
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    if (max === min) return null;
    return { amount: max - min, pct: ((max - min) / max) * 100 };
  }

  isBest(listing: Listing, group: Listing[]): boolean {
    const best = this.bestInGroup(group);
    return best !== null && listing.price.value === best.price.value;
  }

  formatPrice(val: number, currency: string = 'INR'): string {
    if (!val || val <= 0) return '—';
    const sym = currency === 'INR' ? '₹' : currency === 'USD' ? '$' : currency;
    return `${sym}${val.toLocaleString()}`;
  }
}