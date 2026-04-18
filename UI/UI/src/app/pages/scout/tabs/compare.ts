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
    // If the price spread claims >99% savings, it's almost certainly a
    // cross-currency comparison (e.g. $1,249 "vs" ₹169,990 reads as "99.3%
    // cheaper" to naive math). Suppress to avoid misleading the user.
    if (entity.price_spread.diff_percent > 99) return null;
    return { amount: entity.price_spread.savings, pct: entity.price_spread.diff_percent };
  }

  /**
   * Whether this platform entry is the cheapest for the entity.
   *
   * Caveat: the backend's ComparePlatformEntry schema does not expose per-
   * platform currency — we can't fully validate same-currency comparison
   * client-side. What we CAN do: if the price range is absurdly wide
   * (>99% spread), treat the comparison as cross-currency noise and
   * refuse to pick a winner. Same defensive posture as savings().
   */
  isCheapest(entry: ComparePlatformEntry, entity: ComparableEntity): boolean {
    if (!entity.cheapest) return false;
    if (entity.price_spread && entity.price_spread.diff_percent > 99) return false;
    return entry.price === entity.cheapest.price;
  }

  /**
   * Guess the currency for a platform based on its name.
   *
   * WORKAROUND: The /compare/{query} API response does not include a
   * currency field per platform entry (ComparePlatformEntry has only
   * platform/price/url). Until the backend is updated, we guess from
   * platform name — same pattern used in monitor.ts for the alerts table.
   *
   * Known limitation: can't distinguish amazon.com-serving-INR (geo-
   * localized) from regular amazon.com. For amazon.com the scraper does
   * return INR in those cases, but this guess assumes USD.
   */
  currencyFor(platform: string): string {
    const p = (platform || '').toLowerCase();
    // Indian platforms
    if (p.endsWith('.in') || ['flipkart', 'myntra', 'nykaa', 'beato'].includes(p)) {
      return 'INR';
    }
    // US / global platforms
    if (['amazon', 'walmart', 'target', 'ebay'].includes(p)) {
      return 'USD';
    }
    // Default: assume INR (this deployment is India-facing)
    return 'INR';
  }

  formatPrice(val: number, currency: string = 'INR'): string {
    if (!val || val <= 0) return '—';
    const symbols: Record<string, string> = {
      INR: '₹', USD: '$', EUR: '€', GBP: '£', JPY: '¥',
      AUD: 'A$', CAD: 'C$', SGD: 'S$', AED: 'AED ',
    };
    return `${symbols[currency] || currency + ' '}${val.toLocaleString()}`;
  }
}