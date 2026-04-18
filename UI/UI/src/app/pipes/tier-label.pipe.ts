import { Pipe, PipeTransform, inject } from '@angular/core';
import { TierLabelService } from '../services/tier-label.service';

/**
 * TierLabelPipe
 * -------------
 * Usage in templates:
 *   {{ customer.tier | tierLabel }}   →   '💎 Platinum'  (or '🚀 Elite' if renamed)
 *
 * Why pure:false
 *   Angular pipes default to pure — they only recompute when the INPUT changes.
 *   Our label map is stored in a signal that can change while the input
 *   ('Platinum') stays the same. `pure: false` makes Angular re-run this on
 *   every change detection cycle so a rename in Settings propagates instantly
 *   to every page that displays a tier.
 */
@Pipe({
  name: 'tierLabel',
  standalone: true,
  pure: false,
})
export class TierLabelPipe implements PipeTransform {
  private svc = inject(TierLabelService);

  transform(canonical: string | null | undefined): string {
    return this.svc.translate(canonical);
  }
}
