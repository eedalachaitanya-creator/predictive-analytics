import { Injectable, inject, signal, computed } from '@angular/core';
import { ApiService } from './api.service';
import { AuthService } from './auth.service';

/**
 * TierLabelService
 * ----------------
 * Single source of truth for the FOUR per-client tier display names
 * (Platinum / Gold / Silver / Bronze).
 *
 * The database stores a canonical enum in `customer_tier` ('Platinum'/'Gold'/
 * 'Silver'/'Bronze'). Clients get to rename the *displayed* form on the
 * Settings page (e.g., "Platinum" → "🚀 Elite"). This service fetches the
 * 4 labels from `/api/v1/settings` once, caches them in a signal, and
 * lets any component look them up.
 *
 * When Settings.save() writes new labels, it calls `refresh()` so every
 * other open page updates without a full reload.
 */
@Injectable({ providedIn: 'root' })
export class TierLabelService {
  private api  = inject(ApiService);
  private auth = inject(AuthService);

  // Backing state — defaults match the DB DEFAULTs so UI works before first fetch.
  private _labels = signal<Record<string, string>>({
    Platinum: '💎 Platinum',
    Gold:     '🥇 Gold',
    Silver:   '🥈 Silver',
    Bronze:   '🥉 Bronze',
  });

  /** Read-only signal — components can subscribe to re-render on change. */
  readonly labels = this._labels.asReadonly();

  /** Translate a canonical tier name ('Platinum') to its display label ('💎 Platinum'). */
  translate(canonical: string | null | undefined): string {
    if (!canonical) return '';
    // Normalize: strip whitespace, capitalize first letter so 'platinum'/'PLATINUM' → 'Platinum'.
    const key = canonical.trim();
    const normalized = key.charAt(0).toUpperCase() + key.slice(1).toLowerCase();
    return this._labels()[normalized] ?? canonical;
  }

  /**
   * Pull the 4 tier labels from the backend for the currently active client.
   * Safe to call on app start and again after Settings.save().
   */
  refresh(): void {
    const clientId = this.auth.getClientId();
    if (!clientId) return;  // no client → nothing to fetch

    this.api.get<any>(`/settings?clientId=${clientId}`).subscribe({
      next: (cfg) => {
        this._labels.set({
          Platinum: cfg.tier_label_platinum ?? '💎 Platinum',
          Gold:     cfg.tier_label_gold     ?? '🥇 Gold',
          Silver:   cfg.tier_label_silver   ?? '🥈 Silver',
          Bronze:   cfg.tier_label_bronze   ?? '🥉 Bronze',
        });
      },
      // Soft-fail on purpose — keep defaults in the signal so UI never breaks.
      error: () => { /* silently ignore; defaults stand */ },
    });
  }
}
