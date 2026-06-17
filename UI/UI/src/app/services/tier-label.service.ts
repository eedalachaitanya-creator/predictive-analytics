import { Injectable, inject, signal } from '@angular/core';
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

  /**
   * Translate a canonical tier name ('Platinum') to its display label ('💎 Platinum').
   *
   * The canonical value coming in is ALWAYS the bare enum from the DB column
   * `customer_tier`: 'Platinum', 'Gold', 'Silver', 'Bronze' (or lowercase/mixed
   * variants). It is never the display label itself — display labels live only
   * in `_labels` and are written there by refresh(). Normalizing to title-case
   * is therefore safe and correct: we only need to handle case variation in the
   * raw enum, not strip emojis or other display-only prefixes.
   */
  translate(canonical: string | null | undefined): string {
    if (!canonical) return '';

    const trimmed = canonical.trim();

    // Fast path: exact match (covers 'Platinum', 'Gold', etc. from the DB).
    if (this._labels()[trimmed]) return this._labels()[trimmed];

    // Slow path: case-insensitive match — handles 'platinum', 'GOLD', etc.
    // Only title-cases the FIRST word so 'platinum elite' → 'Platinum elite',
    // not 'Platinum Elite'. We match on the first word because the canonical
    // enum is always a single word.
    const firstWord = trimmed.split(/\s+/)[0];
    const titleFirst = firstWord.charAt(0).toUpperCase() + firstWord.slice(1).toLowerCase();

    return this._labels()[titleFirst] ?? canonical;
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