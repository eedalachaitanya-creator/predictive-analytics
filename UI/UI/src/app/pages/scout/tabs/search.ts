import { Component, computed, effect, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import { ScoutService, Listing, SearchResult } from '../../../services/scout.service';

@Component({
  selector: 'scout-search',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './search.html',
  styleUrls: ['./search.scss']
})
export class ScoutSearchTab implements OnInit {
  private svc = inject(ScoutService);

  mode       = signal<'single' | 'bulk'>('single');
  query      = signal('');
  // platforms is now a computed — derives from the shared websites signal.
  // When Platforms tab adds/removes/toggles a website, this recomputes
  // automatically and the chip list updates without a page refresh.
  platforms  = computed(() => this.svc.activePlatformNames());

  selected   = signal<Set<string>>(new Set());
  searching  = signal(false);
  results    = signal<SearchResult[]>([]);
  error      = signal('');
  expandedRow = signal<number | null>(null);
  bulkFile     = signal<File | null>(null);
  bulkDragover = signal(false);
  // One-time flag: initialize `selected` to "all active" the first time
  // platforms resolves to a non-empty list. After that, user's chip toggles
  // are preserved even when new platforms appear.
  private initialized = false;

  // ── Cancellation state ──────────────────────────────────────────
  // currentRequestId is the UUID we sent with the in-flight search.
  // currentSubscription is the RxJS subscription so we can also abort
  // the HTTP wait on the frontend (otherwise the browser would still
  // wait for the response we no longer care about).
  private currentRequestId: string | null = null;
  private currentSubscription: Subscription | null = null;
  // Set to true *only* when the user clicked Cancel. Used to suppress
  // the noisy error toast that would otherwise show when the HTTP
  // request gets aborted.
  private wasCancelled = false;

  constructor() {
    // Reactively keep the selection in sync with the active platform list:
    // - remove selections for platforms that were deleted or disabled
    // - on first non-empty list, auto-select everything (default behavior)
    effect(() => {
      const active = this.platforms();
      if (!active.length) return;

      if (!this.initialized) {
        this.selected.set(new Set(active));
        this.initialized = true;
        return;
      }

      // Prune any selected platforms that no longer exist. Keep the rest.
      const activeSet = new Set(active);
      const pruned = new Set([...this.selected()].filter(p => activeSet.has(p)));
      if (pruned.size !== this.selected().size) {
        this.selected.set(pruned);
      }
    });
  }

  ngOnInit() {
    // Ask the shared service to fetch platforms. The computed signal above
    // will pick up the result. Safe to call even if another component already
    // triggered a refresh — the signal just updates once.
    this.svc.refreshPlatforms();
  }

  togglePlatform(name: string) {
    const s = new Set(this.selected());
    s.has(name) ? s.delete(name) : s.add(name);
    this.selected.set(s);
  }
  selectAll() { this.selected.set(new Set(this.platforms())); }
  selectNone() { this.selected.set(new Set()); }

  search() {
    const q = this.query().trim();
    if (!q || this.searching()) return;

    // Generate a request_id that the backend uses to register this search
    // in its cancellation registry. crypto.randomUUID is widely supported
    // (Chrome 92+, Firefox 95+, Safari 15.4+) — covers the targeted dev/user
    // browsers. No polyfill needed.
    this.currentRequestId = crypto.randomUUID();
    this.wasCancelled = false;

    this.searching.set(true);
    this.error.set('');
    this.results.set([]);
    this.expandedRow.set(null);

    this.currentSubscription = this.svc.searchProducts(
      q,
      [...this.selected()],
      false,
      this.currentRequestId,
    ).subscribe({
      next: (res: any) => {
        // Backend signals user cancellation with status='cancelled' and
        // empty products. No error toast — the user knows they cancelled.
        if (res?.status === 'cancelled') {
          this.results.set([]);
        } else {
          this.results.set(res.products);
        }
        this.resetSearchState();
      },
      error: (err: any) => {
        // If the user clicked Cancel, the HTTP request gets aborted on
        // unsubscribe. RxJS may emit an error here — don't show it as a
        // "Search failed" toast; the cancel was intentional.
        if (!this.wasCancelled) {
          this.error.set(err.message || 'Search failed');
        }
        this.resetSearchState();
      },
    });
  }

  /**
   * Cancel an in-flight search. Tells the backend to stop scraping AND
   * unsubscribes from the HTTP request locally so the UI returns to ready
   * state immediately.
   */
  cancel() {
    if (!this.currentRequestId || !this.searching()) return;

    const id = this.currentRequestId;
    this.wasCancelled = true;

    // Tell the backend to stop. Fire-and-forget — we don't await this.
    // The backend's scraper checkpoints will see the cancel signal within
    // 1-3 seconds and abandon the search.
    this.svc.cancelSearch(id).subscribe({
      next: () => { /* fine — backend acknowledged */ },
      error: () => { /* fine — backend already finished or never had it */ },
    });

    // Locally: stop waiting for the response. This makes the UI snappy —
    // the user doesn't have to watch the spinner for 1-3s while the
    // backend cleans up.
    this.currentSubscription?.unsubscribe();
    this.resetSearchState();
  }

  /**
   * Reset all per-search state. Called from both successful completion and
   * cancellation paths so we never have stale request_ids hanging around.
   */
  private resetSearchState() {
    this.searching.set(false);
    this.currentRequestId = null;
    this.currentSubscription = null;
  }

  onKeydown(e: KeyboardEvent) { if (e.key === 'Enter') this.search(); }

  onFileSelect(e: Event) { const f = (e.target as HTMLInputElement).files; if (f?.length) this.bulkFile.set(f[0]); }
  onDragOver(e: DragEvent) { e.preventDefault(); this.bulkDragover.set(true); }
  onDragLeave() { this.bulkDragover.set(false); }
  onDrop(e: DragEvent) { e.preventDefault(); this.bulkDragover.set(false); if (e.dataTransfer?.files.length) this.bulkFile.set(e.dataTransfer.files[0]); }
  clearFile() { this.bulkFile.set(null); }

  searchBulk() {
    const file = this.bulkFile();
    if (!file || this.searching()) return;

    // Same cancellation pattern as single search — generate a request_id,
    // pass it to the upload, and wire up Cancel via shared cancel() method.
    this.currentRequestId = crypto.randomUUID();
    this.wasCancelled = false;

    this.searching.set(true);
    this.error.set('');
    this.results.set([]);

    this.currentSubscription = this.svc.uploadBulk(
      file,
      [...this.selected()],
      this.currentRequestId,
    ).subscribe({
      next: (res: any) => {
        if (res?.status === 'cancelled') {
          this.results.set([]);
        } else {
          this.results.set(res.products);
        }
        this.resetSearchState();
      },
      error: (err: any) => {
        if (!this.wasCancelled) {
          this.error.set(err.message || 'Bulk search failed');
        }
        this.resetSearchState();
      },
    });
  }

  toggleExpand(i: number) { this.expandedRow.set(this.expandedRow() === i ? null : i); }

  bestPrice(listings: Listing[]): Listing | null {
    const valid = listings.filter(l => l.price.value > 0);
    if (!valid.length) return null;

    // Count listings per currency
    const counts = new Map<string, number>();
    for (const l of valid) {
      const c = l.price.currency || 'USD';
      counts.set(c, (counts.get(c) || 0) + 1);
    }

    // Find the currency with the highest count.
    // Rationale: comparing raw numeric values across currencies is meaningless
    // ($1,249.97 vs ₹169,990 — the smaller number is NOT the cheaper product).
    // We pick whichever currency appears in the most listings, then return the
    // cheapest listing in that currency. If there is no majority (a tie between
    // currencies), we return null — the UI then shows no "Best" badge, which
    // is more honest than picking an arbitrary winner.
    // Single-listing case: that listing wins trivially.
    let majorityCurrency: string | null = null;
    let maxCount = 0;
    let hasTie = false;
    for (const [curr, count] of counts.entries()) {
      if (count > maxCount) {
        majorityCurrency = curr;
        maxCount = count;
        hasTie = false;
      } else if (count === maxCount) {
        hasTie = true;
      }
    }

    // True tie with no majority → no meaningful "best"
    if (hasTie || !majorityCurrency) return null;

    // Cheapest within the majority currency
    const majorityListings = valid.filter(l => l.price.currency === majorityCurrency);
    return majorityListings.reduce((m, l) => l.price.value < m.price.value ? l : m);
  }

  // A listing is "best" only if it matches the currency AND value of bestPrice().
  // Listings in a non-majority currency never show as best.
  isBest(l: Listing, all: Listing[]): boolean {
    const b = this.bestPrice(all);
    return !!b
      && l.price.currency === b.price.currency
      && l.price.value === b.price.value
      && l.price.value > 0;
  }

  fmt(p: any): string {
    if (!p || !p.value || p.value <= 0) return '—';
    const symbols: Record<string, string> = {
      INR: '₹', USD: '$', EUR: '€', GBP: '£', JPY: '¥',
      AUD: 'A$', CAD: 'C$', SGD: 'S$', AED: 'AED ',
    };
    return (symbols[p.currency] || p.currency + ' ') + p.value.toLocaleString();
  }

  statusIcon(s?: string): string { return s === 'found' ? '✅' : s === 'not_found' ? '❌' : '⏳'; }
  specKeys(specs: Record<string, string>): string[] { return specs ? Object.keys(specs).slice(0, 12) : []; }
}