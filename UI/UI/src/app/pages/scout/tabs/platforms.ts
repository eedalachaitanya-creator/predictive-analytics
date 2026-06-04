import { Component, computed, inject, signal, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ScoutService, Website } from '../../../services/scout.service';

@Component({
  selector: 'scout-platforms',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './platforms.html',
  styleUrls: ['./platforms.scss']
})
export class ScoutPlatformsTab implements OnInit, OnDestroy {
  private svc = inject(ScoutService);

  // Read directly from the shared signal. When Search, Monitor, or any other
  // component triggers a refresh (or when add/delete/toggle here completes),
  // this table updates without a manual reload.
  websites = computed(() => this.svc.websites());
  loading   = signal(true);
  adding    = signal(false);
  // True from the moment the user clicks Cancel until the backend's
  // cancelled response arrives (~2-3s due to cooperative cancellation
  // checkpoint granularity). Used purely for UI — disables the Cancel
  // button and changes its label so the user knows the click registered.
  cancelling = signal(false);
  newName   = signal('');
  // Holds the UUID for the add-website request currently in flight.
  // Set when we kick off the request, cleared in the next/error/cancelled
  // handlers. The Cancel button uses this to call /websites/cancel/{id}.
  currentRequestId = signal<string | null>(null);
  error     = signal('');
  success   = signal('');
  // How long flashed messages stay on screen before auto-clearing.
  // 4s is long enough to read a one-line confirmation, short enough
  // that stale messages don't pile up if the user is doing many ops.
  private readonly FLASH_MS = 4000;
  // Tracks the active auto-clear timer so successive flashes don't
  // cause an earlier timer to clear a later message prematurely.
  // Number in browsers, NodeJS.Timeout in SSR — `any` covers both
  // without dragging @types/node into a browser-only component.
  private flashTimer: any = null;
  editingIdx = signal<number | null>(null);
  editUrl   = signal('');

  // Delete confirmation modal state
  confirmDelete = signal<Website | null>(null);
  deleting      = signal(false);

  ngOnInit() {
    // Service handles the network call and updates the shared signal.
    this.svc.refreshPlatforms().subscribe({
      next: () => this.loading.set(false),
      error: () => this.loading.set(false),
    });
  }

  

  addWebsite() {
    const name = this.newName().trim();
    if (!name || this.adding()) return;

    // Generate a UUID for this attempt. The frontend owns the ID so the
    // Cancel button can post to /websites/cancel/{id} without waiting for
    // the backend to round-trip an ID first. Same pattern as Search.
    const requestId = crypto.randomUUID();
    this.currentRequestId.set(requestId);

    this.adding.set(true);
    this.error.set('');
    this.success.set('');

    this.svc.addWebsite(name, requestId).subscribe({
      next: res => {
        // Two response shapes from the backend:
        //   success:   { data: Website }
        //   cancelled: { status: 'cancelled', data: null, request_id: '...' }
        // We branch on status because a cancelled response has data=null
        // and would crash res.data.name below.
        if (res?.status === 'cancelled') {
          // Clean cancel — don't show success, don't show error, just reset.
          // The input is preserved so the user can retry without retyping.
          this.adding.set(false);
          this.cancelling.set(false);
          this.currentRequestId.set(null);
          return;
        }
        const site = res.data!;
        this.flashSuccess(`Added "${site.name}" — search URL: ${site.search_url}`);
        this.newName.set('');
        this.adding.set(false);
        this.cancelling.set(false);
        this.currentRequestId.set(null);
      },
      error: err => {
        // Persistent error — addWebsite is the one path where the user
        // really needs to read what went wrong (heavy bot wall, wrong
        // domain, etc.). Auto-clearing after 4s would mean they miss it
        // if the error appears 30s into the wait while they're looking
        // away. Stays until they click the × in the template, type a
        // new name (clearMessages clears it), or start another action.
        //
        // We still kill the flash timer in case a previous flashSuccess
        // is mid-countdown — without this, that timer would clear our
        // persistent error after a few seconds.
        if (this.flashTimer !== null) {
          clearTimeout(this.flashTimer);
          this.flashTimer = null;
        }
        this.success.set('');
        this.error.set(err.message || 'Failed to add website');
        this.adding.set(false);
        this.cancelling.set(false);
        this.currentRequestId.set(null);
      }
    });
  }

  /**
   * User clicked Cancel during add-website. Fire-and-forget: we tell the
   * backend to stop, then let the original addWebsite subscription resolve
   * with {status: 'cancelled'} on its own. We do NOT flip `adding` to false
   * here — that happens in the next() handler when the cancelled response
   * arrives. Doing it here would re-enable the form while the backend is
   * still resolving, allowing a second click that would race with the first.
   *
   * What we DO flip immediately is `cancelling` — purely a UI signal so the
   * button shows "Cancelling…" and gets disabled the instant the user clicks.
   * The backend may take 2-3s to actually stop (one Playwright iteration);
   * without this signal the UI would still say "Searching.." during that
   * window, which feels broken even though it's working correctly.
   */
  cancelAdd() {
    const requestId = this.currentRequestId();
    if (!requestId || this.cancelling()) return;
    this.cancelling.set(true);
    this.svc.cancelAddWebsite(requestId).subscribe({
      // Best-effort. Errors here (e.g., network blip) are non-actionable —
      // the backend's check_cancelled fires from a registry flag, so even
      // if the cancel POST fails, the next checkpoint won't see the flag.
      error: err => console.warn('[platforms] cancel POST failed:', err),
    });
  }


  toggleActive(site: Website) {
    const action = site.active
      ? this.svc.deactivateWebsite(site)
      : this.svc.reactivateWebsite(site.name);

    // svc methods auto-refresh the shared signal, so table + Search tab
    // both update without manual reload.
    action.subscribe({
      error: err => this.flashError(err.message || 'Toggle failed')
    });
  }

  startEdit(i: number) {
    this.editingIdx.set(i);
    this.editUrl.set(this.websites()[i].search_url);
  }

  cancelEdit() {
    this.editingIdx.set(null);
    this.editUrl.set('');
  }


  saveEdit(site: Website) {
    this.svc.updateWebsite({ name: site.name, search_url: this.editUrl() }).subscribe({
      next: () => {
        this.editingIdx.set(null);
        this.flashSuccess(`Updated search URL for "${site.name}"`);
      },
      error: err => this.flashError(err.message || 'Update failed')
    });
  }

  askDeleteWebsite(site: Website) {
    this.confirmDelete.set(site);
    this.clearMessages();
  }

  cancelDelete() {
    this.confirmDelete.set(null);
  }

  // Actually performs the delete after user confirms in the modal
  confirmDeleteWebsite() {
    const site = this.confirmDelete();
    if (!site || this.deleting()) return;

    this.deleting.set(true);
    this.svc.deleteWebsite(site.name).subscribe({
      next: (res: any) => {
        const counts = res?.deleted_counts || {};
        const detail = [
          counts.price_history   ? `${counts.price_history} price records`   : null,
          counts.price_alerts    ? `${counts.price_alerts} alerts`           : null,
          counts.product_results ? `${counts.product_results} scrape results`: null,
        ].filter(Boolean).join(', ');

        this.flashSuccess(
          detail
            ? `Deleted "${site.name}" — purged ${detail}`
            : `Deleted "${site.name}"`
        );
        this.confirmDelete.set(null);
        this.deleting.set(false);
      },
      error: err => {
        this.flashError(err.message || 'Delete failed');
        this.deleting.set(false);
        this.confirmDelete.set(null);
      }
    });
  }

  onAddKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') this.addWebsite();
  }

  clearMessages() {
    // Synchronous clear used when the user starts a new action — typing in
    // the input, opening the delete modal, etc. We also kill any pending
    // auto-clear timer so it can't fire later and clobber the next message.
    if (this.flashTimer !== null) {
      clearTimeout(this.flashTimer);
      this.flashTimer = null;
    }
    this.error.set('');
    this.success.set('');
  }

  /**
   * Show a success message that auto-clears after FLASH_MS.
   * Use this for transient confirmations (Added/Updated/Deleted X).
   * Cancels any pending clear from a previous flash so the timer
   * always reflects the most recent message, not the oldest one.
   */
  private flashSuccess(message: string) {
    this.error.set('');
    this.success.set(message);
    this.scheduleClear();
  }

  /**
   * Show an error message that auto-clears after FLASH_MS.
   * Use this for non-fatal/transient errors where the user doesn't
   * need the message to persist (e.g., toggle failed, update failed).
   * Do NOT use for errors the user must read and react to — for those
   * keep using `error.set` directly so the message stays visible.
   */
  private flashError(message: string) {
    this.success.set('');
    this.error.set(message);
    this.scheduleClear();
  }

  private scheduleClear() {
    // Cancel previous timer first. Without this, a fast sequence of two
    // flashes (e.g., delete A then delete B 500ms later) would have the
    // first timer fire 3.5s into B's display and clear B early.
    if (this.flashTimer !== null) {
      clearTimeout(this.flashTimer);
    }
    this.flashTimer = setTimeout(() => {
      this.error.set('');
      this.success.set('');
      this.flashTimer = null;
    }, this.FLASH_MS);
  }

  ngOnDestroy() {
    // Component being torn down (user navigated away). Cancel the pending
    // timer so its callback doesn't run on a destroyed component — that
    // would call .set() on signals after destroy and trigger console
    // warnings in dev mode at minimum.
    if (this.flashTimer !== null) {
      clearTimeout(this.flashTimer);
      this.flashTimer = null;
    }
  }
}