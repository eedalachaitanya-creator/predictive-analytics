import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

/**
 * Shared open/closed state for the responsive sidebar drawer.
 *
 * The topbar's hamburger calls `toggle()` / `close()`; the sidebar subscribes
 * to `sidebarOpen$` to add/remove its `.open` class and show the dimming
 * overlay on small screens. On desktop the sidebar is shown via CSS regardless,
 * so the default state is closed (the drawer only matters on mobile).
 *
 * Restored 2026-06-08: `sidebar.ts` and `topbar.ts` (commit ed49dc5 "UI fixes
 * with RWD") imported this service, but the file itself was never committed,
 * which broke `ng build` / `ng test` on main.
 */
@Injectable({ providedIn: 'root' })
export class SidebarService {
  private readonly _open = new BehaviorSubject<boolean>(false);

  /** Emits the current open state, and every change thereafter. */
  readonly sidebarOpen$: Observable<boolean> = this._open.asObservable();

  /** Current open state (synchronous read). */
  get isOpen(): boolean {
    return this._open.value;
  }

  toggle(): void {
    this._open.next(!this._open.value);
  }

  open(): void {
    this._open.next(true);
  }

  close(): void {
    this._open.next(false);
  }
}
