import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, NavigationEnd, RouterLink, RouterLinkActive } from '@angular/router';
import { toSignal } from '@angular/core/rxjs-interop';
import { filter, map, startWith } from 'rxjs/operators';
import { ScoutChatTab } from './tabs/chat';
import { ScoutMonitorTab } from './tabs/monitor';
import { ScoutSearchTab } from './tabs/search';
import { ScoutPlatformsTab } from './tabs/platforms';

// The set of valid tab keys. Used as the TypeScript type AND to validate
// URL segments — if the URL has some garbage like /app/scout/wrong, we
// fall back to 'chat' instead of showing a blank page.
type ScoutTab = 'chat' | 'monitor' | 'search' | 'platforms';
const VALID_TABS: readonly ScoutTab[] = ['chat', 'monitor', 'search', 'platforms'] as const;

interface TabDef {
  id: ScoutTab;
  label: string;
  icon: string;
  path: string;   // Full absolute route for routerLink — lets pill tabs
                  // update the URL instead of just flipping a signal.
}

@Component({
  selector: 'app-scout',
  standalone: true,
  imports: [
    CommonModule,
    RouterLink,           // So pill tabs can use [routerLink]
    RouterLinkActive,     // Optional — marks active pill via routerLinkActive
    ScoutChatTab, ScoutMonitorTab, ScoutSearchTab, ScoutPlatformsTab,
  ],
  templateUrl: './scout.html',
  styleUrls: ['./scout.scss']
})
export class ScoutComponent {
  private router = inject(Router);

  tabs: TabDef[] = [
    { id: 'monitor',   label: 'Price Monitor', icon: '📈', path: '/app/scout/monitor' },
    { id: 'search',    label: 'Search',        icon: '🔍', path: '/app/scout/search' },
    { id: 'platforms', label: 'Platforms',     icon: '🌐', path: '/app/scout/platforms' },
    // { id: 'chat',      label: 'Chat',          icon: '💬', path: '/app/scout/chat' },

  ];

  // activeTab is now DERIVED from the URL, not stored as its own state.
  // When the user clicks a pill or a sidebar item, the router changes the
  // URL → NavigationEnd fires → this signal recomputes → [hidden]
  // bindings in the template flip to show the right tab.
  //
  // toSignal() converts an RxJS observable into an Angular signal. We
  // subscribe to router events, filter for NavigationEnd, extract the last
  // URL segment, validate it, and emit the tab key. startWith() seeds the
  // signal with whatever the URL is on initial page load (otherwise the
  // first value wouldn't arrive until the user navigated somewhere).
  activeTab = toSignal(
    this.router.events.pipe(
      filter(e => e instanceof NavigationEnd),
      map((e: NavigationEnd) => this.tabFromUrl(e.urlAfterRedirects)),
      startWith(this.tabFromUrl(this.router.url)),
    ),
    { initialValue: this.tabFromUrl(this.router.url) },
  );

  /**
   * Extract the tab key from a URL like "/app/scout/monitor".
   *
   * Behavior:
   *   /app/scout          → 'chat'     (default, matches the router redirect)
   *   /app/scout/monitor  → 'monitor'
   *   /app/scout/xyz      → 'chat'     (invalid segment → fall back)
   *   /app/upload         → 'chat'     (not under /app/scout → fall back)
   *
   * The fallback to 'chat' is deliberate: if this component is ever shown
   * under a weird URL, we want to render SOMETHING rather than a blank page.
   */
  private tabFromUrl(url: string): ScoutTab {
    const after = url.split('/app/scout/')[1];
    if (!after) return 'chat';                       // /app/scout or not-scout
    const segment = after.split('?')[0].split('/')[0] as ScoutTab;
    return VALID_TABS.includes(segment) ? segment : 'monitor';
  }
}