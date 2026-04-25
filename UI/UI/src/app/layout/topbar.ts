import { Component, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, NavigationEnd } from '@angular/router';
import { AuthService } from '../services/auth.service';
import { toSignal } from '@angular/core/rxjs-interop';
import { filter, map, startWith } from 'rxjs/operators';

const META: Record<string, { title: string; meta: string }> = {
  upload:     { title: '📤 Upload Data',           meta: 'Upload all 11 master files' },
  validation: { title: '✅ Validation Preview',     meta: 'Quality checks run after each upload' },
  settings:   { title: '⚙️ Configure & Run',       meta: 'Configure pipeline rules and start processing' },
  dashboard:  { title: '📊 Output Dashboard',        meta: 'Pipeline results' },
  downloads:  { title: '📥 Downloads',               meta: 'Export your results' },
  chat:       { title: '🤖 Agent Chat',              meta: 'Ask questions about your data' },
  messages:   { title: '💬 Message Templates',       meta: 'Configure templates by Tier × Risk Level' },
  clients:    { title: '👥 Client Management',       meta: 'Admin Console · Manage all retail clients' },
  users:      { title: '👤 User Management',         meta: 'Admin Console · Assign roles and client access' },
  monitor:    { title: '💰 Cost Monitoring',         meta: 'Admin Console · LLM cost tracking' },
  analytics:  { title: '📈 Admin Analytics',         meta: 'Admin Console · Cross-client KPIs' },
  audit:      { title: '🔒 Audit Log',               meta: 'Admin Console · Full history · 365-day retention' },
};

// Pages that live in the Admin Console sidebar group. They are cross-tenant,
// so the topbar should NOT prepend "ClientName (CLT-###) ·" to their meta
// line — that produced "— () · Admin Console · …" when no tenant was selected.
const ADMIN_PAGES = new Set(['clients', 'users', 'monitor', 'analytics', 'audit']);

@Component({
  selector: 'app-topbar',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './topbar.html',
  styleUrls: ['./topbar.scss']
})
export class TopbarComponent {
  auth   = inject(AuthService);
  router = inject(Router);

  // Derive current page key from the active URL segment after /app/
  private activeKey = toSignal(
    this.router.events.pipe(
      filter(e => e instanceof NavigationEnd),
      map((e: NavigationEnd) => this.keyFromUrl(e.urlAfterRedirects)),
      startWith(this.keyFromUrl(this.router.url))
    ),
    { initialValue: this.keyFromUrl(this.router.url) }
  );

  pageMeta = computed(() => {
    const key  = this.activeKey() ?? '';
    const meta = META[key] ?? { title: 'Analytics Platform', meta: '' };

    // Admin-console pages are cross-tenant — they don't belong to any one
    // client, so show the page meta as-is instead of prefixing a client that
    // doesn't apply (which rendered as "— () · …" for super admins).
    if (ADMIN_PAGES.has(key)) {
      return { title: meta.title, meta: meta.meta };
    }

    // Tenant-scoped pages: only prepend the client prefix when we actually
    // have a real client selected. Falling back to "— ()" is worse than
    // showing just the page meta.
    const id   = this.auth.getClientId();
    const name = this.auth.getClientName();
    if (!id) {
      return { title: meta.title, meta: meta.meta };
    }
    return {
      title: meta.title,
      meta: `${name} (${id}) · ${meta.meta}`
    };
  });

  private keyFromUrl(url: string): string {
    // /app/dashboard?foo=bar  →  'dashboard'
    const segment = url.split('/app/')[1] ?? '';
    return segment.split('?')[0].split('/')[0];
  }
}
