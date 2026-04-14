import { Component, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, NavigationEnd } from '@angular/router';
import { AuthService } from '../services/auth.service';
import { toSignal } from '@angular/core/rxjs-interop';
import { filter, map, startWith } from 'rxjs/operators';

const META: Record<string, { title: string; meta: string }> = {
  upload:     { title: '📤 Upload Data',           meta: 'Walmart Inc. (CLT-001) · Upload all 11 master files' },
  validation: { title: '✅ Validation Preview',     meta: 'Quality checks run after each upload' },
  settings:   { title: '⚙️ Settings & Parameters',  meta: 'Configure pipeline rules before processing' },
  run:        { title: '🚀 Run Processing',          meta: 'Start the analytics pipeline' },
  dashboard:  { title: '📊 Output Dashboard',        meta: 'Walmart Inc. (CLT-001) · Pipeline results' },
  downloads:  { title: '📥 Downloads',               meta: 'Export your results' },
  messages:   { title: '💬 Message Templates',       meta: 'Configure templates by Tier × Risk Level' },
  clients:    { title: '👥 Client Management',       meta: 'Admin Console · Manage all retail clients' },
  users:      { title: '👤 User Management',         meta: 'Admin Console · Assign roles and client access' },
  sysconfig:  { title: '🖥️ System Configuration',   meta: 'Admin Console · Global pipeline defaults' },
  monitor:    { title: '📡 Pipeline Monitor',        meta: 'Admin Console · Live job view' },
  analytics:  { title: '📈 Admin Analytics',         meta: 'Admin Console · Cross-client KPIs' },
  audit:      { title: '🔒 Audit Log',               meta: 'Admin Console · Full history · 365-day retention' },
};

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

  pageMeta = computed(() => META[this.activeKey() ?? ''] ?? { title: 'Walmart Analytics', meta: '' });

  private keyFromUrl(url: string): string {
    // /app/dashboard?foo=bar  →  'dashboard'
    const segment = url.split('/app/')[1] ?? '';
    return segment.split('?')[0].split('/')[0];
  }
}
