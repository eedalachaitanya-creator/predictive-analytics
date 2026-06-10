import { Component, inject, computed, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, NavigationEnd } from '@angular/router';
import { AuthService } from '../services/auth.service';
import { toSignal } from '@angular/core/rxjs-interop';
import { filter, map, startWith } from 'rxjs/operators';
import { SidebarService } from '../services/sidebar.service';

const META: Record<string, { title: string; meta: string }> = {
  // ── Analyst Agent ────────────────────────────────────────────────
  upload:           { title: '📤 Upload Data',           meta: 'Analyst Agent · Upload all 11 master files' },
  validation:       { title: '✅ Validation Preview',     meta: 'Analyst Agent · Quality checks run after each upload' },
  settings:         { title: '⚙️ Configure & Run',       meta: 'Analyst Agent · Configure pipeline rules and start processing' },
  dashboard:        { title: '📊 Dashboard',             meta: 'Analyst Agent · Pipeline results' },
  downloads:        { title: '📥 Downloads',             meta: 'Analyst Agent · Export your results' },
  'churn-scores':   { title: '📈 Churn Scores',          meta: 'Analyst Agent · ML-predicted churn risk per customer' },
  'cost-tracking':  { title: '💰 Cost Tracking',     meta: 'Analyst Agent · Token usage and spend across the platform' },
  chat:             { title: '🤖 Agent Chat',            meta: 'Analyst Agent · Ask questions about your data' },

  // ── Scout Agent ──────────────────────────────────────────────────
  scout:            { title: '🔍 Scout Agent',           meta: 'Scout Agent · Market intelligence and competitor tracking' },

  // ── Strategist Agent ─────────────────────────────────────────────
  'pricing-engine': { title: '💎 Pricing Engine',        meta: 'Strategist Agent · Dynamic pricing recommendations' },
  'market-trends':  { title: '📈 Market Trends',         meta: 'Strategist Agent · Category and competitor trend analysis' },

  // ── Retention Agent ──────────────────────────────────────────────
  'run-pipeline':      { title: '🚀 Run Pipeline',       meta: 'Retention Agent · Trigger retention model execution' },
  'retention-summary': { title: '📋 Retention Summary',  meta: 'Retention Agent · Outcomes and intervention results' },

  // ── Admin Console ────────────────────────────────────────────────
  clients:    { title: '👥 Client Management',       meta: 'Admin Console · Manage all retail clients' },
  users:      { title: '👤 User Management',         meta: 'Admin Console · Assign roles and client access' },
  monitor:    { title: '💰 Cost Monitoring',         meta: 'Admin Console · LLM cost tracking' },
  analytics:  { title: '📈 Admin Analytics',         meta: 'Admin Console · Cross-client KPIs' },
  audit:      { title: '🔒 Audit Log',               meta: 'Admin Console · Full history · 365-day retention' },
};

const ADMIN_PAGES = new Set(['clients', 'users', 'monitor', 'analytics', 'audit']);

@Component({
  selector: 'app-topbar',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './topbar.html',
  styleUrls: ['./topbar.scss']
})
export class TopbarComponent {
  auth   = inject(AuthService);
  router = inject(Router);
  sidebarService = inject(SidebarService);
  isSidebarOpen = false;

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

    if (ADMIN_PAGES.has(key)) {
      return { title: meta.title, meta: meta.meta };
    }

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
    const segment = url.split('/app/')[1] ?? '';
    return segment.split('?')[0].split('/')[0];
  }

  // ── Change Password ──────────────────────────────────────────────
  changePassOpen    = signal(false);
  changePassLoading = signal(false);
  changePassError   = signal('');
  changePassSuccess = signal('');
  currentPass       = signal('');
  newPass           = signal('');
  confirmPass       = signal('');
  showCurrentPass   = signal(false);
  showNewPass       = signal(false);
  showConfirmPass   = signal(false);

  openChangePassword() {
    this.currentPass.set('');
    this.newPass.set('');
    this.confirmPass.set('');
    this.changePassError.set('');
    this.changePassSuccess.set('');
    this.showCurrentPass.set(false);
    this.showNewPass.set(false);
    this.showConfirmPass.set(false);
    this.changePassOpen.set(true);
  }

  closeChangePassword() {
    this.changePassOpen.set(false);
    this.changePassError.set('');
    this.changePassSuccess.set('');
  }

  submitChangePassword() {
    if (!this.currentPass().trim()) {
      this.changePassError.set('Current password is required.'); return;
    }
    if (this.newPass() !== this.newPass().trim()) {
      this.changePassError.set('Password cannot start or end with a space.'); return;
    }
    if (this.newPass().length < 8) {
      this.changePassError.set('New password must be at least 8 characters.'); return;
    }
    if (this.newPass() !== this.confirmPass()) {
      this.changePassError.set('New passwords do not match.'); return;
    }
    this.changePassLoading.set(true);
    this.changePassError.set('');

    this.auth.changePassword(this.currentPass(), this.newPass()).subscribe({
      next: () => {
        this.changePassLoading.set(false);
        this.changePassSuccess.set('Password changed successfully! Use your new password next login.');
        setTimeout(() => this.closeChangePassword(), 3000);
      },
      error: (err) => {
        this.changePassLoading.set(false);
        this.changePassError.set(
          err?.error?.detail ?? err?.error?.message ?? 'Could not change password.'
        );
      }
    });
  }

  toggleSidebar() {
    this.sidebarService.toggle();
  }

  closeSidebar() {
    this.sidebarService.close();
  }
}