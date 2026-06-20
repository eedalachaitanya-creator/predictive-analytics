import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';
import { AuthService } from '../services/auth.service';
import { SidebarService } from '../services/sidebar.service';

interface NavItem { path: string; label: string; icon: string; }
interface NavGroup { label: string; icon: string; pathPrefixes: string[]; children: NavItem[]; }

@Component({
  selector: 'app-sidebar',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink, RouterLinkActive],
  templateUrl: './sidebar.html',
  styleUrls: ['./sidebar.scss']
})
export class SidebarComponent {
  auth   = inject(AuthService);
  router = inject(Router);
  sidebarService = inject(SidebarService);

  isSidebarOpen = false;


  ngOnInit() {
    this.sidebarService.sidebarOpen$.subscribe(state => {
      this.isSidebarOpen = state;
    });
  }

  analystGroup: NavGroup = {
    label: 'Analyst Agent',
    icon:  '🔮',
    pathPrefixes: [
      '/app/upload', '/app/validation', '/app/settings',
      '/app/dashboard', '/app/churn-scores',
      // Agent Chat + Cost Tracking hidden from the Analyst Agent nav (commented
      // out per request — routes still exist; un-comment to re-enable).
      // '/app/chat', '/app/cost-tracking',
   ],
    children: [
      { path: '/app/dashboard',     label: 'Dashboard',       icon: '📊' },
      { path: '/app/upload',        label: 'Upload Data',     icon: '📤' },
      { path: '/app/validation',    label: 'Validation',      icon: '✅' },
      { path: '/app/settings',      label: 'Configure & Run', icon: '⚙️' },
      { path: '/app/churn-scores',  label: 'Churn Scores',    icon: '📈' },
      // Hidden per request (un-comment to restore):
      // { path: '/app/chat',          label: 'Agent Chat',      icon: '🤖' },
      // { path: '/app/cost-tracking', label: 'Cost Tracking',   icon: '💰' },
    ],
  };

  scoutGroup: NavGroup = {
    label: 'Scout Agent',
    icon:  '🔍',
    pathPrefixes: ['/app/scout'],
    children: [
      { path: '/app/scout/monitor',   label: 'Price Monitor', icon: '📈' },
      { path: '/app/scout/search',    label: 'Search',        icon: '🔍' },
      { path: '/app/scout/platforms', label: 'Platforms',     icon: '🌐' },
      { path: '/app/scout/chat',      label: 'Chat',          icon: '💬' },
    ],
  };

  strategistGroup: NavGroup = {
    label: 'Strategist Agent',
    icon:  '🧠',
    pathPrefixes: ['/app/pricing-engine', '/app/market-trends'],
    children: [
      { path: '/app/pricing-engine', label: 'Pricing Engine', icon: '🧠' },
      { path: '/app/market-trends',  label: 'Market Trends',  icon: '📈' },
    ],
  };

  retentionGroup: NavGroup = {
    label: 'Retention Agent',
    icon:  '🎯',
    pathPrefixes: [
      '/app/run-pipeline', '/app/retention-summary',
    ],
    children: [
      { path: '/app/run-pipeline',      label: 'Generate Offers', icon: '🚀' },
      { path: '/app/retention-summary', label: 'Summary',         icon: '📊' },
    ],
  };

  otherNav: NavItem[] = [];

  // ── Analyst accordion ──────────────────────────────────────────────
  private readonly EXPAND_KEY = 'wap_sidebar_analyst_open';
  analystOpen = signal<boolean>(this.restoreOpen());

  private restoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    return this.isInGroup();
  }

  isInGroup(): boolean {
    return this.analystGroup.pathPrefixes.some(p => this.router.url.startsWith(p));
  }

  toggleAnalyst() {
    const next = !this.analystOpen();
    this.analystOpen.set(next);
    sessionStorage.setItem(this.EXPAND_KEY, next ? '1' : '0');
  }

  // ── Scout accordion ────────────────────────────────────────────────
  private readonly SCOUT_EXPAND_KEY = 'wap_sidebar_scoutanalyst_open';
  scoutOpen = signal<boolean>(this.scoutrestoreOpen());

  private scoutrestoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.SCOUT_EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    return this.isInscoutGroup();
  }

  isInscoutGroup(): boolean {
    return this.scoutGroup.pathPrefixes.some(p => this.router.url.startsWith(p));
  }

  scoutAnalyst() {
    const next = !this.scoutOpen();
    this.scoutOpen.set(next);
    sessionStorage.setItem(this.SCOUT_EXPAND_KEY, next ? '1' : '0');
  }

  // ── Strategist accordion ───────────────────────────────────────────
  private readonly STRATEGIST_EXPAND_KEY = 'wap_sidebar_Strategistanalyst_open';
  StrategistOpen = signal<boolean>(this.StrategistrestoreOpen());

  private StrategistrestoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.STRATEGIST_EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    return this.isInStrategistGroup();
  }

  isInStrategistGroup(): boolean {
    return this.strategistGroup.pathPrefixes.some(p => this.router.url.startsWith(p));
  }

  StrategistAnalyst() {
    const next = !this.StrategistOpen();
    this.StrategistOpen.set(next);
    sessionStorage.setItem(this.STRATEGIST_EXPAND_KEY, next ? '1' : '0');
  }

  // ── Retention accordion ────────────────────────────────────────────
  private readonly RETENTION_EXPAND_KEY = 'wap_sidebar_retentionanalyst_open';
  retentionOpen = signal<boolean>(this.retentionrestoreOpen());

  private retentionrestoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.RETENTION_EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    return this.isInretentionGroup();
  }

  isInretentionGroup(): boolean {
    return this.retentionGroup.pathPrefixes.some(p => this.router.url.startsWith(p));
  }

  retentionAnalyst() {
    const next = !this.retentionOpen();
    this.retentionOpen.set(next);
    sessionStorage.setItem(this.RETENTION_EXPAND_KEY, next ? '1' : '0');
  }

  // ── Admin nav ──────────────────────────────────────────────────────
  adminNav: NavItem[] = [
    { path: '/app/clients',   label: 'Clients',         icon: '👥' },
    { path: '/app/users',     label: 'Users',            icon: '👤' },
    { path: '/app/monitor',   label: 'Cost Monitoring',  icon: '💰' },
    { path: '/app/analytics', label: 'Analytics',        icon: '📈' },
    { path: '/app/audit',     label: 'Audit',            icon: '🔒' },
  ];

  // ── User menu (bottom of sidebar) ─────────────────────────────────
  userMenuOpen      = signal(false);
  logoutConfirmOpen = signal(false);

  toggleUserMenu() {
    const next = !this.userMenuOpen();
    this.userMenuOpen.set(next);
    if (!next) this.logoutConfirmOpen.set(false);
  }

  confirmLogout() {
    this.logoutConfirmOpen.set(true);
    this.userMenuOpen.set(false);
  }

  cancelLogout() {
    this.logoutConfirmOpen.set(false);
  }

  logout() {
    this.logoutConfirmOpen.set(false);
    this.auth.logout();
  }

  // ── Change Password ────────────────────────────────────────────────
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
    this.userMenuOpen.set(false);
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
      this.changePassError.set('New password and confirm password do not match.'); return;
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
    this.isSidebarOpen  = !this.isSidebarOpen ;
  } 

  closeSidebar() {
    this.isSidebarOpen  = false ;
  }
}