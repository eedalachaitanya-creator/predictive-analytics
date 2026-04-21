import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';
import { AuthService } from '../services/auth.service';

interface NavItem { path: string; label: string; icon: string; }
interface NavGroup { label: string; icon: string; pathPrefixes: string[]; children: NavItem[]; }

@Component({
  selector: 'app-sidebar',
  standalone: true,
  imports: [CommonModule, RouterLink, RouterLinkActive],
  templateUrl: './sidebar.html',
  styleUrls: ['./sidebar.scss']
})
export class SidebarComponent {
  auth   = inject(AuthService);
  router = inject(Router);

  // ── Grouped sub-nav: one collapsible "Analyst Agent" group ─────────────────
  // All analyst-pipeline pages + Agent Chat + Cost Tracking live under one
  // parent entry so the sidebar isn't a flat 13-item list. pathPrefixes is
  // used to auto-expand the group when the user is on any of its pages.
  analystGroup: NavGroup = {
    label: 'Analyst Agent',
    icon:  '🔮',
    pathPrefixes: [
      '/app/upload', '/app/validation', '/app/settings', '/app/run',
      '/app/dashboard', '/app/churn-scores', '/app/downloads',
      '/app/messages', '/app/chat', '/app/cost-tracking',
    ],
    children: [
      { path: '/app/upload',        label: 'Upload Data',   icon: '📤' },
      { path: '/app/validation',    label: 'Validation',    icon: '✅' },
      { path: '/app/settings',      label: 'Settings',      icon: '⚙️' },
      { path: '/app/run',           label: 'Run',           icon: '🚀' },
      { path: '/app/dashboard',     label: 'Dashboard',     icon: '📊' },
      { path: '/app/churn-scores',  label: 'Churn Scores',  icon: '📈' },
      { path: '/app/downloads',     label: 'Downloads',     icon: '📥' },
      { path: '/app/messages',      label: 'Messages',      icon: '💬' },
      { path: '/app/chat',          label: 'Agent Chat',    icon: '🤖' },
      { path: '/app/cost-tracking', label: 'Cost Tracking', icon: '💰' },
    ],
  };

  // Sibling top-level items (other agents).
  otherNav: NavItem[] = [
    { path: '/app/scout',      label: 'Scout Agent',      icon: '🔍' },
    { path: '/app/strategist', label: 'Strategist Agent', icon: '🧠' },
    { path: '/app/retention',  label: 'Retention Agent',  icon: '🎯' },
  ];

  // Expanded state — default OPEN if the current URL is inside the group,
  // otherwise the user has to click to expand. Persisted in sessionStorage
  // so the collapse choice survives page navigation within the tab.
  private readonly EXPAND_KEY = 'wap_sidebar_analyst_open';
  analystOpen = signal<boolean>(this.restoreOpen());

  private restoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    // No stored preference → open if user is currently on a child page.
    return this.isInGroup();
  }

  isInGroup(): boolean {
    const url = this.router.url;
    return this.analystGroup.pathPrefixes.some(p => url.startsWith(p));
  }

  toggleAnalyst() {
    const next = !this.analystOpen();
    this.analystOpen.set(next);
    sessionStorage.setItem(this.EXPAND_KEY, next ? '1' : '0');
  }

  adminNav: NavItem[] = [
    { path: '/app/clients',   label: 'Clients',    icon: '👥' },
    { path: '/app/users',     label: 'Users',      icon: '👤' },
    { path: '/app/sysconfig', label: 'Sys Config', icon: '🖥️' },
    { path: '/app/monitor',   label: 'Monitor',    icon: '📡' },
    { path: '/app/analytics', label: 'Analytics',  icon: '📈' },
    { path: '/app/audit',     label: 'Audit',      icon: '🔒' },
    { path: '/app/scout',      label: 'Scout Agent',      icon: '🔍' },
    { path: '/app/strategist', label: 'Strategist Agent', icon: '🧠' },
    { path: '/app/retention',  label: 'Retention Agent',  icon: '🎯' },
  ];

  logout() { this.auth.logout(); }
}