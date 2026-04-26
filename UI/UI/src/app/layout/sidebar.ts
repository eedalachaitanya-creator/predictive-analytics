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
    // 2026-04-25: '/app/messages' removed from both pathPrefixes and
    // children — outreach template configuration is owned by the
    // Retention Agent, not the Analyst Agent.
    pathPrefixes: [
      '/app/upload', '/app/validation', '/app/settings',
      '/app/dashboard', '/app/churn-scores', '/app/downloads',
      '/app/chat', '/app/cost-tracking',
    ],
    children: [
      { path: '/app/upload',        label: 'Upload Data',   icon: '📤' },
      { path: '/app/validation',    label: 'Validation',    icon: '✅' },
      { path: '/app/settings',      label: 'Configure & Run', icon: '⚙️' },
      { path: '/app/dashboard',     label: 'Dashboard',     icon: '📊' },
      { path: '/app/churn-scores',  label: 'Churn Scores',  icon: '📈' },
      { path: '/app/downloads',     label: 'Downloads',     icon: '📥' },
      { path: '/app/chat',          label: 'Agent Chat',    icon: '🤖' },
      { path: '/app/cost-tracking', label: 'Cost Tracking', icon: '💰' },
    ],
  };

    scoutGroup: NavGroup = {
    label: 'Scout Agent',
    icon:  '🔍',
    // pathPrefixes determines when this accordion auto-expands — any URL
    // starting with /app/scout will cause isInscoutGroup() to return true.
    pathPrefixes: ['/app/scout'],
    children: [
      // Paths match the child routes we defined in app.routes.ts. The Scout
      // component reads the last URL segment to decide which tab to show.
      // Icons chosen to match the pill tabs inside the Scout page itself.
      { path: '/app/scout/chat',      label: 'Chat',          icon: '💬' },
      { path: '/app/scout/monitor',   label: 'Price Monitor', icon: '📈' },
      { path: '/app/scout/search',    label: 'Search',        icon: '🔍' },
      { path: '/app/scout/compare',   label: 'Compare',       icon: '⚖️' },
      { path: '/app/scout/platforms', label: 'Platforms',     icon: '🌐' },
    ],
  };

    strategistGroup: NavGroup = {
      label: 'Strategist Agent',
      icon:  '🧠',
      pathPrefixes: [
        '/app/pricing-engine', '/app/market-trends', '/app/pipeline-monitor',
      ],
      children: [
        { path: '/app/pricing-engine',    label: 'Pricing Engine',   icon: '🧠' },
        { path: '/app/market-trends',     label: 'Market Trends',    icon: '📈' },
        { path: '/app/pipeline-monitor',  label: 'Pipeline Monitor', icon: '📊' },
      ],
    };

   retentionGroup: NavGroup = {
    label: 'Retention Agent',
    icon:  '🎯',
    pathPrefixes: [
      '/app/run-pipeline', '/app/interventions', '/app/escalations', '/app/retention-summary',
    ],
    children: [
      { path: '/app/run-pipeline',       label: 'Run Pipeline',  icon: '🚀' },
      { path: '/app/interventions',      label: 'Interventions', icon: '📋' },
      { path: '/app/escalations',        label: 'Escalations',   icon: '🔔' },
      { path: '/app/retention-summary',  label: 'Summary',       icon: '📊' },
    ],
  };

  // Sibling top-level items (other agents).
  otherNav: NavItem[] = [
    // { path: '/app/scout',      label: 'Scout Agent',      icon: '🔍' },
   // { path: '/app/strategist', label: 'Strategist Agent', icon: '🧠' },
  //  { path: '/app/retention',  label: 'Retention Agent',  icon: '🎯' },
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


  private readonly SCOUT_EXPAND_KEY = 'wap_sidebar_scoutanalyst_open';
  scoutOpen = signal<boolean>(this.scoutrestoreOpen());

    private scoutrestoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.SCOUT_EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    // No stored preference → open if user is currently on a child page.
    return this.isInscoutGroup();
  }
 
  isInscoutGroup(): boolean {
    const url = this.router.url;
    return this.scoutGroup.pathPrefixes.some(p => url.startsWith(p));
  }

    scoutAnalyst() {
    const next = !this.scoutOpen();
    this.scoutOpen.set(next);
    sessionStorage.setItem(this.EXPAND_KEY, next ? '1' : '0');
  }

  private readonly STRATEGIST_EXPAND_KEY = 'wap_sidebar_Strategistanalyst_open';
  StrategistOpen = signal<boolean>(this.StrategistrestoreOpen());

    private StrategistrestoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.STRATEGIST_EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    return this.isInStrategistGroup();
  }
 
  isInStrategistGroup(): boolean {
    const url = this.router.url;
    return this.strategistGroup.pathPrefixes.some(p => url.startsWith(p));
  }

    StrategistAnalyst() {
    const next = !this.StrategistOpen();
    this.StrategistOpen.set(next);
    sessionStorage.setItem(this.EXPAND_KEY, next ? '1' : '0');
  }

  //Code for Retention menu 
  private readonly RETENTION_EXPAND_KEY = 'wap_sidebar_retentionanalyst_open';
  retentionOpen = signal<boolean>(this.retentionrestoreOpen());

    private retentionrestoreOpen(): boolean {
    const saved = sessionStorage.getItem(this.RETENTION_EXPAND_KEY);
    if (saved === '0') return false;
    if (saved === '1') return true;
    return this.isInretentionGroup();
  }
 
  isInretentionGroup(): boolean {
    const url = this.router.url;
    return this.retentionGroup.pathPrefixes.some(p => url.startsWith(p));
  }

    retentionAnalyst() {
    const next = !this.retentionOpen();
    this.retentionOpen.set(next);
    sessionStorage.setItem(this.EXPAND_KEY, next ? '1' : '0');
  }

  adminNav: NavItem[] = [
    { path: '/app/clients',   label: 'Clients',    icon: '👥' },
    { path: '/app/users',     label: 'Users',      icon: '👤' },
    { path: '/app/monitor',   label: 'Cost Monitoring', icon: '💰' },
    { path: '/app/analytics', label: 'Analytics',  icon: '📈' },
    { path: '/app/audit',     label: 'Audit',      icon: '🔒' },
  ];

  logout() { this.auth.logout(); }
}