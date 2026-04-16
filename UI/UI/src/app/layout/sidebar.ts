import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';
import { AuthService } from '../services/auth.service';

interface NavItem { path: string; label: string; icon: string; }

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

  clientNav: NavItem[] = [
    { path: '/app/upload',     label: 'Upload Data', icon: '📤' },
    { path: '/app/validation', label: 'Validation',  icon: '✅' },
    { path: '/app/settings',   label: 'Settings',    icon: '⚙️' },
    { path: '/app/run',        label: 'Run',         icon: '🚀' },
    { path: '/app/dashboard',  label: 'Dashboard',   icon: '📊' },
    { path: '/app/churn-scores', label: 'Analyst Agent', icon: '🔮' },
    { path: '/app/downloads',  label: 'Downloads',   icon: '📥' },
    { path: '/app/chat',       label: 'Agent Chat',  icon: '🤖' },
    { path: '/app/messages',   label: 'Messages',    icon: '💬' },
  ];

  adminNav: NavItem[] = [
    { path: '/app/clients',   label: 'Clients',    icon: '👥' },
    { path: '/app/users',     label: 'Users',      icon: '👤' },
    { path: '/app/sysconfig', label: 'Sys Config', icon: '🖥️' },
    { path: '/app/monitor',   label: 'Monitor',    icon: '📡' },
    { path: '/app/analytics', label: 'Analytics',  icon: '📈' },
    { path: '/app/audit',     label: 'Audit',      icon: '🔒' },
  ];

  logout() { this.auth.logout(); }
}
