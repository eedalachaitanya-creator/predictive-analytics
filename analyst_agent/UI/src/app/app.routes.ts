import { Routes } from '@angular/router';
import { LoginComponent } from './auth/login';
import { ShellComponent } from './layout/shell';
import { authGuard } from './guards/auth.guard';
import { noAuthGuard } from './guards/no-auth.guard';
import { adminGuard } from './guards/admin.guard';

export const routes: Routes = [
  { path: '', redirectTo: 'login', pathMatch: 'full' },
  { path: 'login', component: LoginComponent, canActivate: [noAuthGuard] },

  {
    path: 'app',
    component: ShellComponent,
    canActivate: [authGuard],
    children: [
      { path: '', redirectTo: 'upload', pathMatch: 'full' },

      // ── Client Portal ────────────────────────────────────────
      { path: 'upload',     loadComponent: () => import('./pages/upload').then(m => m.UploadComponent) },
      { path: 'validation', loadComponent: () => import('./pages/validation').then(m => m.ValidationComponent) },
      { path: 'settings',   loadComponent: () => import('./pages/settings').then(m => m.SettingsComponent) },
      { path: 'run',        loadComponent: () => import('./pages/run').then(m => m.RunComponent) },
      { path: 'dashboard',  loadComponent: () => import('./pages/dashboard').then(m => m.DashboardComponent) },
      { path: 'downloads',  loadComponent: () => import('./pages/downloads').then(m => m.DownloadsComponent) },
      { path: 'chat',       loadComponent: () => import('./pages/chat').then(m => m.ChatComponent) },
      { path: 'messages',   loadComponent: () => import('./pages/messages').then(m => m.MessagesComponent) },

      // ── Admin Console (admin guard) ───────────────────────────
      { path: 'clients',   loadComponent: () => import('./pages/clients').then(m => m.ClientsComponent),   canActivate: [adminGuard] },
      { path: 'users',     loadComponent: () => import('./pages/users').then(m => m.UsersComponent),       canActivate: [adminGuard] },
      { path: 'sysconfig', loadComponent: () => import('./pages/sys-config').then(m => m.SysConfigComponent), canActivate: [adminGuard] },
      { path: 'monitor',   loadComponent: () => import('./pages/monitor').then(m => m.MonitorComponent),   canActivate: [adminGuard] },
      { path: 'analytics', loadComponent: () => import('./pages/analytics').then(m => m.AnalyticsComponent), canActivate: [adminGuard] },
      { path: 'audit',     loadComponent: () => import('./pages/audit').then(m => m.AuditComponent),       canActivate: [adminGuard] },

      { path: '**', redirectTo: 'upload' },
    ]
  },

  { path: '**', redirectTo: 'login' }
];
