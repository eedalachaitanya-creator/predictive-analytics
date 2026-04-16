import { Routes } from '@angular/router';
import { LoginComponent } from './auth/login';
import { RegisterComponent } from './auth/register';
import { ShellComponent } from './layout/shell';
import { authGuard } from './guards/auth.guard';
import { noAuthGuard } from './guards/no-auth.guard';
import { adminGuard } from './guards/admin.guard';
import { clientGuard } from './guards/client.guard';

export const routes: Routes = [
  { path: '', redirectTo: 'login', pathMatch: 'full' },
  { path: 'login', component: LoginComponent, canActivate: [noAuthGuard] },
  { path: 'register', component: RegisterComponent, canActivate: [noAuthGuard] },

  {
    path: 'app',
    component: ShellComponent,
    canActivate: [authGuard],
    children: [
      { path: '', redirectTo: 'upload', pathMatch: 'full' },

      // ── Client Portal (blocked for admins) ──────────────────
      { path: 'upload',     loadComponent: () => import('./pages/upload').then(m => m.UploadComponent),         canActivate: [clientGuard] },
      { path: 'validation', loadComponent: () => import('./pages/validation').then(m => m.ValidationComponent), canActivate: [clientGuard] },
      { path: 'settings',   loadComponent: () => import('./pages/settings').then(m => m.SettingsComponent),     canActivate: [clientGuard] },
      { path: 'run',        loadComponent: () => import('./pages/run').then(m => m.RunComponent),               canActivate: [clientGuard] },
      { path: 'dashboard',  loadComponent: () => import('./pages/dashboard').then(m => m.DashboardComponent),   canActivate: [clientGuard] },
      { path: 'downloads',  loadComponent: () => import('./pages/downloads').then(m => m.DownloadsComponent),   canActivate: [clientGuard] },
      { path: 'churn-scores', loadComponent: () => import('./pages/churn-scores').then(m => m.ChurnScoresComponent), canActivate: [clientGuard] },
      { path: 'chat',       loadComponent: () => import('./pages/chat').then(m => m.ChatComponent),             canActivate: [clientGuard] },
      { path: 'messages',   loadComponent: () => import('./pages/messages').then(m => m.MessagesComponent),     canActivate: [clientGuard] },

      // ── Admin Console (blocked for clients) ──────────────────
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
