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
      { path: 'dashboard',  loadComponent: () => import('./pages/dashboard').then(m => m.DashboardComponent),   canActivate: [clientGuard] },
      { path: 'downloads',  loadComponent: () => import('./pages/downloads').then(m => m.DownloadsComponent),   canActivate: [clientGuard] },
      { path: 'churn-scores', loadComponent: () => import('./pages/churn-scores').then(m => m.ChurnScoresComponent), canActivate: [clientGuard] },
      { path: 'cost-tracking', loadComponent: () => import('./pages/cost-tracking').then(m => m.CostTrackingComponent), canActivate: [clientGuard] },
      { path: 'chat',       loadComponent: () => import('./pages/chat').then(m => m.ChatComponent),             canActivate: [clientGuard] },
      // 2026-04-25: 'messages' route removed — outreach template
      // configuration is the Retention Agent's job. Analyst Agent no
      // longer exposes a Message Templates page.
       {
        path: 'scout',
        loadComponent: () => import('./pages/scout/scout').then(m => m.ScoutComponent),
        children: [
          // Default: hitting /app/scout lands on Chat
          { path: '', redirectTo: 'chat', pathMatch: 'full' },
          // These children don't load components — the Scout parent renders all
          // 5 tab components always. The path segment is only used to set the
          // active tab via the URL. Empty component refs would still work, but
          // we use a sentinel component-less approach: the parent watches its
          // own ActivatedRoute.firstChild to detect which tab should be active.
          { path: 'chat',      children: [] },
          { path: 'monitor',   children: [] },
          { path: 'search',    children: [] },
          { path: 'compare',   children: [] },
          { path: 'platforms', children: [] },
        ],
      },

      { path: 'pricing-engine',    loadComponent: () => import('./pages/pricing-engine').then(m => m.PricingEngineComponent),       canActivate: [clientGuard] },
      { path: 'market-trends',     loadComponent: () => import('./pages/market-trends').then(m => m.MarketTrendsComponent),         canActivate: [clientGuard] },
      
      { path: 'strategist', redirectTo: 'pricing-engine', pathMatch: 'full' },
      { path: 'run-pipeline',       loadComponent: () => import('./pages/run-pipeline').then(m => m.RunPipelineComponent),             canActivate: [clientGuard] },
      { path: 'interventions',      loadComponent: () => import('./pages/interventions').then(m => m.InterventionsComponent),         canActivate: [clientGuard] },
      { path: 'escalations',        loadComponent: () => import('./pages/escalations').then(m => m.EscalationsComponent),             canActivate: [clientGuard] },
      { path: 'retention-summary',  loadComponent: () => import('./pages/retention-summary').then(m => m.RetentionSummaryComponent),  canActivate: [clientGuard] },
      { path: 'retention',          redirectTo: 'run-pipeline', pathMatch: 'full' },
      // ── Admin Console (blocked for clients) ──────────────────
      { path: 'clients',   loadComponent: () => import('./pages/clients').then(m => m.ClientsComponent),   canActivate: [adminGuard] },
      { path: 'users',     loadComponent: () => import('./pages/users').then(m => m.UsersComponent),       canActivate: [adminGuard] },
      { path: 'monitor',   loadComponent: () => import('./pages/monitor').then(m => m.MonitorComponent),   canActivate: [adminGuard] },
      { path: 'analytics', loadComponent: () => import('./pages/analytics').then(m => m.AnalyticsComponent), canActivate: [adminGuard] },
      { path: 'audit',     loadComponent: () => import('./pages/audit').then(m => m.AuditComponent),       canActivate: [adminGuard] },

      { path: '**', redirectTo: 'upload' },
    ]
  },

  { path: '**', redirectTo: 'login' }
];