import { Component, inject, signal, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { Subscription, filter } from 'rxjs';
import { CommonModule } from '@angular/common';
import { RetentionService, Intervention } from '../../../services/retention.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'retention-interventions',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './interventions.html',
  styleUrls: ['./interventions.scss']
})
export class RetentionInterventionsTab implements OnInit, OnDestroy {
  private svc    = inject(RetentionService);
  private auth   = inject(AuthService);
  private router = inject(Router);
  private routerSub?: Subscription;
  clientId = this.auth.getClientId();

  rows    = signal<Intervention[]>([]);
  loading = signal(true);
  error   = signal('');

  ngOnInit() {
    this.load();
    this.routerSub = this.router.events
      .pipe(filter((e): e is NavigationEnd => e instanceof NavigationEnd))
      .subscribe(e => {
        if (e.urlAfterRedirects.includes('/interventions')) {
          this.load();
        }
      });
  }

  ngOnDestroy() {
    this.routerSub?.unsubscribe();
  }

  load() {
    this.loading.set(true);
    this.error.set('');
    this.svc.getInterventions(this.clientId).subscribe({
      next: (res) => { this.rows.set(res.interventions || res || []); this.loading.set(false); },
      error: () => { this.error.set('Failed to load interventions.'); this.loading.set(false); }
    });
  }

  riskColor(r: string)   { return r === 'HIGH' ? 'red' : r === 'MEDIUM' ? 'yellow' : 'green'; }
  channelIcon(c: string) { return c === 'email' ? '✉️' : c === 'sms' ? '📱' : '🔔'; }
  fmtDate(d: string)     { return d ? new Date(d).toLocaleDateString() : '—'; }
  fmtPct(n: any)         { return (parseFloat(n) || 0).toFixed(1) + '%'; }
  fmtProb(n: any)        { return ((parseFloat(n) || 0) * 100).toFixed(1) + '%'; }
}