import { Component, inject, signal, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { Subscription, filter } from 'rxjs';
import { CommonModule } from '@angular/common';
import { RetentionService } from '../../../services/retention.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'retention-escalations',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './escalations.html',
  styleUrls: ['./escalations.scss']
})
export class RetentionEscalationsTab implements OnInit, OnDestroy {
  private svc    = inject(RetentionService);
  private auth   = inject(AuthService);
  private router = inject(Router);
  private routerSub?: Subscription;
  clientId     = this.auth.getClientId();

  rows    = signal<any[]>([]);
  loading = signal(true);
  error   = signal('');

  ngOnInit() {
    this.load();
    // Reload whenever user navigates back to this page
    this.routerSub = this.router.events
      .pipe(filter((e): e is NavigationEnd => e instanceof NavigationEnd))
      .subscribe(e => {
        if (e.urlAfterRedirects.includes('/escalations')) {
          this.load();
        }
      });
  }

  ngOnDestroy() {
    this.routerSub?.unsubscribe();
  }

  load() {
    this.loading.set(true);
    this.error.set('');   // clear stale error so a successful retry doesn't leave red banner
    this.svc.getEscalations(this.clientId).subscribe({
      next: (res) => { this.rows.set(res.escalations || []); this.loading.set(false); },
      error: () => { this.error.set('Failed to load escalations.'); this.loading.set(false); }
    });
  }

  fmtProb(n: any)  { return ((parseFloat(n) || 0) * 100).toFixed(1) + '%'; }
  fmtLtv(n: any)   { return '$' + (parseFloat(n) || 0).toFixed(2); }
  fmtPct(n: any)   { return (parseFloat(n) || 0).toFixed(1) + '%'; }
  fmtDate(d: string)  { return d ? new Date(d).toLocaleString() : '—'; }
  tierColor(t: string){ return t === 'Platinum' ? 'purple' : t === 'Gold' ? 'yellow' : t === 'Silver' ? 'gray' : 'orange'; }
}