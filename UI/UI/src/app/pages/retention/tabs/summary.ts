import { Component, inject, signal, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { Subscription, filter } from 'rxjs';
import { CommonModule } from '@angular/common';
import { RetentionService, RetentionSummary } from '../../../services/retention.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'retention-summary',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './summary.html',
  styleUrls: ['./summary.scss']
})
export class RetentionSummaryTab implements OnInit, OnDestroy {
  private svc    = inject(RetentionService);
  private auth   = inject(AuthService);
  private router = inject(Router);
  private routerSub?: Subscription;
  clientId     = this.auth.getClientId();

  data    = signal<RetentionSummary | null>(null);
  loading = signal(true);
  error   = signal('');
  msg     = signal('');

  ngOnInit() {
    this.load();
    this.routerSub = this.router.events
      .pipe(filter((e): e is NavigationEnd => e instanceof NavigationEnd))
      .subscribe(e => {
        if (e.urlAfterRedirects.includes('/retention-summary')) {
          this.load();
        }
      });
  }

  ngOnDestroy() { this.routerSub?.unsubscribe(); }

  load() {
    this.loading.set(true);
    this.error.set('');
    this.svc.getSummary(this.clientId).subscribe({
      next: (res) => { this.data.set(res); this.loading.set(false); },
      error: () => { this.error.set('Failed to load summary.'); this.loading.set(false); }
    });
  }

  fmtPct(n: any) { return (parseFloat(n) || 0).toFixed(1) + '%'; }
}