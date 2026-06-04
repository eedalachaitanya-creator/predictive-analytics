import { Component, inject, signal, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { Subscription, filter } from 'rxjs';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RetentionService, Intervention, OutcomeRequest } from '../../../services/retention.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'retention-interventions',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './interventions.html',
  styleUrls: ['./interventions.scss']
})
export class RetentionInterventionsTab implements OnInit, OnDestroy {
  private svc    = inject(RetentionService);
  private auth   = inject(AuthService);
  private router = inject(Router);
  private routerSub?: Subscription;
  clientId     = this.auth.getClientId();

  rows          = signal<Intervention[]>([]);
  loading       = signal(true);
  error         = signal('');
  saving        = signal<number | null>(null);
  outcomeMsg    = signal('');
  selected      = signal<Intervention | null>(null);
  outcomeStatus = signal<'accepted' | 'declined' | 'no_response' | 'bounced'>('accepted');
  revenueInput  = signal('');

  ngOnInit() {
    this.load();
    // Reload whenever user navigates back to this page (after generating offers, etc.)
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
    this.error.set('');   // clear stale error so a successful retry doesn't leave red banner
    this.svc.getInterventions(this.clientId).subscribe({
      next: (res) => { this.rows.set(res.interventions || res || []); this.loading.set(false); },
      error: () => { this.error.set('Failed to load interventions.'); this.loading.set(false); }
    });
  }

  openOutcome(row: Intervention) {
    this.selected.set(row);
    this.outcomeStatus.set('accepted');
    this.revenueInput.set('');
    this.outcomeMsg.set('');
  }

  closeOutcome() { this.selected.set(null); }

  saveOutcome() {
    const row = this.selected();
    if (!row) return;

    let revenue: number | undefined = undefined;
    if (this.revenueInput().trim()) {
      revenue = parseFloat(this.revenueInput());
      if (isNaN(revenue) || revenue < 0) {
        this.outcomeMsg.set('❌ Revenue must be a valid positive number.');
        return;
      }
    }

    this.saving.set(row.intervention_id);
    const body: OutcomeRequest = {
      intervention_id: row.intervention_id,
      offer_status:    this.outcomeStatus(),
      revenue_recovered: revenue
    };
    this.svc.recordOutcome(row.intervention_id, body).subscribe({
      next: () => {
        this.outcomeMsg.set('✅ Outcome saved.');
        this.saving.set(null);
        this.selected.set(null);
        this.load();
      },
      error: (err) => {
        this.outcomeMsg.set('❌ ' + (err?.error?.detail || 'Failed to save.'));
        this.saving.set(null);
      }
    });
  }

  riskColor(r: string)   { return r === 'HIGH' ? 'red' : r === 'MEDIUM' ? 'yellow' : 'green'; }
  statusColor(s: string) {
    if (s === 'accepted')    return 'green';
    if (s === 'declined')    return 'red';
    if (s === 'no_response') return 'gray';
    if (s === 'bounced')     return 'orange';
    return 'blue';
  }
  channelIcon(c: string) { return c === 'email' ? '✉️' : c === 'sms' ? '📱' : '🔔'; }
  fmtDate(d: string)     { return d ? new Date(d).toLocaleDateString() : '—'; }
  fmtPct(n: number)      { return (n || 0).toFixed(1) + '%'; }
  fmtProb(n: number)     { return (n * 100).toFixed(1) + '%'; }
}