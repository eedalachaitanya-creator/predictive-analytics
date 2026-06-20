import { Component, inject, signal, computed, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { Subscription, filter } from 'rxjs';
import { CommonModule } from '@angular/common';
import { RetentionService, RetentionSummary, Intervention } from '../../../services/retention.service';
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
  clientId = this.auth.getClientId();

  data    = signal<RetentionSummary | null>(null);
  loading = signal(true);
  error   = signal('');
  msg     = signal('');

  // ── Drill-down modal ──────────────────────────────────────────────
  viewCard    = signal<'all' | 'high' | 'medium' | 'escalated' | null>(null);
  viewLabel   = signal('');
  viewRows    = signal<Intervention[]>([]);
  viewLoading = signal(false);
  viewError   = signal('');

  // ── Pagination ────────────────────────────────────────────────────
  readonly pageSize = 100;
  currentPage  = signal(1);

  totalPages = computed(() =>
    Math.max(1, Math.ceil(this.viewRows().length / this.pageSize))
  );

  pagedRows = computed(() => {
    const start = (this.currentPage() - 1) * this.pageSize;
    return this.viewRows().slice(start, start + this.pageSize);
  });

  paginationLabel = computed(() => {
    const total = this.viewRows().length;
    if (!total) return 'No records';
    const start = (this.currentPage() - 1) * this.pageSize + 1;
    const end   = Math.min(this.currentPage() * this.pageSize, total);
    return `${start}–${end} of ${total.toLocaleString()}`;
  });

  hasPrev = computed(() => this.currentPage() > 1);
  hasNext = computed(() => this.currentPage() < this.totalPages());

  prevPage() { if (this.hasPrev()) this.currentPage.update(p => p - 1); }
  nextPage() { if (this.hasNext()) this.currentPage.update(p => p + 1); }

  ngOnInit() {
    this.load();
    this.routerSub = this.router.events
      .pipe(filter((e): e is NavigationEnd => e instanceof NavigationEnd))
      .subscribe(e => {
        if (e.urlAfterRedirects.includes('/retention-summary')) this.load();
      });
  }

  ngOnDestroy() { this.routerSub?.unsubscribe(); }

  load() {
    this.loading.set(true);
    this.error.set('');
    this.msg.set('');
    this.svc.getSummary(this.clientId).subscribe({
      next: (res) => {
        if (!res || res.total_interventions === 0) {
          this.msg.set('No retention data found yet.');
          this.data.set(null);
        } else {
          this.data.set(res);
        }
        this.loading.set(false);
      },
      error: () => {
        this.msg.set('Could not load summary data.');
        this.error.set('Failed to load summary.');
        this.loading.set(false);
      }
    });
  }

  // ── Card click → open modal ───────────────────────────────────────
  openCardView(type: 'all' | 'high' | 'medium' | 'escalated', label: string) {
    this.viewCard.set(type);
    this.viewLabel.set(label);
    this.viewRows.set([]);
    this.viewError.set('');
    this.viewLoading.set(true);
    this.currentPage.set(1);          // ← reset to page 1 on every open

    this.svc.getInterventionsFiltered(this.clientId, type).subscribe({
      next: (res) => {
        this.viewRows.set(res.interventions || res || []);
        this.viewLoading.set(false);
      },
      error: () => {
        this.viewError.set('Could not load details.');
        this.viewLoading.set(false);
      }
    });
  }

  closeCardView() {
    this.viewCard.set(null);
    this.viewLabel.set('');
    this.viewRows.set([]);
    this.viewError.set('');
    this.currentPage.set(1);
  }

  // ── Helpers ───────────────────────────────────────────────────────
  fmtPct(n: any)     { return (parseFloat(n) || 0).toFixed(1) + '%'; }
  fmtProb(n: any)    { return ((parseFloat(n) || 0) * 100).toFixed(1) + '%'; }
  fmtDate(d: string) { return d ? new Date(d).toLocaleDateString() : '—'; }
  riskColor(r: string)   { return r === 'HIGH' ? 'red' : r === 'MEDIUM' ? 'yellow' : 'green'; }
  channelIcon(c: string) { return c === 'email' ? '✉️' : c === 'sms' ? '📱' : '🔔'; }
}