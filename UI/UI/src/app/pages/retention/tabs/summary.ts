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
  router = inject(Router);  // public so template can use router.navigate()
  private routerSub?: Subscription;
  clientId     = this.auth.getClientId();

  data         = signal<RetentionSummary | null>(null);
  loading      = signal(true);
  error        = signal('');
  msg          = signal('');

  // Drill-down: which filter is active and the filtered list
  activeFilter = signal<'all' | 'HIGH' | 'MEDIUM' | null>(null);
  allInterventions = signal<any[]>([]);
  allFiltered = signal<any[]>([]);          // full filtered list
  filteredInterventions = signal<any[]>([]); // current page slice
  drillLoading = signal(false);
  drillPage = signal(1);
  readonly DRILL_PAGE_SIZE = 10;
  drillTotal = signal(0);

  showDrillDown(tier: 'all' | 'HIGH' | 'MEDIUM') {
    // Toggle off if same card clicked again
    if (this.activeFilter() === tier) {
      this.activeFilter.set(null);
      return;
    }
    this.activeFilter.set(tier);
    if (this.allInterventions().length === 0) {
      this.drillLoading.set(true);
      this.svc.getInterventions(this.clientId).subscribe({
        next: (res: any) => {
          const items = res.interventions || res.data || res || [];
          this.allInterventions.set(items);
          this.applyTierFilter(tier, items);
          this.drillLoading.set(false);
        },
        error: () => this.drillLoading.set(false)
      });
    } else {
      this.applyTierFilter(tier, this.allInterventions());
    }
  }

  private applyTierFilter(tier: string, items: any[]) {
    const filtered = tier === 'all' ? items : items.filter((i: any) => i.risk_tier === tier);
    this.allFiltered.set(filtered);
    this.drillTotal.set(filtered.length);
    this.drillPage.set(1);
    this.filteredInterventions.set(filtered.slice(0, this.DRILL_PAGE_SIZE));
  }

  nextDrillPage() {
    const next = this.drillPage() + 1;
    const start = (next - 1) * this.DRILL_PAGE_SIZE;
    if (start >= this.drillTotal()) return;
    this.drillPage.set(next);
    this.filteredInterventions.set(this.allFiltered().slice(start, start + this.DRILL_PAGE_SIZE));
  }

  prevDrillPage() {
    if (this.drillPage() <= 1) return;
    const prev = this.drillPage() - 1;
    const start = (prev - 1) * this.DRILL_PAGE_SIZE;
    this.drillPage.set(prev);
    this.filteredInterventions.set(this.allFiltered().slice(start, start + this.DRILL_PAGE_SIZE));
  }

  drillHasNext() { return this.drillPage() * this.DRILL_PAGE_SIZE < this.drillTotal(); }

  drillRange(): string {
    if (this.drillTotal() === 0) return '0 of 0';
    const start = (this.drillPage() - 1) * this.DRILL_PAGE_SIZE + 1;
    const end = Math.min(this.drillPage() * this.DRILL_PAGE_SIZE, this.drillTotal());
    const pages = Math.ceil(this.drillTotal() / this.DRILL_PAGE_SIZE);
    return `${start}–${end} of ${this.drillTotal()} · Page ${this.drillPage()} of ${pages}`;
  }

  riskClass(tier: string) {
    return tier === 'HIGH' ? 'badge red' : tier === 'MEDIUM' ? 'badge yellow' : 'badge gray';
  }

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