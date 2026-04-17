import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RetentionService, RetentionResponse, Intervention } from '../../../services/retention.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'retention-run',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './run.html',
  styleUrls: ['./run.scss']
})
export class RetentionRunTab {
  private svc  = inject(RetentionService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  dryRun   = signal(true);
  minRisk  = signal<'HIGH' | 'MEDIUM'>('MEDIUM');
  loading  = signal(false);
  error    = signal('');
  result   = signal<RetentionResponse | null>(null);
  expanded = signal<number | null>(null);

  run() {
    this.error.set('');
    this.result.set(null);
    this.loading.set(true);

    this.svc.run({ client_id: this.clientId, dry_run: this.dryRun(), min_risk: this.minRisk() }).subscribe({
      next: (res) => { this.result.set(res); this.loading.set(false); },
      error: (err) => {
        this.error.set(err?.error?.detail || 'Retention pipeline failed.');
        this.loading.set(false);
      }
    });
  }

  toggleExpand(i: number) { this.expanded.set(this.expanded() === i ? null : i); }

  riskColor(r: string)    { return r === 'HIGH' ? 'red' : r === 'MEDIUM' ? 'yellow' : 'green'; }
  channelIcon(c: string)  { return c === 'email' ? '✉️' : c === 'sms' ? '📱' : '🔔'; }
  fmtLtv(n: number)       { return '$' + (n || 0).toFixed(2); }
  fmtPct(n: number)       { return (n || 0).toFixed(1) + '%'; }
  fmtProb(n: number)      { return (n * 100).toFixed(1) + '%'; }

  summaryKeys(s: any): string[] { return s ? Object.keys(s) : []; }
}