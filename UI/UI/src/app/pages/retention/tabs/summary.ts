import { Component, inject, signal, OnInit } from '@angular/core';
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
export class RetentionSummaryTab implements OnInit {
  private svc  = inject(RetentionService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  data    = signal<RetentionSummary | null>(null);
  loading = signal(true);
  error   = signal('');
  msg     = signal('');

  ngOnInit() { this.load(); }

  load() {
    this.loading.set(true);
    this.error.set('');
    this.svc.getSummary(this.clientId).subscribe({
      next: (res: any) => {
        if (res.message) { this.msg.set(res.message); this.data.set(null); }
        else             { this.data.set(res); this.msg.set(''); }
        this.loading.set(false);
      },
      error: () => { this.error.set('Failed to load summary.'); this.loading.set(false); }
    });
  }

  fmtPct(n: number)      { return (n || 0).toFixed(1) + '%'; }
  fmtRevenue(n: number)  { return '$' + (n || 0).toFixed(2); }
  conversionWidth(n: number) { return Math.min(n, 100).toFixed(0) + '%'; }
}