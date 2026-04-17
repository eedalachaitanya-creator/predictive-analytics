import { Component, inject, signal, OnInit } from '@angular/core';
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
export class RetentionEscalationsTab implements OnInit {
  private svc  = inject(RetentionService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  rows    = signal<any[]>([]);
  loading = signal(true);
  error   = signal('');

  ngOnInit() { this.load(); }

  load() {
    this.loading.set(true);
    this.svc.getEscalations(this.clientId).subscribe({
      next: (res) => { this.rows.set(res.escalations || []); this.loading.set(false); },
      error: () => { this.error.set('Failed to load escalations.'); this.loading.set(false); }
    });
  }

  fmtProb(n: number)  { return (n * 100).toFixed(1) + '%'; }
  fmtLtv(n: number)   { return '$' + (n || 0).toFixed(2); }
  fmtPct(n: number)   { return (n || 0).toFixed(1) + '%'; }
  fmtDate(d: string)  { return d ? new Date(d).toLocaleString() : '—'; }
  tierColor(t: string){ return t === 'Platinum' ? 'purple' : t === 'Gold' ? 'yellow' : t === 'Silver' ? 'gray' : 'orange'; }
}