import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StrategistService, PriceContext } from '../../../services/strategist.service';
import { AuthService } from '../../../services/auth.service';

@Component({
  selector: 'strategist-context',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './context.html',
  styleUrls: ['./context.scss']
})
export class StrategistContextTab implements OnInit {
  private svc  = inject(StrategistService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  rows    = signal<PriceContext[]>([]);
  loading = signal(true);
  error   = signal('');

  ngOnInit() { this.load(); }

  load() {
    this.loading.set(true);
    this.error.set('');
    this.svc.getPriceContexts(this.clientId).subscribe({
      next: (res) => { this.rows.set(res.contexts || res || []); this.loading.set(false); },
      error: (err) => { this.error.set(err?.error?.detail || 'Failed to load.'); this.loading.set(false); }
    });
  }

  fmtPrice(n: number) { return '₹' + (n || 0).toFixed(2); }
  fmtPct(n: number)   { return (n || 0).toFixed(1) + '%'; }
  fmtDate(d: string)  { return d ? new Date(d).toLocaleString() : '—'; }
}