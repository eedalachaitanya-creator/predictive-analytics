import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StrategistService } from '../../../services/strategist.service';

@Component({
  selector: 'strategist-stats',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './stats.html',
  styleUrls: ['./stats.scss']
})
export class StrategistStatsTab implements OnInit {
  private svc = inject(StrategistService);

  data    = signal<any>(null);
  loading = signal(true);
  error   = signal('');

  ngOnInit() { this.load(); }

  load() {
    this.loading.set(true);
    this.error.set('');
    this.svc.getPipelineStats().subscribe({
      next: (res) => { this.data.set(res); this.loading.set(false); },
      error: () => { this.error.set('Could not load stats. Langfuse may not be configured.'); this.loading.set(false); }
    });
  }

  nodeKeys(): string[] {
    return this.data()?.node_latencies ? Object.keys(this.data().node_latencies) : [];
  }

  maxLatency(): number {
    const nl = this.data()?.node_latencies;
    if (!nl) return 1;
    return Math.max(...Object.values(nl) as number[]) || 1;
  }

  barWidth(ms: number): string {
    return Math.min((ms / this.maxLatency()) * 100, 100).toFixed(0) + '%';
  }

  fmtMs(ms: number): string {
    return ms >= 1000 ? (ms / 1000).toFixed(2) + 's' : ms.toFixed(0) + 'ms';
  }

  fmtDate(d: string): string {
    return d ? new Date(d).toLocaleString() : '—';
  }
}