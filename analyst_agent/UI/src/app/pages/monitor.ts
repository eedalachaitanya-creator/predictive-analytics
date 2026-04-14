import { Component, OnInit, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';

interface CostConfig {
  target_per_call: number;
  cost_per_input_token: number;
  cost_per_output_token: number;
  langfuse_enabled: boolean;
  langfuse_configured: boolean;
}

@Component({
  selector: 'app-monitor',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './monitor.html',
  styleUrls: ['./monitor.scss'],
})
export class MonitorComponent implements OnInit {
  private http = inject(HttpClient);
  private base = environment.apiUrl;

  costConfig = signal<CostConfig | null>(null);
  costLoading = signal(true);
  costError = signal<string | null>(null);

  live = [
    { id: 'JOB-4821', client: 'CLT-001 · Walmart', by: 'ops@walmart.com', started: '22:43:41', stage: 'Stage 9/10', pct: 85, eta: '~1s', status: 'running' },
    { id: 'JOB-4820', client: 'CLT-002 · Target', by: 'bi@target.com', started: '22:41:10', stage: 'Stage 5/10', pct: 50, eta: '~8s', status: 'running' },
    { id: 'JOB-4819', client: 'CLT-003 · Costco', by: 'bi@costco.com', started: '22:44:00', stage: '—', pct: 0, eta: '—', status: 'queued' },
  ];
  history = [
    { id: 'JOB-4818', client: 'CLT-001', date: '2026-03-17 21:10', dur: '6.1s', cust: 200, ord: 1894, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4817', client: 'CLT-002', date: '2026-03-17 14:12', dur: '5.8s', cust: 185, ord: 1742, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4816', client: 'CLT-003', date: '2026-03-17 09:30', dur: '7.1s', cust: 312, ord: 2890, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4815', client: 'CLT-001', date: '2026-03-16 22:45', dur: '6.3s', cust: 200, ord: 1891, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4814', client: 'CLT-002', date: '2026-03-16 14:10', dur: '5.9s', cust: 185, ord: 1740, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4813', client: 'CLT-003', date: '2026-03-16 10:00', dur: '7.4s', cust: 312, ord: 2888, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4812', client: 'CLT-004', date: '2026-03-15 11:20', dur: '3.2s', cust: 50, ord: 310, feat: 65, sheets: '12 sheets', ok: false },
    { id: 'JOB-4811', client: 'CLT-001', date: '2026-03-15 22:31', dur: '6.0s', cust: 200, ord: 1889, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4810', client: 'CLT-002', date: '2026-03-15 13:55', dur: '5.7s', cust: 185, ord: 1738, feat: 65, sheets: '12 sheets', ok: true },
    { id: 'JOB-4809', client: 'CLT-003', date: '2026-03-15 09:10', dur: '7.6s', cust: 312, ord: 2885, feat: 65, sheets: '12 sheets', ok: true },
  ];

  ngOnInit() {
    this.loadCostConfig();
  }

  loadCostConfig() {
    this.costLoading.set(true);
    this.costError.set(null);
    this.http.get<CostConfig>(`${this.base}/cost-tracking`).subscribe({
      next: (res) => {
        this.costConfig.set(res);
        this.costLoading.set(false);
      },
      error: () => {
        this.costError.set('Could not load cost tracking info.');
        this.costLoading.set(false);
      },
    });
  }

  formatTokenCost(cost: number): string {
    return '$' + (cost * 1_000_000).toFixed(2) + ' / 1M tokens';
  }
}
