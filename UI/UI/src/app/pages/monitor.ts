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

interface PerClientRow {
  client_id: string;
  client_name: string | null;
  total_calls: number;
  total_cost: number;
  total_tokens: number;
  calls_today: number;
  cost_today: number;
  calls_30d: number;
  cost_30d: number;
  over_budget_count: number;
  over_budget_pct: number;
  avg_cost_per_call: number;
  last_call: string | null;
}

interface PerClientTotals {
  total_calls: number;
  total_cost: number;
  total_tokens: number;
  calls_today: number;
  cost_today: number;
  calls_30d: number;
  cost_30d: number;
  over_budget_count: number;
}

interface PerClientResponse {
  clients: PerClientRow[];
  totals: PerClientTotals;
  target_per_call?: number;
  langfuse_enabled?: boolean;
  langfuse_configured?: boolean;
  error?: string;
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

  // Cross-tenant per-client breakdown for admins.
  perClient = signal<PerClientRow[]>([]);
  perClientTotals = signal<PerClientTotals | null>(null);
  perClientLoading = signal(true);
  perClientError = signal<string | null>(null);

  ngOnInit() {
    this.loadCostConfig();
    this.loadPerClient();
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

  loadPerClient() {
    this.perClientLoading.set(true);
    this.perClientError.set(null);
    this.http.get<PerClientResponse>(`${this.base}/cost-tracking/per-client`).subscribe({
      next: (res) => {
        this.perClient.set(res.clients || []);
        this.perClientTotals.set(res.totals || null);
        this.perClientLoading.set(false);
        if (res.error) this.perClientError.set(res.error);
      },
      error: () => {
        this.perClientError.set('Could not load per-client cost data.');
        this.perClientLoading.set(false);
      },
    });
  }

  formatTokenCost(cost: number): string {
    return '$' + (cost * 1_000_000).toFixed(2) + ' / 1M tokens';
  }

  // Cost can be fractions of a cent — show 4 decimals for clarity.
  formatUsd(value: number | null | undefined): string {
    if (value == null) return '$0.0000';
    return '$' + Number(value).toFixed(4);
  }

  // "2026-04-21T14:33:11.123+00:00" → "Apr 21, 14:33" (local time).
  formatTimestamp(iso: string | null): string {
    if (!iso) return '—';
    try {
      const d = new Date(iso);
      return d.toLocaleString(undefined, {
        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
      });
    } catch {
      return iso;
    }
  }

  // "CLT-001" + "Walmart" → "Walmart (CLT-001)" ; unknown names fall back to the id.
  formatClient(row: PerClientRow): string {
    if (row.client_name) return `${row.client_name} (${row.client_id})`;
    return row.client_id;
  }
}
