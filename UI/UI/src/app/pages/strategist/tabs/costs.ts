import { Component, inject, signal, OnInit, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { StrategistService } from '../../../services/strategist.service';
import { AuthService } from '../../../services/auth.service';

interface CostRow { name: string; cost: string; saved: boolean; }

@Component({
  selector: 'strategist-costs',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './costs.html',
  styleUrls: ['./costs.scss']
})
export class StrategistCostsTab implements OnInit {
  private svc  = inject(StrategistService);
  private auth = inject(AuthService);
  clientId     = this.auth.getClientId();

  rows        = signal<CostRow[]>([]);
  loading     = signal(false);
  loadingData = signal(true);
  success     = signal('');
  error       = signal('');

  ngOnInit() { this.loadData(); }

  loadData() {
    this.loadingData.set(true);
    this.error.set('');

    // Load product names from Scout entities
    this.svc.getSampleRequest(this.clientId).subscribe({
      next: (res: any) => {
        const productNames: string[] = (res?.scout_output?.products || []).map((p: any) => p.name);

        // Then load any already-saved costs
        this.svc.getCosts(this.clientId).subscribe({
          next: (costsRes: any) => {
            const saved: Record<string, string> = {};
            for (const c of (costsRes.costs || [])) {
              saved[c.product_name] = String(c.cost_usd);
            }
            // Merge: one row per Scout product, prefilled if already saved
            const rows = productNames.map(name => ({
              name,
              cost: saved[name] || '',
              saved: !!saved[name]
            }));
            this.rows.set(rows.length ? rows : []);
            this.loadingData.set(false);
          },
          error: () => {
            // Costs fetch failed — still show products with empty costs
            const rows = productNames.map(name => ({ name, cost: '', saved: false }));
            this.rows.set(rows);
            this.loadingData.set(false);
          }
        });
      },
      error: () => {
        // Server not running — show empty form, client can still enter manually
        this.rows.set([{ name: '', cost: '', saved: false }]);
        this.loadingData.set(false);
      }
    });
  }

  updateCost(i: number, value: string) {
    this.rows.update(r => {
      const updated = [...r];
      updated[i] = { ...updated[i], cost: value, saved: false };
      return updated;
    });
  }

  save() {
    this.error.set(''); this.success.set('');
    const costs: Record<string, number> = {};
    for (const row of this.rows()) {
      if (!row.cost) continue;
      const v = parseFloat(row.cost);
      if (isNaN(v) || v <= 0) {
        this.error.set('Cost price for "' + row.name + '" must be greater than zero.');
        return;
      }
      costs[row.name] = v;
    }
    if (!Object.keys(costs).length) {
      this.error.set('Enter at least one cost price before saving.');
      return;
    }
    this.loading.set(true);
    this.svc.saveCosts(this.clientId, costs).subscribe({
      next: () => {
        this.rows.update(r => r.map(row => ({ ...row, saved: !!costs[row.name] })));
        this.success.set('Cost prices saved. The pricing engine will use these on every run.');
        this.loading.set(false);
      },
      error: (err) => {
        this.error.set(err?.error?.detail || 'Failed to save. Please try again.');
        this.loading.set(false);
      }
    });
  }

  hasUnsaved = computed(() => this.rows().some(r => r.cost && !r.saved));
}