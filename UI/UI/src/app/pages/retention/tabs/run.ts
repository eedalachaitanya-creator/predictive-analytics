import { Component, inject, signal, computed } from '@angular/core';
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
  sending  = signal(false);
  error    = signal('');
  result   = signal<RetentionResponse | null>(null);
  expanded = signal<number | null>(null);

  // Customer selection state
  selectedIds     = signal<Set<string>>(new Set());
  // Per-customer discount overrides {customer_id: discount_pct}
  discountEdits   = signal<Record<string, number>>({});

  // Computed: how many customers are selected
  selectedCount = computed(() => this.selectedIds().size);

  // Computed: all customers selected?
  allSelected = computed(() => {
    const interventions = this.result()?.interventions || [];
    return interventions.length > 0 && this.selectedIds().size === interventions.length;
  });

  run() {
    this.error.set('');
    if (!this.clientId) {
      this.error.set('No client selected. Please select a client from the top menu.');
      return;
    }
    this.result.set(null);
    this.selectedIds.set(new Set());
    this.discountEdits.set({});
    this.loading.set(true);

    this.svc.run({
      client_id:              this.clientId,
      dry_run:                true,   // always preview first
      min_risk:               this.minRisk(),
      min_probability_medium: 0.40,
    }).subscribe({
      next: (res) => {
        this.result.set(res);
        // Auto-select only customers with actual discounts
        // Re-engagement (0% discount) customers are unchecked by default
        const allIds = new Set(
          res.interventions
            .filter(i => i.discount_pct > 0)
            .map(i => i.customer_id)
        );
        this.selectedIds.set(allIds);
        this.loading.set(false);
      },
      error: (err) => {
        this.error.set(err?.error?.detail || 'Retention pipeline failed.');
        this.loading.set(false);
      }
    });
  }

  sendSelected() {
    this.error.set('');
    if (this.selectedCount() === 0) {
      this.error.set('Select at least one customer to send offers.');
      return;
    }
    this.sending.set(true);

    const customerIds    = Array.from(this.selectedIds());
    const customDiscounts = Object.keys(this.discountEdits()).length > 0
      ? this.discountEdits()
      : null;

    this.svc.run({
      client_id:              this.clientId,
      dry_run:                false,
      min_risk:               this.minRisk(),
      min_probability_medium: 0.40,
      customer_ids:           customerIds,
      custom_discounts:       customDiscounts,
    }).subscribe({
      next: (res) => {
        this.result.set(res);
        this.sending.set(false);
      },
      error: (err) => {
        this.error.set(err?.error?.detail || 'Failed to send offers.');
        this.sending.set(false);
      }
    });
  }

  toggleSelect(customerId: string) {
    const current = new Set(this.selectedIds());
    if (current.has(customerId)) {
      current.delete(customerId);
    } else {
      current.add(customerId);
    }
    this.selectedIds.set(current);
  }

  toggleSelectAll() {
    const interventions = this.result()?.interventions || [];
    if (this.allSelected()) {
      this.selectedIds.set(new Set());
    } else {
      this.selectedIds.set(new Set(interventions.map(i => i.customer_id)));
    }
  }

  isSelected(customerId: string): boolean {
    return this.selectedIds().has(customerId);
  }

  getDiscount(item: Intervention): number {
    return this.discountEdits()[item.customer_id] ?? item.discount_pct;
  }

  setDiscount(customerId: string, value: string) {
    const v = parseFloat(value);
    if (isNaN(v) || v < 0 || v > 100) return;
    this.discountEdits.update(d => ({ ...d, [customerId]: v }));
  }

  // Returns offer_type with the pct updated to match the user's edited discount.
  // e.g. "winback_discount_5pct" → "winback_discount_10pct" when user edits to 10%.
  getOfferType(item: Intervention): string {
    const edited = this.discountEdits()[item.customer_id];
    if (edited === undefined || edited === item.discount_pct) {
      return item.offer_type;
    }
    return item.offer_type.replace(
      /_\d+pct$/,
      `_${Math.round(edited)}pct`
    );
  }

  // Returns the offer message with the discount % replaced by the user's
  // edited value so the preview stays in sync with the discount input.
  getOfferMessage(item: Intervention): string {
    const edited = this.discountEdits()[item.customer_id];
    if (edited === undefined || edited === item.discount_pct) {
      return item.offer_message;
    }
    const original = item.discount_pct;
    // Replace e.g. "5% discount" / "5%" / "5.0%" with the new value
    return item.offer_message
      .replace(
        new RegExp(`${original}\.?0*\s*%`, 'g'),
        `${edited}%`
      );
  }

  toggleExpand(i: number) {
    this.expanded.set(this.expanded() === i ? null : i);
  }

  riskColor(r: string)   { return r === 'HIGH' ? 'red' : r === 'MEDIUM' ? 'yellow' : 'green'; }
  channelIcon(c: string) { return c === 'email' ? '✉️' : c === 'sms' ? '📱' : '🔔'; }
  fmtLtv(n: number)      { return '$' + (n || 0).toFixed(2); }
  fmtPct(n: any)         { return (parseFloat(n) || 0).toFixed(1) + '%'; }
  fmtProb(n: any)        { return ((parseFloat(n) || 0) * 100).toFixed(1) + '%'; }

  summaryKeys(s: any): string[] { return s ? Object.keys(s) : []; }

  isObject(v: any): boolean {
    return v !== null && typeof v === 'object' && !Array.isArray(v);
  }

  objectEntries(obj: any): [string, any][] {
    return obj ? Object.entries(obj) : [];
  }
}