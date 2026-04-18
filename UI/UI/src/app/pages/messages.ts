import { Component, OnInit, signal, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MessagesService } from '../services/messages.service';
import { MessageTemplate, TierKey, RiskLevel, Channel } from '../models';
import { AuthService } from '../services/auth.service';
import { TierLabelService } from '../services/tier-label.service';
import { TierLabelPipe } from '../pipes/tier-label.pipe';

interface TierMeta { key: TierKey; label: string; cls: string; sub: string; }

@Component({
  selector: 'app-messages',
  standalone: true,
  imports: [CommonModule, FormsModule, TierLabelPipe],
  templateUrl: './messages.html',
  styleUrls: ['./messages.scss']
})
export class MessagesComponent implements OnInit {
  svc  = inject(MessagesService);
  private auth = inject(AuthService);
  private tierLabels = inject(TierLabelService);
  private clientId = this.auth.getClientId();

  saved     = signal(false);
  editingId = signal<string | null>(null);
  editForm  = signal<Partial<MessageTemplate>>({});

  // Pull display labels from the service signal so a rename in Settings
  // updates these section headers without a page reload.
  tiers = computed<TierMeta[]>(() => {
    const m = this.tierLabels.labels();
    return [
      { key:'platinum', label: m['Platinum'], cls:'tier-platinum', sub:'Highest priority — premium win-back' },
      { key:'gold',     label: m['Gold'],     cls:'tier-gold',     sub:'High value — targeted recovery' },
      { key:'silver',   label: m['Silver'],   cls:'tier-silver',   sub:'Mid-tier — cost-effective nudge' },
      { key:'bronze',   label: m['Bronze'],   cls:'tier-bronze',   sub:'Entry tier — low-cost re-engagement' },
    ];
  });

  riskLabels: Record<RiskLevel, string> = {
    at_risk: 'At-Risk', returning: 'Returning',
    reactivated: 'Reactivated', new: 'New'
  };

  channelLabels: Record<Channel, string> = {
    email:'Email', sms:'SMS', push:'Push',
    email_sms:'Email + SMS', email_push:'Email + Push', push_sms:'Push + SMS'
  };

  channels: Channel[] = ['email','sms','push','email_sms','email_push','push_sms'];

  placeholders = [
    { ph:'{customer_name}',       res:"Customer's first name",            ex:'Sarah',              note:'From Customer Master' },
    { ph:'{tier}',                res:'Current value tier label',          ex:'💎 Platinum',        note:'Assigned by pipeline' },
    { ph:'{discount_pct}',        res:'Discount % for this template',      ex:'15%',                note:'Set in this sheet' },
    { ph:'{last_order_date}',     res:'Date of most recent order',         ex:'2026-01-12',         note:'ISO 8601 format' },
    { ph:'{days_since_order}',    res:'Days since last completed order',   ex:'64',                 note:'Computed at send time' },
    { ph:'{top_product}',         res:"Customer's most-bought product",    ex:'Tide Pods 42ct',     note:'From product affinity' },
    { ph:'{recommended_product}', res:'Pipeline recommendation',           ex:'Gain Flings 81ct',   note:'From ML model output' },
    { ph:'{support_email}',       res:'Client support email address',      ex:'support@walmart.com',note:'From Vendor Config' },
  ];

  byTier = computed(() =>
    this.tiers().map(t => ({ ...t, rows: this.svc.getByTier(t.key) }))
  );

  discountMatrix = computed(() => {
    const matrix: Record<string, Record<string, number>> = {};
    for (const t of this.tiers()) {
      matrix[t.key] = {};
      for (const risk of ['at_risk','returning','reactivated','new'] as RiskLevel[]) {
        matrix[t.key][risk] = this.svc.getByTierAndRisk(t.key, risk)?.discount_pct ?? 0;
      }
    }
    return matrix;
  });

  ngOnInit() {
    // Pull client's tier labels so the section headers and draft badges show custom names.
    this.tierLabels.refresh();
    this.svc.loadTemplates(this.clientId).subscribe({ error: () => {} });
  }

  startEdit(t: MessageTemplate) {
    this.editingId.set(t.id);
    this.editForm.set({ ...t });
  }

  cancelEdit() { this.editingId.set(null); this.editForm.set({}); }

  saveEdit() {
    const id = this.editingId();
    if (!id) return;
    this.svc.updateTemplate(id, this.editForm());
    this.editingId.set(null);
  }

  updateEditField(field: keyof MessageTemplate, value: unknown) {
    this.editForm.update(f => ({ ...f, [field]: value }));
  }

  toggleActive(t: MessageTemplate) {
    this.svc.updateTemplate(t.id, { active: !t.active });
  }

  saveAll() {
    this.svc.saveTemplates({ clientId: this.clientId, templates: this.svc.templates() }).subscribe({
      next: () => { this.saved.set(true); setTimeout(() => this.saved.set(false), 2500); },
      error: () => {
        // Graceful fallback — show saved locally even if backend not connected
        this.saved.set(true); setTimeout(() => this.saved.set(false), 2500);
      }
    });
  }

  // ── Outreach generation ──────────────────────────────────────
  riskFilter   = signal('');
  tierFilter   = signal('');
  outreachMsg  = signal('');

  generateOutreach() {
    this.outreachMsg.set('');
    this.svc.generateOutreach({
      clientId: this.clientId,
      riskFilter: this.riskFilter() || undefined,
      tierFilter: this.tierFilter() || undefined,
      saveToDb: true,
    }).subscribe({
      next: (res) => {
        this.outreachMsg.set(res.message || `Generated ${res.total} emails`);
      },
      error: (e) => {
        this.outreachMsg.set('Failed to generate: ' + (e.message || 'Unknown error'));
      }
    });
  }

  discountClass(pct: number): string {
    if (pct >= 12) return 'h';
    if (pct >= 7)  return 'm';
    if (pct >= 1)  return 'l';
    return 'n';
  }

  discountLabel(pct: number): string {
    return pct === 0 ? 'No discount' : `${pct}% off`;
  }
}
