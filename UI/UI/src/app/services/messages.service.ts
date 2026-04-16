import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import {
  MessageTemplate, SaveTemplatesRequest,
  TierKey, RiskLevel, Channel,
  DB_TO_TIER_KEY, DB_TO_RISK_LEVEL, DB_TO_CHANNEL,
  ValueProposition
} from '../models';

// Default templates — field names now match DB (tier_name, risk_level, etc.)
// subject / body are the extended fields managed by the messages API.
export const DEFAULT_TEMPLATES: MessageTemplate[] = [
  { id:'tpl-001', tier_name:'platinum', risk_level:'at_risk',    discount_pct:15, channel:'email_sms', action_type:'Personal Outreach', message_template:'Hi {name}, we miss you! Here\'s 15% off your favourite category.', priority:1, subject:'We miss you, {customer_name} — here\'s 15% off', body:'Hi {customer_name}, it\'s been {days_since_order} days since your last order. As a valued Platinum member, enjoy 15% off your next purchase. Use code PLAT15. Your favourite: {top_product}. Offer expires in 7 days.', active:true, updatedAt:'' },
  { id:'tpl-002', tier_name:'platinum', risk_level:'returning',  discount_pct:10, channel:'email',     action_type:'Loyalty Reward',    message_template:'Great to see you back, {name}! 10% off — no minimum spend.',           priority:2, subject:'Welcome back, {customer_name}! Your 10% loyalty reward is waiting', body:'Great to see you back, {customer_name}! As a Platinum member returning to us, we\'re rewarding you with 10% off — no minimum spend. Code: BACK10.', active:true, updatedAt:'' },
  { id:'tpl-003', tier_name:'platinum', risk_level:'reactivated',discount_pct:12, channel:'email_push',action_type:'Reactivation',      message_template:'Welcome back to Platinum, {name}! 12% off your reactivation order.',       priority:3, subject:'{customer_name}, you\'re back — celebrating with 12% off', body:'Hi {customer_name}, welcome back to Platinum! We\'ve missed you. Enjoy 12% off your reactivation order. Code: REACT12.', active:true, updatedAt:'' },
  { id:'tpl-004', tier_name:'platinum', risk_level:'new',        discount_pct:5,  channel:'email',     action_type:'Welcome',           message_template:'Congratulations {name} on reaching Platinum status! 5% off to start.',    priority:4, subject:'Welcome to Platinum, {customer_name} — a 5% head start', body:'Congratulations {customer_name} on reaching Platinum status! Start your journey with 5% off. Code: NEW5.', active:true, updatedAt:'' },
  { id:'tpl-005', tier_name:'gold',     risk_level:'at_risk',    discount_pct:10, channel:'email_sms', action_type:'Personal Outreach', message_template:'Hi {name}, don\'t let your Gold status slip — 10% off.',                  priority:1, subject:'{customer_name}, don\'t let your Gold status slip — 10% off', body:'Hi {customer_name}, we noticed it\'s been {days_since_order} days. Here\'s 10% off. Code: GOLD10.', active:true, updatedAt:'' },
  { id:'tpl-006', tier_name:'gold',     risk_level:'returning',  discount_pct:8,  channel:'email',     action_type:'Loyalty Reward',    message_template:'You\'re back, {name}! Gold members deserve a treat — 8% off.',           priority:2, subject:'You\'re back, {customer_name}! Enjoy 8% off', body:'Welcome back, {customer_name}! Gold members deserve a treat. Here\'s 8% off. Code: GOLDBACK8.', active:true, updatedAt:'' },
  { id:'tpl-007', tier_name:'gold',     risk_level:'reactivated',discount_pct:10, channel:'email_push',action_type:'Reactivation',      message_template:'Reactivated and ready — 10% off your Gold comeback.',                    priority:3, subject:'Reactivated and ready — 10% off your Gold comeback', body:'Hi {customer_name}, great to see you again! Enjoy 10% off. Code: GREACT10.', active:true, updatedAt:'' },
  { id:'tpl-008', tier_name:'gold',     risk_level:'new',        discount_pct:5,  channel:'email',     action_type:'Welcome',           message_template:'Welcome to Gold, {name} — 5% off to celebrate.',                         priority:4, subject:'Welcome to Gold, {customer_name} — 5% off to celebrate', body:'Hi {customer_name}, you\'ve earned Gold status! 5% off your next order. Code: GNEW5.', active:true, updatedAt:'' },
  { id:'tpl-009', tier_name:'silver',   risk_level:'at_risk',    discount_pct:8,  channel:'push_sms',  action_type:'Personal Outreach', message_template:'Hi {name}, we miss you! Come back with 8% off.',                          priority:1, subject:'Come back, {customer_name} — 8% off just for you', body:'Hi {customer_name}, we miss you! {days_since_order} days since your last order. Come back with 8% off. Code: SIL8.', active:true, updatedAt:'' },
  { id:'tpl-010', tier_name:'silver',   risk_level:'returning',  discount_pct:5,  channel:'email',     action_type:'Loyalty Reward',    message_template:'Good to see you again, {name} — 5% off.',                                 priority:2, subject:'Good to see you again, {customer_name} — 5% off', body:'Welcome back, {customer_name}! Enjoy 5% off as a returning Silver member. Code: SILBACK5.', active:true, updatedAt:'' },
  { id:'tpl-011', tier_name:'silver',   risk_level:'reactivated',discount_pct:7,  channel:'email',     action_type:'Reactivation',      message_template:'You\'re reactivated, {name} — 7% off.',                                   priority:3, subject:'{customer_name}, you\'re reactivated — 7% off', body:'Hi {customer_name}, glad you\'re back! 7% off your reactivation order. Code: SREACT7.', active:true, updatedAt:'' },
  { id:'tpl-012', tier_name:'silver',   risk_level:'new',        discount_pct:3,  channel:'email',     action_type:'Welcome',           message_template:'Welcome to Silver, {name} — 3% off your first order.',                    priority:4, subject:'Welcome to Silver, {customer_name} — 3% off', body:'Congratulations {customer_name}! You\'ve reached Silver. 3% off your next order. Code: SNEW3.', active:true, updatedAt:'' },
  { id:'tpl-013', tier_name:'bronze',   risk_level:'at_risk',    discount_pct:5,  channel:'push_sms',  action_type:'Personal Outreach', message_template:'Hi {name}, it\'s been {days_since_order} days. Come back with 5% off.',   priority:1, subject:'We haven\'t seen you in a while — 5% off to come back', body:'Hi {customer_name}, it\'s been {days_since_order} days. Come back with 5% off. Code: BRZ5.', active:true, updatedAt:'' },
  { id:'tpl-014', tier_name:'bronze',   risk_level:'returning',  discount_pct:3,  channel:'push',      action_type:'Loyalty Reward',    message_template:'Welcome back, {name}! 3% off your next order.',                           priority:2, subject:'Welcome back, {customer_name}! 3% off', body:'Good to see you, {customer_name}! 3% off your next order. Code: BRZBACK3.', active:true, updatedAt:'' },
  { id:'tpl-015', tier_name:'bronze',   risk_level:'reactivated',discount_pct:5,  channel:'email',     action_type:'Reactivation',      message_template:'Welcome back! 5% off your reactivation order.',                           priority:3, subject:'You\'re back — 5% off your reactivation order', body:'Hi {customer_name}, welcome back! 5% off your reactivation order. Code: BREACT5.', active:true, updatedAt:'' },
  { id:'tpl-016', tier_name:'bronze',   risk_level:'new',        discount_pct:0,  channel:'email',     action_type:'Welcome',           message_template:'Welcome, {name}! Explore our full catalogue.',                             priority:4, subject:'Welcome, {customer_name}! Explore our catalogue', body:'Hi {customer_name}, welcome aboard! Browse our full range at walmart.com.', active:true, updatedAt:'' },
];

/** Build a MessageTemplate from a DB ValueProposition row */
export function fromValueProposition(vp: ValueProposition, idx: number): MessageTemplate {
  return {
    id: `tpl-${String(idx + 1).padStart(3, '0')}`,
    tier_name:        DB_TO_TIER_KEY[vp.tier_name],
    risk_level:       DB_TO_RISK_LEVEL[vp.risk_level],
    discount_pct:     vp.discount_pct,
    channel:          DB_TO_CHANNEL[vp.channel],
    action_type:      vp.action_type,
    message_template: vp.message_template,
    priority:         vp.priority,
    // Extended fields default to empty — set by messages API
    subject:   '',
    body:      vp.message_template,
    active:    true,
    updatedAt: '',
  };
}

@Injectable({ providedIn: 'root' })
export class MessagesService {
  private api = inject(ApiService);

  readonly templates = signal<MessageTemplate[]>([...DEFAULT_TEMPLATES]);
  readonly loading   = signal(false);
  readonly saving    = signal(false);
  readonly error     = signal<string | null>(null);

  loadTemplates(clientId: string): Observable<MessageTemplate[]> {
    this.loading.set(true);
    return this.api.get<MessageTemplate[]>(`/messages/templates?clientId=${clientId}`).pipe(
      tap({
        next:  t => { this.templates.set(t); this.loading.set(false); },
        error: e => {
          // Fall back to defaults if backend not yet available
          this.templates.set([...DEFAULT_TEMPLATES]);
          this.error.set(e.message);
          this.loading.set(false);
        }
      })
    );
  }

  saveTemplates(req: SaveTemplatesRequest): Observable<MessageTemplate[]> {
    this.saving.set(true);
    return this.api.post<MessageTemplate[]>('/messages/templates', req).pipe(
      tap({
        next:  t => { this.templates.set(t); this.saving.set(false); },
        error: e => { this.error.set(e.message); this.saving.set(false); }
      })
    );
  }

  updateTemplate(id: string, changes: Partial<MessageTemplate>): void {
    this.templates.update(list =>
      list.map(t => t.id === id ? { ...t, ...changes, updatedAt: new Date().toISOString() } : t)
    );
  }

  getByTierAndRisk(tier: TierKey, risk: RiskLevel): MessageTemplate | undefined {
    return this.templates().find(t => t.tier_name === tier && t.risk_level === risk);
  }

  getByTier(tier: TierKey): MessageTemplate[] {
    return this.templates().filter(t => t.tier_name === tier);
  }

  // ── Outreach generation ─────────────────────────────────────
  readonly generating = signal(false);
  readonly outreachDrafts = signal<any[]>([]);

  generateOutreach(req: {
    clientId: string;
    riskFilter?: string;
    tierFilter?: string;
    customerIds?: string[];
    saveToDb?: boolean;
  }): Observable<any> {
    this.generating.set(true);
    return this.api.post<any>('/messages/generate-outreach', {
      clientId:    req.clientId,
      riskFilter:  req.riskFilter ?? null,
      tierFilter:  req.tierFilter ?? null,
      customerIds: req.customerIds ?? null,
      saveToDb:    req.saveToDb ?? true,
    }).pipe(
      tap({
        next:  res => { this.outreachDrafts.set(res.drafts ?? []); this.generating.set(false); },
        error: e   => { this.error.set(e.message); this.generating.set(false); }
      })
    );
  }

  getOutreachHistory(clientId: string, page: number = 1): Observable<any> {
    return this.api.get(`/messages/outreach-history?clientId=${clientId}&page=${page}`);
  }
}
