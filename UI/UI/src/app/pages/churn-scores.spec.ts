import { TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { ChurnScoresComponent, toChurnCsv } from './churn-scores';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';
import { TierLabelService } from '../services/tier-label.service';

/**
 * The summary cards must read like plain English (CTO request): keep the risk
 * labels, drop the ML-jargon subtitles (ML predictions / probability ≥ 0.65 /
 * mean — not % HIGH). These tests render the component and assert the card
 * labels/subtitles so the jargon can't creep back.
 */
describe('ChurnScoresComponent — summary cards are layman-friendly', () => {
  let host: HTMLElement;

  const summary = { total_scored: 675, high_risk: 188, medium_risk: 83, low_risk: 404, avg_probability: 0.381 };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        { provide: ApiService, useValue: { get: () => of({ scores: [], summary, totalRows: 0, totalPages: 1 }) } },
        { provide: AuthService, useValue: { getClientId: () => 'CLT-001' } },
        { provide: TierLabelService, useValue: { refresh: () => {} } },
      ],
    });
    const fixture = TestBed.createComponent(ChurnScoresComponent);
    fixture.detectChanges();   // ngOnInit → loadScores (mocked) → renders the cards
    host = fixture.nativeElement as HTMLElement;
  });

  const subs = () => Array.from(host.querySelectorAll('.summary-sub')).map(e => (e.textContent || '').trim());
  const labels = () => Array.from(host.querySelectorAll('.summary-label')).map(e => (e.textContent || '').trim());

  it('keeps the High / Medium / Low risk labels and friendly total/avg labels', () => {
    const l = labels();
    expect(l).toContain('High Risk');
    expect(l).toContain('Medium Risk');
    expect(l).toContain('Low Risk');
    expect(l).toContain('Total Scored Customers');
    expect(l).toContain('Average Churn Risk');
  });

  it('drops every technical probability-formula / ML-jargon subtitle', () => {
    const joined = subs().join(' | ').toLowerCase();
    expect(joined).not.toContain('probability');     // "probability ≥ 0.65" etc.
    expect(joined).not.toContain('ml predictions');
    expect(joined).not.toContain('mean');             // "mean — not % HIGH"
    expect(joined).not.toContain('prob');             // "0.35 ≤ prob < 0.65"
  });

  it('still shows the underlying counts (data untouched)', () => {
    const text = (host.textContent || '');
    expect(text).toContain('675');
    expect(text).toContain('188');
  });
});


/**
 * The Churn Scores page gets a "Download CSV" button (the only download in the
 * app now — the Downloads page is removed). toChurnCsv turns the displayed
 * scores into a CSV; these tests pin the header, rows, and CSV escaping.
 */
describe('toChurnCsv — churn-scores CSV export', () => {
  const row = (over: Record<string, unknown> = {}) => ({
    customer_id: 'WMT-CUST-001', customer_name: 'Jane Smith', customer_email: 'jane@x.com',
    churn_probability: 0.91, risk_tier: 'HIGH',
    driver_1: 'Days Since Last Order', driver_2: '', driver_3: '',
    scored_at: '2026-06-03', model_version: 'temporal_rf',
    total_orders: 12, total_spend: 3400.5, avg_order_value: 283.4,
    rfm_recency: 1, rfm_frequency: 5, rfm_monetary: 5, rfm_total: 11,
    tier: 'Platinum', avg_rating: 4.2, total_tickets: 1, ...over,
  });

  it('emits a header row plus one row per score', () => {
    const lines = toChurnCsv([row(), row({ customer_id: 'WMT-CUST-002' })] as never).split('\n');
    expect(lines.length).toBe(3);
    expect(lines[0]).toContain('Customer ID');
    expect(lines[0]).toContain('Churn Probability');
    expect(lines[0]).toContain('Risk Tier');
    expect(lines[1]).toContain('WMT-CUST-001');
    expect(lines[1]).toContain('0.91');
    expect(lines[1]).toContain('HIGH');
  });

  it('escapes commas and quotes per CSV rules', () => {
    const csv = toChurnCsv([row({ customer_name: 'Smith, Jane "JJ"' })] as never);
    expect(csv).toContain('"Smith, Jane ""JJ"""');
  });

  it('handles an empty list (header only)', () => {
    expect(toChurnCsv([] as never).split('\n').length).toBe(1);
  });
});
