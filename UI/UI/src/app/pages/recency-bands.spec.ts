import { describe, it, expect } from 'vitest';
import { recencyDescription } from './recency-bands';

/**
 * Short, user-friendly captions under the Purchase-Recency bands. The day
 * boundaries are derived from the tenant's churn window (and its midpoint),
 * so they must stay in sync with whatever window the tenant configured.
 */
describe('recencyDescription — Purchase-Recency band captions', () => {
  it('describes Lapsed as no order beyond the full window', () => {
    expect(recencyDescription('Churned', 40)).toBe('No order in 40+ days');
  });

  it('describes Slowing Down as the second half of the window', () => {
    expect(recencyDescription('At-Risk', 40)).toBe('Last order 20–40 days ago');
  });

  it('describes Recently Purchased as the recent half of the window', () => {
    expect(recencyDescription('Active', 40)).toBe('Ordered in the last 20 days');
  });

  it('rounds the midpoint for odd windows (90 → 45)', () => {
    expect(recencyDescription('Active', 90)).toBe('Ordered in the last 45 days');
    expect(recencyDescription('At-Risk', 90)).toBe('Last order 45–90 days ago');
  });

  it('returns empty string for an unknown band', () => {
    expect(recencyDescription('Mystery', 40)).toBe('');
  });
});
