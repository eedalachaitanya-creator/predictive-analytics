import { describe, it, expect } from 'vitest';
import { paginate } from './paginate';

/**
 * Client-side pagination helper for the Validation page's "View" modal
 * (column-detail rows). Must behave like the rest of the app's paginators:
 * 1-based pages, clamps out-of-range requests, and always reports at least
 * one page (so the footer reads "1–N of N" even for a short list).
 */
describe('paginate — column-detail modal pagination', () => {
  const items = Array.from({ length: 17 }, (_, i) => i + 1); // 1..17

  it('returns the first full page and correct total page count', () => {
    const r = paginate(items, 1, 10);
    expect(r.slice).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(r.totalPages).toBe(2);
    expect(r.page).toBe(1);
  });

  it('returns the partial last page', () => {
    const r = paginate(items, 2, 10);
    expect(r.slice).toEqual([11, 12, 13, 14, 15, 16, 17]);
    expect(r.page).toBe(2);
  });

  it('clamps a page beyond the range to the last page', () => {
    const r = paginate(items, 99, 10);
    expect(r.page).toBe(2);
    expect(r.slice).toEqual([11, 12, 13, 14, 15, 16, 17]);
  });

  it('clamps page 0 / negative to the first page', () => {
    expect(paginate(items, 0, 10).page).toBe(1);
    expect(paginate(items, -5, 10).page).toBe(1);
  });

  it('reports a single page (not zero) for an empty list', () => {
    const r = paginate([], 1, 10);
    expect(r.slice).toEqual([]);
    expect(r.totalPages).toBe(1);
    expect(r.page).toBe(1);
  });

  it('handles an exact multiple of the page size', () => {
    const twenty = Array.from({ length: 20 }, (_, i) => i + 1);
    expect(paginate(twenty, 2, 10).totalPages).toBe(2);
    expect(paginate(twenty, 2, 10).slice.length).toBe(10);
  });
});
