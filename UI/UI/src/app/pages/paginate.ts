/**
 * Pure, framework-free client-side pagination helper.
 *
 * Used by the Validation page's column-detail modal (and safe to reuse
 * anywhere a small in-memory list needs the same paging UX as the rest of
 * the app). 1-based pages; out-of-range requests are clamped; an empty list
 * still reports one page so the footer reads "1–0 of 0" rather than "page 1
 * of 0".
 */
export interface Page<T> {
  /** The items on the resolved page. */
  slice: T[];
  /** Total number of pages (always >= 1). */
  totalPages: number;
  /** The resolved (clamped, 1-based) page number. */
  page: number;
}

export function paginate<T>(items: T[], page: number, pageSize: number): Page<T> {
  const list = items ?? [];
  const size = pageSize > 0 ? pageSize : 1;
  const totalPages = Math.max(1, Math.ceil(list.length / size));
  const requested = Math.floor(page) || 1;
  const clamped = Math.min(Math.max(1, requested), totalPages);
  const start = (clamped - 1) * size;
  return { slice: list.slice(start, start + size), totalPages, page: clamped };
}
