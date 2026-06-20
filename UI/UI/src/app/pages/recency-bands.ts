/**
 * Short, user-friendly captions for the dashboard's Purchase-Recency bands.
 *
 * Keyed by the BACKEND label ('Active' | 'At-Risk' | 'Churned'); the bands are
 * a pure order-recency split of the churn window:
 *   Recently Purchased  → ordered within the recent half-window
 *   Slowing Down        → ordered in the older half of the window
 *   Lapsed              → no order for the whole window (== the Lapsed card)
 *
 * Framework-free so it can be unit-tested without the Angular linker.
 */
export function recencyDescription(backendLabel: string, windowDays: number): string {
  const half = Math.round(windowDays / 2);
  switch (backendLabel) {
    case 'Active':  return `Ordered in the last ${half} days`;
    case 'At-Risk': return `Last order ${half}–${windowDays} days ago`;
    case 'Churned': return `No order in ${windowDays}+ days`;
    default:        return '';
  }
}
