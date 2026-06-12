import { humanizeColumnName } from './validation';

/**
 * QA: the Validation page's "Column Detail" grid (and the missing-column warning)
 * showed raw snake_case DB column names — e.g. `customer_name`, `order_value_usd`.
 * humanizeColumnName turns them into readable Title Case, matching the clients
 * Data-Overview and upload-preview grids (so no table in the app shows raw
 * snake_case). Acronyms like ID / USD stay upper-case.
 */
describe('humanizeColumnName — validation column labels', () => {
  it('title-cases a snake_case column', () => {
    expect(humanizeColumnName('customer_name')).toBe('Customer Name');
  });

  it('keeps known acronyms upper-case', () => {
    expect(humanizeColumnName('customer_id')).toBe('Customer ID');
    expect(humanizeColumnName('order_value_usd')).toBe('Order Value USD');
    expect(humanizeColumnName('sku')).toBe('SKU');
  });

  it('handles single words and empties', () => {
    expect(humanizeColumnName('rating')).toBe('Rating');
    expect(humanizeColumnName('')).toBe('');
  });
});
