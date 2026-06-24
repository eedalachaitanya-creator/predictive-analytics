import { toSkippedCsv } from './upload';

/**
 * The skipped-records popup gets an "Export CSV" button so a client can
 * download the report of tickets that weren't synced. toSkippedCsv turns the
 * skipped rows into a CSV; these tests pin the header, rows, and CSV escaping,
 * mirroring the toChurnCsv contract.
 */
describe('toSkippedCsv — skipped-records CSV export', () => {
  const rec = (over: Record<string, unknown> = {}) => ({
    record: 'KAN-5',
    customerRef: 'cust-002',
    reason: "No matching customer for 'cust-002'",
    ...over,
  });

  it('emits a header row plus one row per skipped record', () => {
    const lines = toSkippedCsv([rec(), rec({ record: 'KAN-6' })] as never).split('\n');
    expect(lines.length).toBe(3);
    expect(lines[0]).toContain('Record');
    expect(lines[0]).toContain('Customer Ref');
    expect(lines[0]).toContain('Reason');
    expect(lines[1]).toContain('KAN-5');
    expect(lines[1]).toContain('cust-002');
  });

  it('escapes commas and quotes per CSV rules', () => {
    const csv = toSkippedCsv([rec({ reason: 'No match, see "CUST-002"' })] as never);
    expect(csv).toContain('"No match, see ""CUST-002"""');
  });

  it('handles an empty list (header only)', () => {
    expect(toSkippedCsv([] as never).split('\n').length).toBe(1);
  });
});
