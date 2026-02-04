import { detectSourceAliases } from '@/lib/intent-detection';

describe('detectSourceAliases', () => {
  const availableSources = [
    { alias: 'invoices' },
    { alias: 'payments' },
    { alias: 'invoices_csv' },
    { alias: 'payments_csv' },
  ];

  it('detects CSV aliases when user says "invoices csv and payments csv"', () => {
    const result = detectSourceAliases('reconcile the invoices csv and payments csv', availableSources);
    expect(result.left).toBe('invoices_csv');
    expect(result.right).toBe('payments_csv');
  });

  it('detects DB aliases when user says "invoices and payments"', () => {
    const result = detectSourceAliases('reconcile invoices and payments', availableSources);
    expect(result.left).toBe('invoices');
    expect(result.right).toBe('payments');
  });

  it('returns null when no match', () => {
    const result = detectSourceAliases('hello world', availableSources);
    expect(result.left).toBeNull();
    expect(result.right).toBeNull();
  });

  it('detects "payment csv" (singular) as "payments_csv"', () => {
    const result = detectSourceAliases('reconcile the invoices csv and payment csv', availableSources);
    expect(result.left).toBe('invoices_csv');
    expect(result.right).toBe('payments_csv');
  });
});
