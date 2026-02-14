#!/usr/bin/env npx ts-node
/**
 * Test Data Generator for Kalla Reconciliation Engine
 *
 * Generates parameterized CSV datasets with known match rates for integration testing.
 *
 * Usage:
 *   npx ts-node scripts/generate-test-data.ts [options]
 *
 * Options:
 *   --rows <n>        Number of left-side rows (default: 100)
 *   --match-rate <r>  Fraction of rows that match exactly (default: 0.6)
 *   --tolerance-rate <r>  Fraction matched via tolerance (default: 0.15)
 *   --duplicate-rate <r>  Fraction with duplicate keys on right (default: 0.05)
 *   --outdir <dir>    Output directory (default: testdata/generated)
 */

import * as fs from 'fs';
import * as path from 'path';

// --- CLI argument parsing ---

function parseArgs(): {
  rows: number;
  matchRate: number;
  toleranceRate: number;
  duplicateRate: number;
  outdir: string;
} {
  const args = process.argv.slice(2);
  const opts = {
    rows: 100,
    matchRate: 0.6,
    toleranceRate: 0.15,
    duplicateRate: 0.05,
    outdir: 'testdata/generated',
  };

  for (let i = 0; i < args.length; i += 2) {
    const flag = args[i];
    const value = args[i + 1];
    switch (flag) {
      case '--rows':
        opts.rows = parseInt(value, 10);
        break;
      case '--match-rate':
        opts.matchRate = parseFloat(value);
        break;
      case '--tolerance-rate':
        opts.toleranceRate = parseFloat(value);
        break;
      case '--duplicate-rate':
        opts.duplicateRate = parseFloat(value);
        break;
      case '--outdir':
        opts.outdir = value;
        break;
    }
  }

  return opts;
}

// --- Data generation ---

function padId(prefix: string, n: number): string {
  return `${prefix}-${String(n).padStart(6, '0')}`;
}

function randomAmount(): number {
  return Math.round((Math.random() * 50000 + 100) * 100) / 100;
}

function randomDate(year: number): string {
  const month = Math.floor(Math.random() * 12) + 1;
  const day = Math.floor(Math.random() * 28) + 1;
  return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

const CUSTOMER_NAMES = [
  'Acme Corp', 'TechStart Inc', 'Global Traders', 'Innovate Solutions',
  'DataFlow Systems', 'CloudNine Hosting', 'Enterprise Holdings',
  'StartupXYZ', 'MegaCorp Industries', 'SmallBiz Co',
  'FutureTech Labs', 'Euro Partners', 'British Solutions',
  'QuickPay Corp', 'Alpha Dynamics', 'Beta Industries',
  'Gamma Solutions', 'Delta Services', 'Epsilon Tech', 'Zeta Corp',
];

const PAYMENT_METHODS = ['wire_transfer', 'ach', 'credit_card', 'check'];
const CURRENCIES = ['USD', 'USD', 'USD', 'USD', 'EUR', 'GBP']; // weighted toward USD

interface InvoiceRow {
  invoice_id: string;
  customer_id: string;
  customer_name: string;
  invoice_date: string;
  due_date: string;
  amount: number;
  currency: string;
  status: string;
  description: string;
}

interface PaymentRow {
  payment_id: string;
  payer_id: string;
  payer_name: string;
  payment_date: string;
  paid_amount: number;
  currency: string;
  payment_method: string;
  reference_number: string;
  bank_reference: string;
  notes: string;
}

function generate(opts: ReturnType<typeof parseArgs>): {
  invoices: InvoiceRow[];
  payments: PaymentRow[];
  stats: Record<string, number>;
} {
  const { rows, matchRate, toleranceRate, duplicateRate } = opts;

  const exactCount = Math.floor(rows * matchRate);
  const toleranceCount = Math.floor(rows * toleranceRate);
  const duplicateCount = Math.floor(rows * duplicateRate);
  const orphanLeftCount = rows - exactCount - toleranceCount - duplicateCount;
  const orphanRightCount = Math.floor(rows * 0.1); // extra right-side orphans

  const invoices: InvoiceRow[] = [];
  const payments: PaymentRow[] = [];

  let invoiceIdx = 1;
  let paymentIdx = 1;

  // --- Exact matches ---
  for (let i = 0; i < exactCount; i++) {
    const custId = padId('CUST', (i % 20) + 1);
    const custName = CUSTOMER_NAMES[i % CUSTOMER_NAMES.length];
    const amount = randomAmount();
    const currency = CURRENCIES[i % CURRENCIES.length];
    const invId = padId('INV', invoiceIdx++);
    const payId = padId('PAY', paymentIdx++);

    invoices.push({
      invoice_id: invId,
      customer_id: custId,
      customer_name: custName,
      invoice_date: randomDate(2024),
      due_date: randomDate(2024),
      amount,
      currency,
      status: 'pending',
      description: `Service ${i + 1}`,
    });

    payments.push({
      payment_id: payId,
      payer_id: custId,
      payer_name: custName,
      payment_date: randomDate(2024),
      paid_amount: amount,
      currency,
      payment_method: PAYMENT_METHODS[i % PAYMENT_METHODS.length],
      reference_number: invId,
      bank_reference: `BR-${payId}`,
      notes: `Exact match for ${invId}`,
    });
  }

  // --- Tolerance matches (amount differs by up to 2%) ---
  for (let i = 0; i < toleranceCount; i++) {
    const custId = padId('CUST', (i % 20) + 1);
    const custName = CUSTOMER_NAMES[i % CUSTOMER_NAMES.length];
    const amount = randomAmount();
    const currency = CURRENCIES[i % CURRENCIES.length];
    const invId = padId('INV', invoiceIdx++);
    const payId = padId('PAY', paymentIdx++);

    // Vary amount within 2%
    const variance = 1 + (Math.random() * 0.04 - 0.02); // 0.98 to 1.02
    const paidAmount = Math.round(amount * variance * 100) / 100;

    invoices.push({
      invoice_id: invId,
      customer_id: custId,
      customer_name: custName,
      invoice_date: randomDate(2024),
      due_date: randomDate(2024),
      amount,
      currency,
      status: 'pending',
      description: `Tolerance service ${i + 1}`,
    });

    payments.push({
      payment_id: payId,
      payer_id: custId,
      payer_name: custName,
      payment_date: randomDate(2024),
      paid_amount: paidAmount,
      currency,
      payment_method: PAYMENT_METHODS[i % PAYMENT_METHODS.length],
      reference_number: invId,
      bank_reference: `BR-${payId}`,
      notes: `Tolerance match for ${invId}`,
    });
  }

  // --- Duplicate keys on right side ---
  for (let i = 0; i < duplicateCount; i++) {
    const custId = padId('CUST', (i % 20) + 1);
    const custName = CUSTOMER_NAMES[i % CUSTOMER_NAMES.length];
    const amount = randomAmount();
    const currency = 'USD';
    const invId = padId('INV', invoiceIdx++);

    invoices.push({
      invoice_id: invId,
      customer_id: custId,
      customer_name: custName,
      invoice_date: randomDate(2024),
      due_date: randomDate(2024),
      amount,
      currency,
      status: 'pending',
      description: `Duplicate test ${i + 1}`,
    });

    // Two payments for same invoice (split)
    const halfAmount = Math.round((amount / 2) * 100) / 100;
    for (let j = 0; j < 2; j++) {
      const payId = padId('PAY', paymentIdx++);
      payments.push({
        payment_id: payId,
        payer_id: custId,
        payer_name: custName,
        payment_date: randomDate(2024),
        paid_amount: halfAmount,
        currency,
        payment_method: 'wire_transfer',
        reference_number: `${invId}-PART${j + 1}`,
        bank_reference: `BR-${payId}`,
        notes: `Split payment ${j + 1} for ${invId}`,
      });
    }
  }

  // --- Left-side orphans (invoices with no payment) ---
  for (let i = 0; i < orphanLeftCount; i++) {
    const custId = padId('CUST', 100 + i);
    const custName = `Orphan Customer ${i + 1}`;
    const invId = padId('INV', invoiceIdx++);

    invoices.push({
      invoice_id: invId,
      customer_id: custId,
      customer_name: custName,
      invoice_date: randomDate(2024),
      due_date: randomDate(2024),
      amount: randomAmount(),
      currency: 'USD',
      status: 'pending',
      description: `Orphan invoice ${i + 1}`,
    });
  }

  // --- Right-side orphans (payments with no invoice) ---
  for (let i = 0; i < orphanRightCount; i++) {
    const custId = padId('CUST', 200 + i);
    const payId = padId('PAY', paymentIdx++);

    payments.push({
      payment_id: payId,
      payer_id: custId,
      payer_name: `Unknown Payer ${i + 1}`,
      payment_date: randomDate(2024),
      paid_amount: randomAmount(),
      currency: 'USD',
      payment_method: PAYMENT_METHODS[i % PAYMENT_METHODS.length],
      reference_number: `UNKNOWN-${i + 1}`,
      bank_reference: `BR-${payId}`,
      notes: `Orphan payment ${i + 1}`,
    });
  }

  return {
    invoices,
    payments,
    stats: {
      total_invoices: invoices.length,
      total_payments: payments.length,
      exact_matches: exactCount,
      tolerance_matches: toleranceCount,
      duplicates: duplicateCount,
      orphan_left: orphanLeftCount,
      orphan_right: orphanRightCount,
    },
  };
}

// --- CSV writing ---

function toCsv(rows: Record<string, string | number>[]): string {
  if (rows.length === 0) return '';
  const headers = Object.keys(rows[0]);
  const lines = [headers.join(',')];
  for (const row of rows) {
    const values = headers.map((h) => {
      const v = row[h];
      const s = String(v ?? '');
      return s.includes(',') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
    });
    lines.push(values.join(','));
  }
  return lines.join('\n') + '\n';
}

// --- Main ---

function main() {
  const opts = parseArgs();
  const { invoices, payments, stats } = generate(opts);

  // Ensure output directory
  const outdir = path.resolve(opts.outdir);
  fs.mkdirSync(outdir, { recursive: true });

  // Write CSVs
  const invoicesPath = path.join(outdir, 'invoices.csv');
  const paymentsPath = path.join(outdir, 'payments.csv');
  fs.writeFileSync(invoicesPath, toCsv(invoices as unknown as Record<string, string | number>[]));
  fs.writeFileSync(paymentsPath, toCsv(payments as unknown as Record<string, string | number>[]));

  // Write stats for test assertions
  const statsPath = path.join(outdir, 'stats.json');
  fs.writeFileSync(statsPath, JSON.stringify(stats, null, 2) + '\n');

  console.log('Generated test data:');
  console.log(`  Invoices: ${invoicesPath} (${invoices.length} rows)`);
  console.log(`  Payments: ${paymentsPath} (${payments.length} rows)`);
  console.log(`  Stats:    ${statsPath}`);
  console.log('');
  console.log('Distribution:');
  for (const [key, value] of Object.entries(stats)) {
    console.log(`  ${key}: ${value}`);
  }
}

main();
