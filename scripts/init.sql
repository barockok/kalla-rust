-- Kalla Reconciliation Engine - Database Schema and Seed Data
-- PostgreSQL 16

-- ============================================
-- SCHEMA DEFINITIONS
-- ============================================

-- Data sources table
CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alias VARCHAR(255) UNIQUE NOT NULL,
    uri TEXT NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'connected',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Recipes table
CREATE TABLE IF NOT EXISTS recipes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipe_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    config JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Reconciliation runs
CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipe_id VARCHAR(255) REFERENCES recipes(recipe_id),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    left_source VARCHAR(500) NOT NULL,
    right_source VARCHAR(500) NOT NULL,
    left_record_count BIGINT DEFAULT 0,
    right_record_count BIGINT DEFAULT 0,
    matched_count BIGINT DEFAULT 0,
    unmatched_left_count BIGINT DEFAULT 0,
    unmatched_right_count BIGINT DEFAULT 0,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Matched records evidence
CREATE TABLE IF NOT EXISTS matched_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES runs(id) ON DELETE CASCADE,
    left_key VARCHAR(255) NOT NULL,
    right_key VARCHAR(255) NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    confidence DECIMAL(5,4) DEFAULT 1.0,
    matched_at TIMESTAMPTZ DEFAULT NOW()
);

-- Unmatched records evidence
CREATE TABLE IF NOT EXISTS unmatched_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES runs(id) ON DELETE CASCADE,
    source_side VARCHAR(10) NOT NULL CHECK (source_side IN ('left', 'right')),
    record_key VARCHAR(255) NOT NULL,
    attempted_rules TEXT[],
    closest_candidate VARCHAR(255),
    rejection_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_runs_recipe_id ON runs(recipe_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_matched_records_run_id ON matched_records(run_id);
CREATE INDEX IF NOT EXISTS idx_unmatched_records_run_id ON unmatched_records(run_id);

-- ============================================
-- SAMPLE DATA: INVOICES TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    invoice_date DATE NOT NULL,
    due_date DATE NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'pending',
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- SAMPLE DATA: PAYMENTS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(20) PRIMARY KEY,
    payer_id VARCHAR(20) NOT NULL,
    payer_name VARCHAR(255) NOT NULL,
    payment_date DATE NOT NULL,
    paid_amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    payment_method VARCHAR(50),
    reference_number VARCHAR(50),
    bank_reference VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- SEED DATA: INVOICES
-- ============================================

INSERT INTO invoices (invoice_id, customer_id, customer_name, invoice_date, due_date, amount, currency, status, description) VALUES
-- Exact matches (will match with payments)
('INV-2024-001', 'CUST-001', 'Acme Corporation', '2024-01-15', '2024-02-15', 15000.00, 'USD', 'pending', 'Software license Q1 2024'),
('INV-2024-002', 'CUST-002', 'TechStart Inc', '2024-01-18', '2024-02-18', 7500.50, 'USD', 'pending', 'Consulting services January'),
('INV-2024-003', 'CUST-003', 'Global Traders Ltd', '2024-01-20', '2024-02-20', 25000.00, 'USD', 'pending', 'Annual maintenance contract'),
('INV-2024-004', 'CUST-001', 'Acme Corporation', '2024-01-25', '2024-02-25', 3200.00, 'USD', 'pending', 'Additional user licenses'),
('INV-2024-005', 'CUST-004', 'Innovate Solutions', '2024-01-28', '2024-02-28', 12750.00, 'USD', 'pending', 'Custom development Phase 1'),

-- Tolerance matches (amounts slightly different)
('INV-2024-006', 'CUST-005', 'DataFlow Systems', '2024-02-01', '2024-03-01', 8500.00, 'USD', 'pending', 'Data integration services'),
('INV-2024-007', 'CUST-006', 'CloudNine Hosting', '2024-02-05', '2024-03-05', 4999.99, 'USD', 'pending', 'Cloud hosting February'),

-- Partial matches (1:N - one invoice, multiple payments)
('INV-2024-008', 'CUST-007', 'Enterprise Holdings', '2024-02-08', '2024-03-08', 50000.00, 'USD', 'pending', 'Enterprise license bundle'),

-- Unmatched invoices (no corresponding payment)
('INV-2024-009', 'CUST-008', 'StartupXYZ', '2024-02-10', '2024-03-10', 2500.00, 'USD', 'pending', 'Starter package'),
('INV-2024-010', 'CUST-009', 'MegaCorp Industries', '2024-02-12', '2024-03-12', 175000.00, 'USD', 'pending', 'Enterprise deployment'),
('INV-2024-011', 'CUST-010', 'SmallBiz Co', '2024-02-15', '2024-03-15', 850.00, 'USD', 'pending', 'Monthly subscription'),
('INV-2024-012', 'CUST-011', 'FutureTech Labs', '2024-02-18', '2024-03-18', 32000.00, 'USD', 'pending', 'R&D collaboration'),

-- Different currency
('INV-2024-013', 'CUST-012', 'Euro Partners GmbH', '2024-02-20', '2024-03-20', 10000.00, 'EUR', 'pending', 'European market expansion'),
('INV-2024-014', 'CUST-013', 'British Solutions Ltd', '2024-02-22', '2024-03-22', 8500.00, 'GBP', 'pending', 'UK market support'),

-- Already paid (for testing status)
('INV-2024-015', 'CUST-014', 'QuickPay Corp', '2024-01-05', '2024-02-05', 6000.00, 'USD', 'paid', 'Express service');

-- ============================================
-- SEED DATA: PAYMENTS
-- ============================================

INSERT INTO payments (payment_id, payer_id, payer_name, payment_date, paid_amount, currency, payment_method, reference_number, bank_reference, notes) VALUES
-- Exact matches
('PAY-2024-001', 'CUST-001', 'Acme Corporation', '2024-02-10', 15000.00, 'USD', 'wire_transfer', 'INV-2024-001', 'WT-78234523', 'Payment for INV-2024-001'),
('PAY-2024-002', 'CUST-002', 'TechStart Inc', '2024-02-15', 7500.50, 'USD', 'ach', 'INV-2024-002', 'ACH-92834756', 'Invoice payment'),
('PAY-2024-003', 'CUST-003', 'Global Traders Ltd', '2024-02-18', 25000.00, 'USD', 'wire_transfer', 'INV-2024-003', 'WT-82736455', 'Annual maintenance'),
('PAY-2024-004', 'CUST-001', 'Acme Corporation', '2024-02-20', 3200.00, 'USD', 'credit_card', 'INV-2024-004', 'CC-12983746', 'Additional licenses'),
('PAY-2024-005', 'CUST-004', 'Innovate Solutions', '2024-02-25', 12750.00, 'USD', 'wire_transfer', 'INV-2024-005', 'WT-98273645', 'Development Phase 1'),

-- Tolerance matches (amounts slightly different - bank fees deducted)
('PAY-2024-006', 'CUST-005', 'DataFlow Systems', '2024-02-28', 8485.00, 'USD', 'wire_transfer', 'INV-2024-006', 'WT-72634589', 'Wire fee deducted'),
('PAY-2024-007', 'CUST-006', 'CloudNine Hosting', '2024-03-01', 5000.00, 'USD', 'ach', 'INV-2024-007', 'ACH-83746529', 'Rounded payment'),

-- Split payments (multiple payments for INV-2024-008)
('PAY-2024-008A', 'CUST-007', 'Enterprise Holdings', '2024-02-25', 25000.00, 'USD', 'wire_transfer', 'INV-2024-008-PART1', 'WT-19283746', 'Partial payment 1 of 2'),
('PAY-2024-008B', 'CUST-007', 'Enterprise Holdings', '2024-03-05', 25000.00, 'USD', 'wire_transfer', 'INV-2024-008-PART2', 'WT-28374659', 'Partial payment 2 of 2'),

-- Unmatched payments (no corresponding invoice - overpayments, advances)
('PAY-2024-009', 'CUST-015', 'NewClient LLC', '2024-02-28', 5000.00, 'USD', 'wire_transfer', 'ADVANCE-001', 'WT-38475962', 'Advance payment for future work'),
('PAY-2024-010', 'CUST-016', 'Mystery Payer Inc', '2024-03-01', 1234.56, 'USD', 'ach', NULL, 'ACH-47586930', 'Unknown reference'),
('PAY-2024-011', 'CUST-017', 'Duplicate Payment Co', '2024-03-02', 7500.50, 'USD', 'wire_transfer', 'INV-2024-002-DUP', 'WT-58697041', 'Possible duplicate'),
('PAY-2024-012', 'CUST-018', 'International Buyer', '2024-03-03', 15000.00, 'EUR', 'wire_transfer', 'INT-ORDER-001', 'WT-INTL-001', 'International payment'),

-- Different name variations (fuzzy matching test)
('PAY-2024-013', 'CUST-001', 'ACME Corp.', '2024-03-05', 1500.00, 'USD', 'check', 'MISC-001', 'CHK-001234', 'Miscellaneous payment'),
('PAY-2024-014', 'CUST-014', 'Quick Pay Corporation', '2024-02-01', 6000.00, 'USD', 'ach', 'INV-2024-015', 'ACH-99887766', 'Early payment');

-- ============================================
-- SEED DATA: DATA SOURCES
-- ============================================

INSERT INTO sources (alias, uri, source_type, status) VALUES
('invoices', 'postgres://kalla:kalla_secret@postgres:5432/kalla?table=invoices', 'postgres', 'connected'),
('payments', 'postgres://kalla:kalla_secret@postgres:5432/kalla?table=payments', 'postgres', 'connected'),
('invoices_csv', 'file:///app/testdata/invoices.csv', 'csv', 'connected'),
('payments_csv', 'file:///app/testdata/payments.csv', 'csv', 'connected');

-- ============================================
-- SEED DATA: SAMPLE RECIPE
-- ============================================

INSERT INTO recipes (recipe_id, name, description, config) VALUES
('invoice-payment-match', 'Invoice to Payment Reconciliation', 'Match invoices with incoming payments based on reference numbers and amounts', '{
  "version": "1.0",
  "recipe_id": "invoice-payment-match",
  "sources": {
    "left": {
      "alias": "invoices",
      "uri": "postgres://kalla:kalla_secret@postgres:5432/kalla?table=invoices",
      "primary_key": ["invoice_id"]
    },
    "right": {
      "alias": "payments",
      "uri": "postgres://kalla:kalla_secret@postgres:5432/kalla?table=payments",
      "primary_key": ["payment_id"]
    }
  },
  "match_rules": [
    {
      "name": "exact_reference_match",
      "pattern": "1:1",
      "conditions": [
        {"left": "invoice_id", "op": "eq", "right": "reference_number"}
      ],
      "priority": 1
    },
    {
      "name": "amount_and_customer_match",
      "pattern": "1:1",
      "conditions": [
        {"left": "customer_id", "op": "eq", "right": "payer_id"},
        {"left": "amount", "op": "tolerance", "right": "paid_amount", "threshold": 0.02}
      ],
      "priority": 2
    },
    {
      "name": "split_payment_match",
      "pattern": "1:N",
      "conditions": [
        {"left": "customer_id", "op": "eq", "right": "payer_id"},
        {"left": "invoice_id", "op": "startswith", "right": "reference_number"}
      ],
      "priority": 3
    }
  ],
  "output": {
    "matched": "evidence/matched.parquet",
    "unmatched_left": "evidence/unmatched_invoices.parquet",
    "unmatched_right": "evidence/unmatched_payments.parquet"
  }
}'::jsonb);

-- ============================================
-- VIEWS FOR REPORTING
-- ============================================

-- Summary view for reconciliation status
CREATE OR REPLACE VIEW reconciliation_summary AS
SELECT
    r.id as run_id,
    r.recipe_id,
    rec.name as recipe_name,
    r.status,
    r.left_record_count,
    r.right_record_count,
    r.matched_count,
    r.unmatched_left_count,
    r.unmatched_right_count,
    CASE
        WHEN r.left_record_count > 0
        THEN ROUND((r.matched_count::numeric / r.left_record_count * 100), 2)
        ELSE 0
    END as match_rate_pct,
    r.started_at,
    r.completed_at,
    EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) as duration_seconds
FROM runs r
LEFT JOIN recipes rec ON r.recipe_id = rec.recipe_id;

-- Invoice aging report
CREATE OR REPLACE VIEW invoice_aging AS
SELECT
    invoice_id,
    customer_name,
    amount,
    invoice_date,
    due_date,
    CURRENT_DATE - due_date as days_overdue,
    CASE
        WHEN CURRENT_DATE <= due_date THEN 'current'
        WHEN CURRENT_DATE - due_date <= 30 THEN '1-30 days'
        WHEN CURRENT_DATE - due_date <= 60 THEN '31-60 days'
        WHEN CURRENT_DATE - due_date <= 90 THEN '61-90 days'
        ELSE '90+ days'
    END as aging_bucket,
    status
FROM invoices
WHERE status != 'paid';

-- Payment summary by method
CREATE OR REPLACE VIEW payment_summary AS
SELECT
    payment_method,
    currency,
    COUNT(*) as payment_count,
    SUM(paid_amount) as total_amount,
    AVG(paid_amount) as avg_amount,
    MIN(payment_date) as first_payment,
    MAX(payment_date) as last_payment
FROM payments
GROUP BY payment_method, currency
ORDER BY total_amount DESC;

-- ============================================
-- GRANT PERMISSIONS
-- ============================================

-- Grant usage on all tables to the kalla user (if using different roles)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kalla;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kalla;

COMMIT;
