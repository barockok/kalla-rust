import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { inferRules } from "../infer-rules.js";
import { callClaude } from "../../llm/client.js";
import type { InferRulesInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("infer_rules", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: InferRulesInput = {
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_id", data_type: "varchar" },
        { name: "transaction_date", data_type: "date" },
        { name: "amount", data_type: "decimal" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_id", data_type: "varchar" },
        { name: "invoice_date", data_type: "date" },
        { name: "total_amount", data_type: "decimal" },
      ],
    },
    sample_a: [
      { transaction_id: "TXN-001", transaction_date: "2026-01-15", amount: 1500.0 },
    ],
    sample_b: [
      { invoice_id: "INV-1001", invoice_date: "2026-01-14", total_amount: 1500.0 },
    ],
    mappings: [
      { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "Both date columns" },
      { field_a: "amount", field_b: "total_amount", confidence: 0.87, reason: "Both amount columns" },
    ],
  };

  it("should call Claude and return pattern, primary keys, and rules", async () => {
    mockCallClaude.mockResolvedValueOnce({
      pattern: { type: "1:N", description: "One bank txn matches multiple invoices", confidence: 0.88 },
      primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
      rules: [
        {
          name: "Amount Sum Match",
          sql: "ABS(l.amount - SUM(r.total_amount)) <= 0.01",
          description: "Sum of invoice amounts equals bank transaction",
          confidence: 0.91,
          evidence: [{ transaction_id: "TXN-001", amount: 1500, total_amount: 1500 }],
        },
      ],
    });

    const result = await inferRules.handler(baseInput);

    expect(result.pattern.type).toBe("1:N");
    expect(result.primary_keys.source_a).toEqual(["transaction_id"]);
    expect(result.rules).toHaveLength(1);
    expect(result.rules[0].sql).toContain("SUM");
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include sample data and mappings in prompt", async () => {
    mockCallClaude.mockResolvedValueOnce({
      pattern: { type: "1:1", description: "Direct match", confidence: 0.9 },
      primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
      rules: [],
    });

    await inferRules.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("TXN-001");
    expect(userMessage).toContain("transaction_date \u2192 invoice_date");
  });

  it("should have correct tool metadata", () => {
    expect(inferRules.name).toBe("infer_rules");
    expect(inferRules.description).toBeTruthy();
  });
});
