import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { nlToSql } from "../nl-to-sql.js";
import { callClaude } from "../../llm/client.js";
import type { NlToSqlInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("nl_to_sql", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: NlToSqlInput = {
    text: "Invoice date must be within 7 days of bank transaction date",
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_date", data_type: "date" },
        { name: "amount", data_type: "decimal" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_date", data_type: "date" },
        { name: "total_amount", data_type: "decimal" },
      ],
    },
    mappings: [
      { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "Both date columns" },
    ],
  };

  it("should convert NL to DataFusion SQL expression", async () => {
    mockCallClaude.mockResolvedValueOnce({
      name: "Date Range Match",
      sql: "r.invoice_date BETWEEN l.transaction_date - INTERVAL '7 days' AND l.transaction_date + INTERVAL '7 days'",
      description: "Invoice date within 7 days of transaction",
      confidence: 0.88,
    });

    const result = await nlToSql.handler(baseInput);

    expect(result.sql).toContain("INTERVAL");
    expect(result.name).toBe("Date Range Match");
    expect(result.confidence).toBeGreaterThan(0);
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include mappings in prompt context", async () => {
    mockCallClaude.mockResolvedValueOnce({
      name: "Test", sql: "l.a = r.b", description: "test", confidence: 0.5,
    });

    await nlToSql.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("transaction_date → invoice_date");
  });

  it("should have correct tool metadata", () => {
    expect(nlToSql.name).toBe("nl_to_sql");
  });
});
