import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { previewMatch } from "../preview-match.js";
import { callClaude } from "../../llm/client.js";
import type { PreviewMatchInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("preview_match", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: PreviewMatchInput = {
    match_sql: "SELECT l.*, r.* FROM bank l JOIN invoices r ON l.amount = r.total",
    sample_a: [
      { transaction_id: "TXN-001", amount: 1500.0, date: "2026-01-15" },
      { transaction_id: "TXN-002", amount: 200.0, date: "2026-01-16" },
    ],
    sample_b: [
      { invoice_id: "INV-1001", total: 1500.0, inv_date: "2026-01-14" },
      { invoice_id: "INV-1002", total: 750.0, inv_date: "2026-01-15" },
    ],
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_id", data_type: "varchar" },
        { name: "amount", data_type: "decimal" },
        { name: "date", data_type: "date" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_id", data_type: "varchar" },
        { name: "total", data_type: "decimal" },
        { name: "inv_date", data_type: "date" },
      ],
    },
    primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
    rules: [
      { name: "Amount Match", sql: "ABS(l.amount - r.total) <= 0.01", description: "Exact amount match" },
    ],
  };

  it("should call Claude and return matches with summary", async () => {
    mockCallClaude.mockResolvedValueOnce({
      matches: [
        {
          left_row: { transaction_id: "TXN-001", amount: 1500.0 },
          right_rows: [{ invoice_id: "INV-1001", total: 1500.0 }],
          status: "matched",
        },
        {
          left_row: { transaction_id: "TXN-002", amount: 200.0 },
          right_rows: [],
          status: "unmatched",
        },
      ],
      summary: { total_left: 2, total_right: 2, matched: 1, unmatched: 1 },
    });

    const result = await previewMatch.handler(baseInput);

    expect(result.matches).toHaveLength(2);
    expect(result.matches[0].status).toBe("matched");
    expect(result.matches[1].status).toBe("unmatched");
    expect(result.summary.matched).toBe(1);
    expect(result.summary.unmatched).toBe(1);
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include match SQL and rules in prompt", async () => {
    mockCallClaude.mockResolvedValueOnce({
      matches: [],
      summary: { total_left: 2, total_right: 2, matched: 0, unmatched: 2 },
    });

    await previewMatch.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("SELECT l.*, r.*");
    expect(userMessage).toContain("Amount Match");
    expect(userMessage).toContain("TXN-001");
  });

  it("should have correct tool metadata", () => {
    expect(previewMatch.name).toBe("preview_match");
    expect(previewMatch.description).toBeTruthy();
  });
});
