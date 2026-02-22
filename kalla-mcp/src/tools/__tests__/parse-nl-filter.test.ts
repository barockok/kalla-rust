import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { parseNlFilter } from "../parse-nl-filter.js";
import { callClaude } from "../../llm/client.js";
import type { ParseNlFilterInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("parse_nl_filter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const baseInput: ParseNlFilterInput = {
    text: "Only transactions above $500 from last month",
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
    current_mappings: [
      { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "date columns" },
      { field_a: "amount", field_b: "total_amount", confidence: 0.87, reason: "amount columns" },
    ],
  };

  it("should call Claude and return parsed filter conditions", async () => {
    mockCallClaude.mockResolvedValueOnce({
      filters: [
        { source: "bank_statement", column: "amount", op: "gt", value: 500 },
        { source: "invoice_system", column: "total_amount", op: "gt", value: 500 },
      ],
      explanation: "Filtering both sources for amounts > $500",
    });

    const result = await parseNlFilter.handler(baseInput);

    expect(result.filters).toHaveLength(2);
    expect(result.filters[0].source).toBe("bank_statement");
    expect(result.filters[0].column).toBe("amount");
    expect(result.explanation).toBeTruthy();
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include current mappings in the prompt", async () => {
    mockCallClaude.mockResolvedValueOnce({
      filters: [],
      explanation: "No filters parsed",
    });

    await parseNlFilter.handler(baseInput);

    const userMessage = mockCallClaude.mock.calls[0][1];
    expect(userMessage).toContain("transaction_date");
    expect(userMessage).toContain("invoice_date");
    expect(userMessage).toContain("field mappings");
  });

  it("should have correct tool metadata", () => {
    expect(parseNlFilter.name).toBe("parse_nl_filter");
    expect(parseNlFilter.inputSchema).toBeDefined();
  });
});
