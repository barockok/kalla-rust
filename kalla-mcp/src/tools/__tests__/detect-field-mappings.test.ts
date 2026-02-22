import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { detectFieldMappings } from "../detect-field-mappings.js";
import { callClaude } from "../../llm/client.js";
import type { DetectFieldMappingsInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("detect_field_mappings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const baseInput: DetectFieldMappingsInput = {
    schema_a: {
      alias: "bank_statement",
      columns: [
        { name: "transaction_id", data_type: "varchar" },
        { name: "transaction_date", data_type: "date" },
        { name: "amount", data_type: "decimal" },
        { name: "description", data_type: "varchar" },
      ],
    },
    schema_b: {
      alias: "invoice_system",
      columns: [
        { name: "invoice_id", data_type: "varchar" },
        { name: "invoice_date", data_type: "date" },
        { name: "total_amount", data_type: "decimal" },
        { name: "vendor_name", data_type: "varchar" },
      ],
    },
  };

  it("should call Claude and return parsed mappings", async () => {
    mockCallClaude.mockResolvedValueOnce({
      mappings: [
        { field_a: "transaction_date", field_b: "invoice_date", confidence: 0.92, reason: "Both date columns" },
        { field_a: "amount", field_b: "total_amount", confidence: 0.87, reason: "Both numeric amount columns" },
      ],
      suggested_filters: [
        { type: "date_range", field_a: "transaction_date", field_b: "invoice_date" },
        { type: "amount_range", field_a: "amount", field_b: "total_amount" },
      ],
    });

    const result = await detectFieldMappings.handler(baseInput);

    expect(result.mappings).toHaveLength(2);
    expect(result.mappings[0].field_a).toBe("transaction_date");
    expect(result.mappings[0].field_b).toBe("invoice_date");
    expect(result.suggested_filters).toHaveLength(2);
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should include sample rows in prompt when provided", async () => {
    mockCallClaude.mockResolvedValueOnce({
      mappings: [],
      suggested_filters: [],
    });

    const inputWithSamples = {
      ...baseInput,
      sample_a: [{ transaction_date: "2026-01-15", amount: 1500.00 }],
      sample_b: [{ invoice_date: "2026-01-14", total_amount: 1500.00 }],
    };

    await detectFieldMappings.handler(inputWithSamples);

    const callArgs = mockCallClaude.mock.calls[0];
    const userMessage = callArgs[1];
    expect(userMessage).toContain("Sample rows");
    expect(userMessage).toContain("2026-01-15");
  });

  it("should have correct tool metadata", () => {
    expect(detectFieldMappings.name).toBe("detect_field_mappings");
    expect(detectFieldMappings.description).toBeTruthy();
    expect(detectFieldMappings.inputSchema).toBeDefined();
  });
});
