import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../llm/client.js", () => ({
  callClaude: vi.fn(),
  parseJsonResponse: vi.fn(),
}));

import { buildRecipe } from "../build-recipe.js";
import { callClaude } from "../../llm/client.js";
import type { BuildRecipeInput } from "../../types/tool-io.js";

const mockCallClaude = vi.mocked(callClaude);

describe("build_recipe", () => {
  beforeEach(() => vi.clearAllMocks());

  const baseInput: BuildRecipeInput = {
    rules: [
      { name: "Amount Sum Match", sql: "ABS(l.amount - SUM(r.total_amount)) <= 0.01", description: "Sum match" },
    ],
    sources: { alias_a: "bank_statement", alias_b: "invoice_system" },
    primary_keys: { source_a: ["transaction_id"], source_b: ["invoice_id"] },
    pattern_type: "1:N",
  };

  it("should call Claude and return match SQL", async () => {
    mockCallClaude.mockResolvedValueOnce({
      match_sql: "SELECT l.transaction_id, r.invoice_id FROM bank_statement l JOIN invoice_system r ON ...",
      explanation: "Joins bank transactions to invoices with amount sum matching",
    });

    const result = await buildRecipe.handler(baseInput);

    expect(result.match_sql).toContain("SELECT");
    expect(result.explanation).toBeTruthy();
    expect(mockCallClaude).toHaveBeenCalledOnce();
  });

  it("should have correct tool metadata", () => {
    expect(buildRecipe.name).toBe("build_recipe");
  });
});
