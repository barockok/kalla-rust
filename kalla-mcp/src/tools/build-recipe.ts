import { callClaude } from "../llm/client.js";
import { BUILD_RECIPE_SYSTEM } from "../llm/prompts.js";
import {
  BuildRecipeInputSchema,
  BuildRecipeOutputSchema,
  type BuildRecipeInput,
  type BuildRecipeOutput,
} from "../types/tool-io.js";

function buildUserMessage(input: BuildRecipeInput): string {
  let msg = `Pattern type: ${input.pattern_type}\n`;
  msg += `Source A: "${input.sources.alias_a}" — Primary keys: [${input.primary_keys.source_a.join(", ")}]\n`;
  msg += `Source B: "${input.sources.alias_b}" — Primary keys: [${input.primary_keys.source_b.join(", ")}]\n\n`;
  msg += "Accepted matching rules:\n";
  input.rules.forEach((r, i) => {
    msg += `\n${i + 1}. ${r.name}\n   SQL: ${r.sql}\n   Description: ${r.description}\n`;
  });
  msg += "\nAssemble these rules into a complete DataFusion SQL query.";
  return msg;
}

export const buildRecipe = {
  name: "build_recipe" as const,
  description:
    "Assemble accepted matching rules into a complete DataFusion SQL reconciliation query.",
  inputSchema: {
    type: "object" as const,
    properties: {
      rules: {
        type: "array",
        description: "Accepted matching rules",
        items: {
          type: "object",
          properties: {
            name: { type: "string" },
            sql: { type: "string" },
            description: { type: "string" },
          },
          required: ["name", "sql", "description"],
        },
      },
      sources: {
        type: "object",
        properties: {
          alias_a: { type: "string" },
          alias_b: { type: "string" },
        },
        required: ["alias_a", "alias_b"],
      },
      primary_keys: {
        type: "object",
        properties: {
          source_a: { type: "array", items: { type: "string" } },
          source_b: { type: "array", items: { type: "string" } },
        },
        required: ["source_a", "source_b"],
      },
      pattern_type: { type: "string", enum: ["1:1", "1:N", "N:M"] },
    },
    required: ["rules", "sources", "primary_keys", "pattern_type"],
  },
  handler: async (input: BuildRecipeInput): Promise<BuildRecipeOutput> => {
    const parsed = BuildRecipeInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(BUILD_RECIPE_SYSTEM, userMessage, BuildRecipeOutputSchema);
  },
};
