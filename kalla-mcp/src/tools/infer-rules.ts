import { callClaude } from "../llm/client.js";
import { INFER_RULES_SYSTEM } from "../llm/prompts.js";
import {
  InferRulesInputSchema,
  InferRulesOutputSchema,
  type InferRulesInput,
  type InferRulesOutput,
} from "../types/tool-io.js";

function formatSampleRows(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "(empty)";
  const keys = Object.keys(rows[0]);
  const header = keys.join(" | ");
  const body = rows
    .slice(0, 10)
    .map((r) => keys.map((k) => String(r[k] ?? "null")).join(" | "))
    .join("\n");
  return `${header}\n${body}`;
}

function buildUserMessage(input: InferRulesInput): string {
  let msg = `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\n\nSample rows (Source A):\n${formatSampleRows(input.sample_a)}`;

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\n\nSample rows (Source B):\n${formatSampleRows(input.sample_b)}`;

  if (input.mappings.length > 0) {
    msg += "\n\nKnown field mappings (A \u2192 B):\n";
    msg += input.mappings
      .map((m) => `  - ${m.field_a} \u2192 ${m.field_b} (confidence: ${m.confidence})`)
      .join("\n");
  }

  msg += "\n\nAnalyze the data and identify: matching pattern, primary keys, and matching rules with DataFusion SQL.";
  return msg;
}

export const inferRules = {
  name: "infer_rules" as const,
  description:
    "Analyze sample data from both sources to detect the matching pattern (1:1, 1:N, N:M), identify primary keys, and generate DataFusion SQL matching rules with evidence.",
  inputSchema: {
    type: "object" as const,
    properties: {
      schema_a: {
        type: "object",
        description: "Left source schema",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: {
                name: { type: "string" },
                data_type: { type: "string" },
              },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      schema_b: {
        type: "object",
        description: "Right source schema",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: {
                name: { type: "string" },
                data_type: { type: "string" },
              },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      sample_a: {
        type: "array",
        description: "Sample rows from source A",
        items: { type: "object" },
      },
      sample_b: {
        type: "array",
        description: "Sample rows from source B",
        items: { type: "object" },
      },
      mappings: {
        type: "array",
        description: "Known field mappings from detect_field_mappings",
        items: {
          type: "object",
          properties: {
            field_a: { type: "string" },
            field_b: { type: "string" },
            confidence: { type: "number" },
            reason: { type: "string" },
          },
          required: ["field_a", "field_b", "confidence", "reason"],
        },
      },
    },
    required: ["schema_a", "schema_b", "sample_a", "sample_b", "mappings"],
  },
  handler: async (input: InferRulesInput): Promise<InferRulesOutput> => {
    const parsed = InferRulesInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(INFER_RULES_SYSTEM, userMessage, InferRulesOutputSchema);
  },
};
