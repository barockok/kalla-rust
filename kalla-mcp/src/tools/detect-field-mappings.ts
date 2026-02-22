import { callClaude } from "../llm/client.js";
import { DETECT_FIELD_MAPPINGS_SYSTEM } from "../llm/prompts.js";
import {
  DetectFieldMappingsInputSchema,
  DetectFieldMappingsOutputSchema,
  type DetectFieldMappingsInput,
  type DetectFieldMappingsOutput,
} from "../types/tool-io.js";

function formatSampleRows(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "(empty)";
  const keys = Object.keys(rows[0]);
  const header = keys.join(" | ");
  const body = rows
    .slice(0, 5)
    .map((r) => keys.map((k) => String(r[k] ?? "null")).join(" | "))
    .join("\n");
  return `${header}\n${body}`;
}

function buildUserMessage(input: DetectFieldMappingsInput): string {
  let msg = `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.sample_a && input.sample_a.length > 0) {
    msg += `\n\nSample rows (Source A):\n${formatSampleRows(input.sample_a)}`;
  }

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.sample_b && input.sample_b.length > 0) {
    msg += `\n\nSample rows (Source B):\n${formatSampleRows(input.sample_b)}`;
  }

  msg += "\n\nIdentify all column pairs that represent the same information.";
  return msg;
}

export const detectFieldMappings = {
  name: "detect_field_mappings" as const,
  description:
    "Analyze two source schemas (with optional sample rows) and detect columns that represent the same real-world information despite having different names. Returns field mappings with confidence scores and suggested filter types.",
  inputSchema: {
    type: "object" as const,
    properties: {
      schema_a: {
        type: "object",
        description: "First source schema with alias and columns",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: {
                name: { type: "string" },
                data_type: { type: "string" },
                nullable: { type: "boolean" },
              },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      schema_b: {
        type: "object",
        description: "Second source schema with alias and columns",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: {
                name: { type: "string" },
                data_type: { type: "string" },
                nullable: { type: "boolean" },
              },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      sample_a: {
        type: "array",
        description: "Optional sample rows from source A for better detection",
        items: { type: "object" },
      },
      sample_b: {
        type: "array",
        description: "Optional sample rows from source B for better detection",
        items: { type: "object" },
      },
    },
    required: ["schema_a", "schema_b"],
  },
  handler: async (input: DetectFieldMappingsInput): Promise<DetectFieldMappingsOutput> => {
    const parsed = DetectFieldMappingsInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(DETECT_FIELD_MAPPINGS_SYSTEM, userMessage, DetectFieldMappingsOutputSchema);
  },
};
