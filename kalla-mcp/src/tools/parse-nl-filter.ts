import { callClaude } from "../llm/client.js";
import { PARSE_NL_FILTER_SYSTEM } from "../llm/prompts.js";
import {
  ParseNlFilterInputSchema,
  ParseNlFilterOutputSchema,
  type ParseNlFilterInput,
  type ParseNlFilterOutput,
} from "../types/tool-io.js";

function buildUserMessage(input: ParseNlFilterInput): string {
  let msg = `User instruction: "${input.text}"\n\n`;

  msg += `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.current_mappings.length > 0) {
    msg += "\n\nCurrent field mappings (source A → source B):\n";
    msg += input.current_mappings
      .map((m) => `  - ${m.field_a} → ${m.field_b} (confidence: ${m.confidence})`)
      .join("\n");
  }

  msg += "\n\nTranslate the user instruction into filter conditions.";
  return msg;
}

export const parseNlFilter = {
  name: "parse_nl_filter" as const,
  description:
    "Translate a natural language filter description into structured filter conditions, using schema context and field mappings to resolve column references.",
  inputSchema: {
    type: "object" as const,
    properties: {
      text: { type: "string", description: "Natural language filter description from user" },
      schema_a: {
        type: "object",
        description: "First source schema",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: { name: { type: "string" }, data_type: { type: "string" } },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      schema_b: {
        type: "object",
        description: "Second source schema",
        properties: {
          alias: { type: "string" },
          columns: {
            type: "array",
            items: {
              type: "object",
              properties: { name: { type: "string" }, data_type: { type: "string" } },
              required: ["name", "data_type"],
            },
          },
        },
        required: ["alias", "columns"],
      },
      current_mappings: {
        type: "array",
        description: "Current field mappings between sources",
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
    required: ["text", "schema_a", "schema_b", "current_mappings"],
  },
  handler: async (input: ParseNlFilterInput): Promise<ParseNlFilterOutput> => {
    const parsed = ParseNlFilterInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(PARSE_NL_FILTER_SYSTEM, userMessage, ParseNlFilterOutputSchema);
  },
};
