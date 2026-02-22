import { callClaude } from "../llm/client.js";
import { NL_TO_SQL_SYSTEM } from "../llm/prompts.js";
import {
  NlToSqlInputSchema,
  NlToSqlOutputSchema,
  type NlToSqlInput,
  type NlToSqlOutput,
} from "../types/tool-io.js";

function buildUserMessage(input: NlToSqlInput): string {
  let msg = `User rule description: "${input.text}"\n\n`;

  msg += `Source A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");

  if (input.mappings.length > 0) {
    msg += "\n\nField mappings (A → B):\n";
    msg += input.mappings
      .map((m) => `  - ${m.field_a} → ${m.field_b} (confidence: ${m.confidence})`)
      .join("\n");
  }

  msg += "\n\nConvert this into a DataFusion SQL expression.";
  return msg;
}

export const nlToSql = {
  name: "nl_to_sql" as const,
  description:
    "Convert a natural language matching rule description into a DataFusion SQL expression, using schema context and field mappings.",
  inputSchema: {
    type: "object" as const,
    properties: {
      text: { type: "string", description: "Natural language rule description" },
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
      mappings: {
        type: "array",
        description: "Field mappings",
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
    required: ["text", "schema_a", "schema_b", "mappings"],
  },
  handler: async (input: NlToSqlInput): Promise<NlToSqlOutput> => {
    const parsed = NlToSqlInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(NL_TO_SQL_SYSTEM, userMessage, NlToSqlOutputSchema);
  },
};
