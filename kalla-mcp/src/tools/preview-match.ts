import { callClaude } from "../llm/client.js";
import { PREVIEW_MATCH_SYSTEM } from "../llm/prompts.js";
import {
  PreviewMatchInputSchema,
  PreviewMatchOutputSchema,
  type PreviewMatchInput,
  type PreviewMatchOutput,
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

function buildUserMessage(input: PreviewMatchInput): string {
  let msg = `Match SQL:\n${input.match_sql}\n`;

  msg += `\nSource A: "${input.schema_a.alias}"\nColumns:\n`;
  msg += input.schema_a.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\nPrimary keys: ${input.primary_keys.source_a.join(", ")}`;
  msg += `\n\nSample rows (Source A):\n${formatSampleRows(input.sample_a)}`;

  msg += `\n\nSource B: "${input.schema_b.alias}"\nColumns:\n`;
  msg += input.schema_b.columns.map((c) => `  - ${c.name} (${c.data_type})`).join("\n");
  msg += `\nPrimary keys: ${input.primary_keys.source_b.join(", ")}`;
  msg += `\n\nSample rows (Source B):\n${formatSampleRows(input.sample_b)}`;

  msg += "\n\nMatching rules:\n";
  msg += input.rules
    .map((r) => `  - ${r.name}: ${r.sql}\n    ${r.description}`)
    .join("\n");

  msg += "\n\nSimulate the match SQL against the sample data above. For each left-source row, determine if it matches any right-source rows based on the rules.";
  return msg;
}

export const previewMatch = {
  name: "preview_match" as const,
  description:
    "Simulate running a match SQL query against sample data to preview matching results. Returns matched/unmatched rows with summary statistics.",
  inputSchema: {
    type: "object" as const,
    properties: {
      match_sql: { type: "string", description: "The complete match SQL query" },
      sample_a: { type: "array", description: "Sample rows from source A", items: { type: "object" } },
      sample_b: { type: "array", description: "Sample rows from source B", items: { type: "object" } },
      schema_a: {
        type: "object", description: "Left source schema",
        properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } },
        required: ["alias", "columns"],
      },
      schema_b: {
        type: "object", description: "Right source schema",
        properties: { alias: { type: "string" }, columns: { type: "array", items: { type: "object", properties: { name: { type: "string" }, data_type: { type: "string" } }, required: ["name", "data_type"] } } },
        required: ["alias", "columns"],
      },
      primary_keys: {
        type: "object", description: "Primary key columns",
        properties: { source_a: { type: "array", items: { type: "string" } }, source_b: { type: "array", items: { type: "string" } } },
        required: ["source_a", "source_b"],
      },
      rules: {
        type: "array", description: "Matching rules", items: {
          type: "object", properties: { name: { type: "string" }, sql: { type: "string" }, description: { type: "string" } },
          required: ["name", "sql", "description"],
        },
      },
    },
    required: ["match_sql", "sample_a", "sample_b", "schema_a", "schema_b", "primary_keys", "rules"],
  },
  handler: async (input: PreviewMatchInput): Promise<PreviewMatchOutput> => {
    const parsed = PreviewMatchInputSchema.parse(input);
    const userMessage = buildUserMessage(parsed);
    return callClaude(PREVIEW_MATCH_SYSTEM, userMessage, PreviewMatchOutputSchema);
  },
};
