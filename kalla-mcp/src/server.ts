import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { detectFieldMappings } from "./tools/detect-field-mappings.js";
import { parseNlFilter } from "./tools/parse-nl-filter.js";
import { inferRules } from "./tools/infer-rules.js";
import { buildRecipe } from "./tools/build-recipe.js";
import { nlToSql } from "./tools/nl-to-sql.js";
import {
  DetectFieldMappingsInputSchema,
  ParseNlFilterInputSchema,
  InferRulesInputSchema,
  BuildRecipeInputSchema,
  NlToSqlInputSchema,
} from "./types/tool-io.js";

export function createServer(): McpServer {
  const server = new McpServer({
    name: "kalla-mcp",
    version: "0.1.0",
  });

  server.tool(
    detectFieldMappings.name,
    detectFieldMappings.description,
    DetectFieldMappingsInputSchema.shape,
    async (args) => {
      try {
        const result = await detectFieldMappings.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );

  server.tool(
    parseNlFilter.name,
    parseNlFilter.description,
    ParseNlFilterInputSchema.shape,
    async (args) => {
      try {
        const result = await parseNlFilter.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );

  server.tool(
    inferRules.name,
    inferRules.description,
    InferRulesInputSchema.shape,
    async (args) => {
      try {
        const result = await inferRules.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );

  server.tool(
    buildRecipe.name,
    buildRecipe.description,
    BuildRecipeInputSchema.shape,
    async (args) => {
      try {
        const result = await buildRecipe.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );

  server.tool(
    nlToSql.name,
    nlToSql.description,
    NlToSqlInputSchema.shape,
    async (args) => {
      try {
        const result = await nlToSql.handler(args);
        return { content: [{ type: "text" as const, text: JSON.stringify(result) }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text" as const, text: JSON.stringify({ error: message }) }], isError: true };
      }
    },
  );

  return server;
}
