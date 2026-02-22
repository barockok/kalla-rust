import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import path from "path";

let client: Client | null = null;
let connecting: Promise<Client> | null = null;

export async function getMcpClient(): Promise<Client> {
  if (client) return client;

  // Prevent concurrent connection attempts
  if (connecting) return connecting;

  connecting = (async () => {
    const mcpServerPath = path.resolve(
      process.cwd(),
      process.env.MCP_SERVER_PATH || "../kalla-mcp/dist/index.js",
    );

    const transport = new StdioClientTransport({
      command: "node",
      args: [mcpServerPath],
      env: {
        ...process.env,
        ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY || "",
        ANTHROPIC_MODEL: process.env.ANTHROPIC_MODEL || "",
        ANTHROPIC_BASE_URL: process.env.ANTHROPIC_BASE_URL || "",
        LLM_API_URL: process.env.LLM_API_URL || "",
      } as Record<string, string>,
    });

    const c = new Client({ name: "kalla-web", version: "0.1.0" });
    await c.connect(transport);
    client = c;
    connecting = null;
    return c;
  })();

  return connecting;
}

interface TextContent {
  type: "text";
  text: string;
}

export async function callMcpTool(
  toolName: string,
  input: Record<string, unknown>,
): Promise<unknown> {
  const c = await getMcpClient();
  const result = await c.callTool({ name: toolName, arguments: input });

  // MCP returns content array; extract text content and parse JSON
  const content = result.content as Array<{ type: string; text?: string }>;
  const textContent = content.find(
    (item): item is TextContent => item.type === "text" && typeof item.text === "string",
  );

  if (!textContent) {
    throw new Error(`No text content in MCP tool response for ${toolName}`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(textContent.text);
  } catch {
    // MCP SDK returns plain-text error messages (not JSON)
    throw new Error(textContent.text);
  }

  if (result.isError) {
    const err = parsed as Record<string, unknown>;
    throw new Error((err.error as string) || `MCP tool ${toolName} failed`);
  }

  return parsed;
}
