import { NextResponse } from "next/server";
import { callMcpTool } from "@/lib/mcp-client";

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { tool, input } = body;

    if (!tool || typeof tool !== "string") {
      return NextResponse.json({ error: "Missing or invalid 'tool' field" }, { status: 400 });
    }

    if (!input || typeof input !== "object") {
      return NextResponse.json({ error: "Missing or invalid 'input' field" }, { status: 400 });
    }

    const result = await callMcpTool(tool, input);
    return NextResponse.json({ result });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[/api/ai] Error:`, message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
