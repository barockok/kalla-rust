import Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";

let anthropicClient: Anthropic | null = null;

function getClient(): Anthropic {
  if (anthropicClient) return anthropicClient;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error("ANTHROPIC_API_KEY is not set");
  const baseURL = process.env.ANTHROPIC_BASE_URL || undefined;
  anthropicClient = new Anthropic({ apiKey, ...(baseURL ? { baseURL } : {}) });
  return anthropicClient;
}

const MODEL = process.env.ANTHROPIC_MODEL || "claude-sonnet-4-20250514";

export function parseJsonResponse<T>(text: string, schema: z.ZodSchema<T>): T {
  const fenced = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
  const jsonStr = fenced ? fenced[1].trim() : text.trim();

  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    throw new Error(`Failed to parse JSON from LLM response: ${jsonStr.slice(0, 200)}`);
  }

  const result = schema.safeParse(parsed);
  if (!result.success) {
    throw new Error(`LLM response failed schema validation: ${result.error.message}`);
  }
  return result.data;
}

export async function callClaude<T>(
  systemPrompt: string,
  userMessage: string,
  outputSchema: z.ZodSchema<T>,
): Promise<T> {
  const client = getClient();

  const response = await client.messages.create({
    model: MODEL,
    max_tokens: 4096,
    system: systemPrompt,
    messages: [{ role: "user", content: userMessage }],
  });

  const textBlock = response.content.find((b) => b.type === "text");
  if (!textBlock || textBlock.type !== "text") {
    throw new Error("No text block in Claude response");
  }

  try {
    return parseJsonResponse(textBlock.text, outputSchema);
  } catch (firstError) {
    const retryResponse = await client.messages.create({
      model: MODEL,
      max_tokens: 4096,
      system: systemPrompt,
      messages: [
        { role: "user", content: userMessage },
        { role: "assistant", content: textBlock.text },
        {
          role: "user",
          content: `Your response had a formatting issue: ${firstError instanceof Error ? firstError.message : String(firstError)}\n\nPlease return ONLY valid JSON matching the required schema. No explanation, just JSON.`,
        },
      ],
    });

    const retryText = retryResponse.content.find((b) => b.type === "text");
    if (!retryText || retryText.type !== "text") {
      throw new Error("No text block in retry response");
    }
    return parseJsonResponse(retryText.text, outputSchema);
  }
}
