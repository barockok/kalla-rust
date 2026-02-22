export class AIError extends Error {
  constructor(message: string, public statusCode?: number) {
    super(message);
    this.name = "AIError";
  }
}

export async function callAI<T>(
  tool: string,
  input: Record<string, unknown>,
): Promise<T> {
  const res = await fetch("/api/ai", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tool, input }),
  });

  const data = await res.json();

  if (!res.ok) {
    throw new AIError(data.error || `AI call failed: ${res.status}`, res.status);
  }

  return data.result as T;
}
