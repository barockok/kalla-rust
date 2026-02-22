import { describe, it, expect } from "vitest";
import { z } from "zod";

describe("parseJsonResponse", () => {
  it("should parse valid JSON from text block", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ name: z.string(), value: z.number() });
    const result = parseJsonResponse('{"name": "test", "value": 42}', schema);
    expect(result).toEqual({ name: "test", value: 42 });
  });

  it("should extract JSON from markdown code block", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ items: z.array(z.string()) });
    const text = 'Here is the result:\n```json\n{"items": ["a", "b"]}\n```';
    const result = parseJsonResponse(text, schema);
    expect(result).toEqual({ items: ["a", "b"] });
  });

  it("should throw on invalid JSON", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ name: z.string() });
    expect(() => parseJsonResponse("not json at all", schema)).toThrow();
  });

  it("should throw on schema validation failure", async () => {
    const { parseJsonResponse } = await import("../client.js");
    const schema = z.object({ name: z.string(), required_field: z.number() });
    expect(() => parseJsonResponse('{"name": "test"}', schema)).toThrow();
  });
});
