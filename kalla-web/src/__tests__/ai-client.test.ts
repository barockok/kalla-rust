import { callAI, AIError } from "@/lib/ai-client";

const mockFetch = jest.fn();
global.fetch = mockFetch;

describe("callAI", () => {
  afterEach(() => mockFetch.mockReset());

  it("calls /api/ai with tool and input, returns result", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ result: { mappings: [] } }),
    });

    const result = await callAI("detect_field_mappings", { schema_a: {} });
    expect(mockFetch).toHaveBeenCalledWith("/api/ai", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tool: "detect_field_mappings", input: { schema_a: {} } }),
    });
    expect(result).toEqual({ mappings: [] });
  });

  it("throws AIError on non-ok response", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 500,
      json: async () => ({ error: "MCP tool failed" }),
    });

    await expect(callAI("bad_tool", {})).rejects.toThrow(AIError);
  });
});
