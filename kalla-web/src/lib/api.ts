const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

// Types matching the Rust server
export interface DataSource {
  alias: string;
  uri: string;
  primary_key?: string[];
}

export interface MatchCondition {
  left: string;
  op: "eq" | "tolerance" | "gt" | "lt" | "gte" | "lte" | "contains" | "startswith" | "endswith";
  right: string;
  threshold?: number;
}

export interface MatchRule {
  name: string;
  pattern: "1:1" | "1:N" | "M:1";
  conditions: MatchCondition[];
  priority?: number;
}

export interface OutputConfig {
  matched: string;
  unmatched_left: string;
  unmatched_right: string;
}

export interface MatchRecipe {
  version: string;
  recipe_id: string;
  sources: {
    left: DataSource;
    right: DataSource;
  };
  match_rules: MatchRule[];
  output: OutputConfig;
}

export interface RunSummary {
  run_id: string;
  recipe_id: string;
  status: string;
  started_at: string;
  matched_count: number;
  unmatched_left_count: number;
  unmatched_right_count: number;
}

export interface RunMetadata {
  run_id: string;
  recipe_id: string;
  started_at: string;
  completed_at?: string;
  left_source: string;
  right_source: string;
  left_record_count: number;
  right_record_count: number;
  matched_count: number;
  unmatched_left_count: number;
  unmatched_right_count: number;
  status: "Running" | "Completed" | "Failed";
}

// API functions
export async function healthCheck(): Promise<string> {
  const res = await fetch(`${API_BASE}/health`);
  return res.text();
}

export interface RegisteredSource {
  alias: string;
  uri: string;
  source_type: string;
  status: string;
}

export async function listSources(): Promise<RegisteredSource[]> {
  const res = await fetch(`${API_BASE}/api/sources`);
  if (!res.ok) {
    throw new Error("Failed to fetch sources");
  }
  return res.json();
}

export async function registerSource(alias: string, uri: string): Promise<{ success: boolean; message: string }> {
  const res = await fetch(`${API_BASE}/api/sources`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ alias, uri }),
  });
  if (!res.ok) {
    const error = await res.text();
    throw new Error(error);
  }
  return res.json();
}

export async function validateRecipe(recipe: MatchRecipe): Promise<{ valid: boolean; errors: string[] }> {
  const res = await fetch(`${API_BASE}/api/recipes/validate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(recipe),
  });
  return res.json();
}

export async function generateRecipe(
  leftSource: string,
  rightSource: string,
  prompt: string
): Promise<{ recipe?: MatchRecipe; error?: string }> {
  const res = await fetch(`${API_BASE}/api/recipes/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      left_source: leftSource,
      right_source: rightSource,
      prompt,
    }),
  });
  if (!res.ok) {
    const error = await res.text();
    throw new Error(error);
  }
  return res.json();
}

export async function createRun(recipe: MatchRecipe): Promise<{ run_id: string; status: string }> {
  const res = await fetch(`${API_BASE}/api/runs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ recipe }),
  });
  if (!res.ok) {
    const error = await res.text();
    throw new Error(error);
  }
  return res.json();
}

export async function listRuns(): Promise<RunSummary[]> {
  const res = await fetch(`${API_BASE}/api/runs`);
  return res.json();
}

export async function getRun(id: string): Promise<RunMetadata> {
  const res = await fetch(`${API_BASE}/api/runs/${id}`);
  if (!res.ok) {
    throw new Error("Run not found");
  }
  return res.json();
}
