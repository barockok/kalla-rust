import type { ChatSession, SourceInfo, SourcePreview } from './chat-types';

// ---------------------------------------------------------------------------
// Agent Tool Implementations
//
// Each tool corresponds to one function the LLM agent can call via tool_use.
// Network-bound tools call the Rust backend HTTP API. Pure-logic tools (like
// propose_match, infer_rules, build_recipe) run locally in the Next.js process.
// ---------------------------------------------------------------------------

const RUST_API = process.env.SERVER_API_URL || process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';

// ---------------------------------------------------------------------------
// Tool: list_sources
// ---------------------------------------------------------------------------

export async function listSources(): Promise<SourceInfo[]> {
  const res = await fetch(`${RUST_API}/api/sources`);
  if (!res.ok) throw new Error(`Failed to list sources: ${res.statusText}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Tool: get_source_preview
// ---------------------------------------------------------------------------

export async function getSourcePreview(
  alias?: string,
  limit: number = 10,
  s3Uri?: string,
): Promise<SourcePreview> {
  // When s3_uri is provided, use the upload preview endpoint (Next.js server-side)
  if (s3Uri) {
    const baseUrl = process.env.NEXT_PUBLIC_BASE_URL || 'http://localhost:3000';
    const res = await fetch(`${baseUrl}/api/uploads/preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ s3_uri: s3Uri }),
    });
    if (!res.ok) throw new Error(`Failed to preview uploaded file: ${res.statusText}`);
    const data = await res.json();

    // Normalize upload preview response to match SourcePreview shape
    const columns = (data.columns as string[]).map((name: string) => ({
      name,
      data_type: 'string',
      nullable: true,
    }));
    const rows = (data.sample as Record<string, string>[]).map(
      (row: Record<string, string>) => data.columns.map((col: string) => row[col] ?? ''),
    );
    return {
      alias: s3Uri,
      columns,
      rows,
      total_rows: data.row_count,
      preview_rows: rows.length,
    };
  }

  if (!alias) throw new Error('Either alias or s3_uri must be provided');

  const res = await fetch(`${RUST_API}/api/sources/${encodeURIComponent(alias)}/preview?limit=${limit}`);
  if (!res.ok) throw new Error(`Failed to get preview for ${alias}: ${res.statusText}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Tool: request_file_upload
// ---------------------------------------------------------------------------

async function requestFileUpload(input: { message: string }): Promise<Record<string, unknown>> {
  return {
    card_type: 'upload_request',
    message: input.message,
  };
}

// ---------------------------------------------------------------------------
// Tool: load_scoped
//
// Loads a filtered subset of rows from a data source by POSTing structured
// filter conditions to the Rust backend, which translates them to the
// source's native query language (SQL WHERE, DataFrame filter, etc.).
// ---------------------------------------------------------------------------

export async function loadScoped(
  alias: string,
  conditions: Array<{ column: string; op: string; value: unknown }>,
  limit: number = 200,
): Promise<SourcePreview> {
  const res = await fetch(
    `${RUST_API}/api/sources/${encodeURIComponent(alias)}/load-scoped`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ conditions, limit }),
    },
  );
  if (!res.ok) throw new Error(`Failed to load scoped data from ${alias}: ${res.statusText}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Tool: propose_match
// ---------------------------------------------------------------------------

export function proposeMatch(
  leftRow: Record<string, string>,
  rightRow: Record<string, string>,
  reasoning: string,
): { left: Record<string, string>; right: Record<string, string>; reasoning: string } {
  return { left: leftRow, right: rightRow, reasoning };
}

// ---------------------------------------------------------------------------
// Tool: infer_rules
//
// Analyzes confirmed match pairs to detect column-level matching rules.
// Returns candidate rules sorted by confidence.
// ---------------------------------------------------------------------------

interface InferredRule {
  left_col: string;
  right_col: string;
  op: string;
  confidence: number;
  reasoning: string;
}

export function inferRules(
  confirmedPairs: Array<{ left: Record<string, unknown>; right: Record<string, unknown> }>,
  leftColumns: string[],
  rightColumns: string[],
): InferredRule[] {
  const rules: InferredRule[] = [];
  const total = confirmedPairs.length;
  if (total === 0) return rules;

  for (const leftCol of leftColumns) {
    for (const rightCol of rightColumns) {
      let exactMatches = 0;
      let toleranceMatches = 0;

      for (const pair of confirmedPairs) {
        const leftVal = String(pair.left[leftCol] ?? '');
        const rightVal = String(pair.right[rightCol] ?? '');

        if (!leftVal || !rightVal) continue;

        // Exact match check
        if (leftVal === rightVal) {
          exactMatches++;
          continue;
        }

        // Numeric tolerance check (5%)
        const leftNum = parseFloat(leftVal);
        const rightNum = parseFloat(rightVal);
        if (!isNaN(leftNum) && !isNaN(rightNum)) {
          const diff = Math.abs(leftNum - rightNum);
          const pctDiff = leftNum !== 0 ? diff / Math.abs(leftNum) : diff;
          if (pctDiff < 0.05) {
            toleranceMatches++;
            continue;
          }
        }
      }

      const exactConfidence = exactMatches / total;
      const toleranceConfidence = toleranceMatches / total;

      if (exactConfidence > 0.7) {
        rules.push({
          left_col: leftCol,
          right_col: rightCol,
          op: 'eq',
          confidence: exactConfidence,
          reasoning: `${Math.round(exactConfidence * 100)}% of pairs have exact match on ${leftCol} = ${rightCol}`,
        });
      } else if (toleranceConfidence > 0.5 || exactConfidence + toleranceConfidence > 0.7) {
        rules.push({
          left_col: leftCol,
          right_col: rightCol,
          op: 'tolerance',
          confidence: exactConfidence + toleranceConfidence,
          reasoning: `${Math.round((exactConfidence + toleranceConfidence) * 100)}% match with tolerance on ${leftCol} ~ ${rightCol}`,
        });
      }
    }
  }

  // Best rules first
  rules.sort((a, b) => b.confidence - a.confidence);
  return rules;
}

// ---------------------------------------------------------------------------
// Tool: build_recipe
// ---------------------------------------------------------------------------

interface RuleInput {
  name: string;
  pattern: string;
  conditions: Array<{ left: string; op: string; right: string; threshold?: number }>;
}

export function buildRecipe(
  leftAlias: string,
  rightAlias: string,
  leftUri: string,
  rightUri: string,
  leftPk: string[],
  rightPk: string[],
  rules: RuleInput[],
): Record<string, unknown> {
  return {
    version: '1.0',
    recipe_id: `recipe-${Date.now()}`,
    sources: {
      left: { alias: leftAlias, uri: leftUri, primary_key: leftPk },
      right: { alias: rightAlias, uri: rightUri, primary_key: rightPk },
    },
    match_rules: rules.map((r, i) => ({
      ...r,
      priority: i + 1,
    })),
    output: {
      matched: 'evidence/matched.parquet',
      unmatched_left: 'evidence/unmatched_left.parquet',
      unmatched_right: 'evidence/unmatched_right.parquet',
    },
  };
}

// ---------------------------------------------------------------------------
// Tool: validate_recipe
// ---------------------------------------------------------------------------

export async function validateRecipe(
  recipe: Record<string, unknown>,
): Promise<{ valid: boolean; errors: string[] }> {
  const res = await fetch(`${RUST_API}/api/recipes/validate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(recipe),
  });
  if (!res.ok) throw new Error(`Validation request failed: ${res.statusText}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Tool: run_sample — execute recipe on sample data
// ---------------------------------------------------------------------------

export async function runSample(
  recipe: Record<string, unknown>,
): Promise<{ run_id: string; status: string }> {
  const res = await fetch(`${RUST_API}/api/runs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ recipe }),
  });
  if (!res.ok) throw new Error(`Run creation failed: ${res.statusText}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Tool: run_full — execute recipe on the full datasets
// (Same endpoint as run_sample; the Rust backend always runs on the full
//  registered source.)
// ---------------------------------------------------------------------------

export async function runFull(
  recipe: Record<string, unknown>,
): Promise<{ run_id: string; status: string }> {
  return runSample(recipe);
}

// ---------------------------------------------------------------------------
// pollRunStatus — poll GET /api/runs/:id until no longer Running
// ---------------------------------------------------------------------------

export async function pollRunStatus(
  runId: string,
  maxWaitMs: number = 30_000,
  intervalMs: number = 1_000,
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + maxWaitMs;

  while (Date.now() < deadline) {
    const res = await fetch(`${RUST_API}/api/runs/${encodeURIComponent(runId)}`);
    if (!res.ok) throw new Error(`Failed to get run status: ${res.statusText}`);

    const run: Record<string, unknown> = await res.json();
    if (run.status !== 'Running') return run;

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Run ${runId} timed out after ${maxWaitMs}ms`);
}

// ---------------------------------------------------------------------------
// executeTool — switch dispatcher called by the agent orchestrator
// ---------------------------------------------------------------------------

export async function executeTool(
  toolName: string,
  args: Record<string, unknown>,
  session: ChatSession,
): Promise<unknown> {
  switch (toolName) {
    case 'list_sources':
      return listSources();

    case 'get_source_preview':
      return getSourcePreview(args.alias as string, (args.limit as number) || 10, args.s3_uri as string | undefined);

    case 'request_file_upload':
      return requestFileUpload(args as { message: string });

    case 'load_scoped':
      return loadScoped(
        args.alias as string,
        args.conditions as Array<{ column: string; op: string; value: unknown }>,
        (args.limit as number) || 200,
      );

    case 'propose_match':
      return proposeMatch(
        args.left_row as Record<string, string>,
        args.right_row as Record<string, string>,
        args.reasoning as string,
      );

    case 'infer_rules':
      return inferRules(
        session.confirmed_pairs,
        args.left_columns as string[],
        args.right_columns as string[],
      );

    case 'build_recipe':
      return buildRecipe(
        args.left_alias as string,
        args.right_alias as string,
        args.left_uri as string,
        args.right_uri as string,
        args.left_pk as string[],
        args.right_pk as string[],
        args.rules as RuleInput[],
      );

    case 'validate_recipe':
      return validateRecipe(args.recipe as Record<string, unknown>);

    case 'run_sample':
      return runSample(args.recipe as Record<string, unknown>);

    case 'run_full':
      return runFull(args.recipe as Record<string, unknown>);

    default:
      throw new Error(`Unknown tool: ${toolName}`);
  }
}
