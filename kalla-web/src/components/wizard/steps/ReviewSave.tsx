"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useWizard } from "@/components/wizard/wizard-context";
import { callAI } from "@/lib/ai-client";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  ArrowRight,
  Loader2,
  FlaskConical,
  Landmark,
  FileText,
  Save,
  Play,
} from "lucide-react";

function rowsToRecords(
  columns: { name: string }[],
  rows: string[][],
): Record<string, unknown>[] {
  return rows.map((row) => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => {
      obj[col.name] = row[i];
    });
    return obj;
  });
}

/* ── RecipeSummary ───────────────────────── */
function RecipeSummary() {
  const { state, dispatch } = useWizard();

  const aliasA = state.leftSource?.alias ?? "Source A";
  const aliasB = state.rightSource?.alias ?? "Source B";
  const acceptedRules = state.inferredRules.filter((r) => r.status === "accepted");

  return (
    <div className="rounded-xl border-[1.5px] border-border p-6">
      <h3 className="text-sm font-semibold">Recipe Summary</h3>

      {/* Recipe Name */}
      <div className="mt-4">
        <label className="text-[13px] font-medium text-muted-foreground">Recipe Name</label>
        <input
          type="text"
          value={state.recipeName}
          onChange={(e) => dispatch({ type: "SET_RECIPE_NAME", name: e.target.value })}
          placeholder="e.g. Bank-to-Invoice Monthly Recon"
          className="mt-1 w-full rounded-lg border-[1.5px] border-input bg-transparent px-3.5 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
        />
      </div>

      {/* Pattern visual */}
      <div className="mt-4 flex items-center justify-center gap-6 rounded-lg bg-muted py-4">
        <span className="inline-flex items-center gap-1.5 rounded-full bg-background px-3 py-1.5 text-sm font-medium">
          <Landmark className="h-3.5 w-3.5" />
          {aliasA}
        </span>
        <Badge className="bg-foreground text-background font-mono text-sm px-3 py-1">
          {state.detectedPattern?.type ?? "—"}
        </Badge>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-background px-3 py-1.5 text-sm font-medium">
          <FileText className="h-3.5 w-3.5" />
          {aliasB}
        </span>
      </div>

      {/* Matching rules */}
      {acceptedRules.length > 0 && (
        <div className="mt-4">
          <p className="text-[13px] font-medium text-muted-foreground">Matching Rules</p>
          <ul className="mt-2 space-y-1.5">
            {acceptedRules.map((rule) => (
              <li key={rule.id} className="text-[13px] flex items-start gap-2">
                <ArrowRight className="mt-0.5 h-3 w-3 text-muted-foreground shrink-0" />
                <span>{rule.description}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

/* ── SampleMatchPreview ──────────────────── */
function SampleMatchPreview() {
  const { state, dispatch } = useWizard();
  const isLoading = state.loading.previewMatch;
  const preview = state.matchPreviewResult;

  useEffect(() => {
    if (preview || !state.builtRecipeSql || !state.sampleLeft || !state.sampleRight) return;

    dispatch({ type: "SET_LOADING", key: "previewMatch", value: true });

    const samplesA = rowsToRecords(state.schemaLeft!, state.sampleLeft.rows);
    const samplesB = rowsToRecords(state.schemaRight!, state.sampleRight.rows);

    const acceptedRules = state.inferredRules
      .filter((r) => r.status === "accepted")
      .map((r) => ({ name: r.name, sql: r.sql, description: r.description }));

    callAI<{
      matches: {
        left_row: Record<string, unknown>;
        right_rows: Record<string, unknown>[];
        status: "matched" | "unmatched" | "partial";
      }[];
      summary: { total_left: number; total_right: number; matched: number; unmatched: number };
    }>("preview_match", {
      match_sql: state.builtRecipeSql,
      sample_a: samplesA,
      sample_b: samplesB,
      schema_a: {
        alias: state.leftSource!.alias,
        columns: state.schemaLeft!,
      },
      schema_b: {
        alias: state.rightSource!.alias,
        columns: state.schemaRight!,
      },
      primary_keys: state.primaryKeys!,
      rules: acceptedRules,
    })
      .then((result) => {
        dispatch({ type: "SET_MATCH_PREVIEW", result });
      })
      .catch((err) => {
        dispatch({
          type: "SET_ERROR",
          key: "previewMatch",
          error: err instanceof Error ? err.message : "Failed to preview matches",
        });
      })
      .finally(() => {
        dispatch({ type: "SET_LOADING", key: "previewMatch", value: false });
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-12 rounded-xl border-[1.5px] border-border">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <p className="text-sm text-muted-foreground">Running sample match preview...</p>
      </div>
    );
  }

  if (state.errors.previewMatch) {
    return (
      <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4">
        <p className="text-sm text-destructive">{state.errors.previewMatch}</p>
      </div>
    );
  }

  if (!preview) return null;

  const leftPk = state.primaryKeys?.source_a[0] ?? "id";

  return (
    <div className="rounded-xl border-[1.5px] border-border">
      <div className="flex items-center justify-between px-6 py-4 border-b">
        <div className="flex items-center gap-2">
          <FlaskConical className="h-4 w-4 text-muted-foreground" />
          <h3 className="text-sm font-semibold">Sample Match Preview</h3>
        </div>
        <Badge className="bg-green-100 text-green-700 border-green-200 text-xs">
          {preview.summary.matched}/{preview.summary.total_left} matched
        </Badge>
      </div>
      <p className="px-6 pt-3 text-[13px] text-muted-foreground">
        Results from running your recipe against the {preview.summary.total_left} sample transactions.
      </p>
      <div className="overflow-x-auto px-3 pb-4 pt-3">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left">
              <th className="px-3 py-2 font-medium text-muted-foreground">Left Record</th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Matched Right Records</th>
              <th className="px-3 py-2 font-medium text-muted-foreground w-24">Status</th>
            </tr>
          </thead>
          <tbody>
            {preview.matches.map((match, i) => (
              <tr key={i} className="border-b last:border-0">
                <td className="px-3 py-2 font-mono text-[13px]">
                  {String(match.left_row[leftPk] ?? JSON.stringify(match.left_row).slice(0, 40))}
                </td>
                <td className="px-3 py-2 text-[13px]">
                  {match.right_rows.length > 0
                    ? match.right_rows.map((r, j) => (
                        <span key={j} className="mr-2 inline-block rounded bg-muted px-1.5 py-0.5 text-xs font-mono">
                          {String(Object.values(r)[0] ?? "—")}
                        </span>
                      ))
                    : <span className="text-muted-foreground">—</span>}
                </td>
                <td className="px-3 py-2">
                  {match.status === "matched" && (
                    <Badge className="bg-green-100 text-green-700 border-green-200 text-xs">Matched</Badge>
                  )}
                  {match.status === "unmatched" && (
                    <Badge variant="destructive" className="text-xs">Unmatched</Badge>
                  )}
                  {match.status === "partial" && (
                    <Badge variant="outline" className="text-xs text-amber-600 border-amber-300">Partial</Badge>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ── ReviewSave (parent) ─────────────────── */
export function ReviewSave() {
  const { state, dispatch } = useWizard();
  const router = useRouter();
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  async function handleSave() {
    if (!state.recipeName.trim()) {
      setSaveError("Recipe name is required");
      return;
    }
    setSaving(true);
    setSaveError(null);

    try {
      const payload = {
        recipe_id: crypto.randomUUID(),
        name: state.recipeName,
        description: "",
        match_sql: state.builtRecipeSql,
        match_description: state.detectedPattern?.description ?? "",
        sources: {
          left: {
            alias: state.leftSource!.alias,
            type: state.leftSource!.source_type,
            uri: state.leftSource!.uri,
            primary_key: state.primaryKeys!.source_a,
          },
          right: {
            alias: state.rightSource!.alias,
            type: state.rightSource!.source_type,
            uri: state.rightSource!.uri,
            primary_key: state.primaryKeys!.source_b,
          },
        },
      };

      const res = await fetch("/api/recipes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || `Save failed: ${res.status}`);
      }

      router.push("/recipes");
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : "Failed to save recipe");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-[22px] font-semibold">Review & Save</h1>
        <p className="text-sm text-muted-foreground">
          Confirm your recipe configuration and save.
        </p>
      </div>

      <RecipeSummary />
      <SampleMatchPreview />

      {/* Save error */}
      {saveError && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4">
          <p className="text-sm text-destructive">{saveError}</p>
        </div>
      )}

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 4 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <div className="flex items-center gap-3">
          <Button variant="outline" disabled className="opacity-50">
            <Play className="mr-2 h-4 w-4" />
            Save & Run Now
          </Button>
          <Button onClick={handleSave} disabled={saving}>
            {saving ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Save className="mr-2 h-4 w-4" />
            )}
            Save Recipe
          </Button>
        </div>
      </div>
    </div>
  );
}
