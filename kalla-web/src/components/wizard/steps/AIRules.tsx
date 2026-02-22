"use client";

import { useEffect, useState } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { callAI } from "@/lib/ai-client";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  ArrowRight,
  Loader2,
  Sparkles,
  Landmark,
  FileText,
  Key,
  CheckCircle2,
  X,
} from "lucide-react";
import type { DetectedPattern, RuleWithStatus } from "@/lib/wizard-types";

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

/* ── PatternCard ──────────────────────────────── */
function PatternCard() {
  const { state } = useWizard();
  const p = state.detectedPattern;
  if (!p) return null;

  const aliasA = state.leftSource?.alias ?? "Source A";
  const aliasB = state.rightSource?.alias ?? "Source B";

  return (
    <div className="rounded-xl border-[1.5px] border-border p-6">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold">Detected Pattern</h3>
        <Badge variant="outline" className="text-xs">
          {Math.round(p.confidence * 100)}% confident
        </Badge>
      </div>
      <div className="mt-4 flex items-center justify-center gap-6 py-4">
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-3 py-1.5 text-sm font-medium">
          <Landmark className="h-3.5 w-3.5" />
          {aliasA}
        </span>
        <Badge className="bg-foreground text-background font-mono text-sm px-3 py-1">
          {p.type}
        </Badge>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-3 py-1.5 text-sm font-medium">
          <FileText className="h-3.5 w-3.5" />
          {aliasB}
        </span>
      </div>
      <p className="text-[13px] leading-relaxed text-muted-foreground">
        {p.description}
      </p>
    </div>
  );
}

/* ── PrimaryKeysCard ──────────────────────────── */
function PrimaryKeysCard() {
  const { state } = useWizard();
  const pk = state.primaryKeys;
  if (!pk) return null;

  return (
    <div className="rounded-xl border-[1.5px] border-border p-6">
      <div className="flex items-center gap-2">
        <Key className="h-4 w-4 text-muted-foreground" />
        <h3 className="text-sm font-semibold">Primary Keys & Join Fields</h3>
      </div>
      <p className="mt-2 text-[13px] leading-relaxed text-muted-foreground">
        AI identified the following fields as primary keys for joining records
        across sources.
      </p>
      <div className="mt-3 flex items-center justify-center gap-6 rounded-lg bg-muted py-3">
        <span className="rounded bg-background px-3 py-1 text-sm font-mono">
          {pk.source_a.join(", ")}
        </span>
        <ArrowRight className="h-4 w-4 text-muted-foreground" />
        <span className="rounded bg-background px-3 py-1 text-sm font-mono">
          {pk.source_b.join(", ")}
        </span>
      </div>
    </div>
  );
}

/* ── RuleCard ─────────────────────────────────── */
function RuleCard({ rule }: { rule: RuleWithStatus }) {
  const { dispatch } = useWizard();

  if (rule.status === "rejected") return null;

  return (
    <div className="rounded-xl border-[1.5px] border-border p-5">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-semibold">{rule.name}</h4>
        <Badge variant="outline" className="text-xs">
          {Math.round(rule.confidence * 100)}% match
        </Badge>
      </div>
      <p className="mt-2 text-[13px] leading-relaxed text-muted-foreground">
        {rule.description}
      </p>
      <div className="mt-3 rounded-lg bg-muted px-3.5 py-2.5">
        <code className="text-xs font-mono text-foreground whitespace-pre-wrap break-all">
          {rule.sql}
        </code>
      </div>
      {rule.evidence.length > 0 && (
        <div className="mt-3">
          <p className="text-xs font-medium text-muted-foreground mb-1">
            Sample Evidence
          </p>
          <div className="overflow-x-auto rounded border text-xs">
            <table className="w-full">
              <thead>
                <tr className="border-b bg-muted/50">
                  {Object.keys(rule.evidence[0]).map((k) => (
                    <th key={k} className="px-2 py-1 text-left font-medium">
                      {k}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rule.evidence.slice(0, 3).map((row, i) => (
                  <tr key={i} className="border-b last:border-0">
                    {Object.values(row).map((v, j) => (
                      <td key={j} className="px-2 py-1">
                        {String(v)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      <div className="mt-3 flex items-center justify-end gap-2">
        {rule.status === "pending" && (
          <>
            <Button
              size="sm"
              variant="outline"
              className="text-destructive border-destructive/30 hover:bg-destructive/10"
              onClick={() => dispatch({ type: "REJECT_RULE", id: rule.id })}
            >
              <X className="mr-1 h-3.5 w-3.5" />
              Reject
            </Button>
            <Button
              size="sm"
              className="bg-green-600 hover:bg-green-700 text-white"
              onClick={() => dispatch({ type: "ACCEPT_RULE", id: rule.id })}
            >
              <CheckCircle2 className="mr-1 h-3.5 w-3.5" />
              Accept
            </Button>
          </>
        )}
        {rule.status === "accepted" && (
          <Badge className="bg-green-100 text-green-700 border-green-200">
            Accepted
          </Badge>
        )}
      </div>
    </div>
  );
}

/* ── AddCustomRule ─────────────────────────────── */
function AddCustomRule() {
  const { state, dispatch } = useWizard();
  const [text, setText] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit() {
    if (!text.trim()) return;
    setSubmitting(true);
    try {
      const result = await callAI<{
        name: string;
        sql: string;
        description: string;
        confidence: number;
      }>("nl_to_sql", {
        text,
        schema_a: {
          alias: state.leftSource!.alias,
          columns: state.schemaLeft!,
        },
        schema_b: {
          alias: state.rightSource!.alias,
          columns: state.schemaRight!,
        },
        mappings: state.fieldMappings,
      });
      dispatch({
        type: "ADD_CUSTOM_RULE",
        rule: {
          id: `custom-${Date.now()}`,
          name: result.name,
          sql: result.sql,
          description: result.description,
          confidence: result.confidence,
          evidence: [],
          status: "accepted",
        },
      });
      setText("");
    } catch {
      dispatch({
        type: "SET_ERROR",
        key: "nlToSql",
        error: "Failed to convert rule",
      });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-2">
      <h4 className="text-sm font-medium">Add Custom Rule</h4>
      <p className="text-xs text-muted-foreground">
        Describe a matching rule in plain language. AI will convert it to SQL.
      </p>
      <div className="flex items-center gap-2 rounded-lg border-[1.5px] border-input px-3.5 py-2.5">
        <Sparkles className="h-4 w-4 text-muted-foreground shrink-0" />
        <input
          type="text"
          className="flex-1 bg-transparent text-sm placeholder:text-muted-foreground focus:outline-none"
          placeholder="e.g. Invoice date must be within 7 days of bank transaction date"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSubmit()}
          disabled={submitting}
        />
        {submitting && (
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        )}
      </div>
    </div>
  );
}

/* ── AIRules (parent) ─────────────────────────── */
export function AIRules() {
  const { state, dispatch } = useWizard();
  const isLoading = state.loading.inferRules;

  useEffect(() => {
    if (state.detectedPattern || !state.sampleLeft || !state.sampleRight)
      return;

    dispatch({ type: "SET_LOADING", key: "inferRules", value: true });

    const samplesA = rowsToRecords(state.schemaLeft!, state.sampleLeft.rows);
    const samplesB = rowsToRecords(state.schemaRight!, state.sampleRight.rows);

    callAI<{
      pattern: {
        type: string;
        description: string;
        confidence: number;
      };
      primary_keys: { source_a: string[]; source_b: string[] };
      rules: {
        name: string;
        sql: string;
        description: string;
        confidence: number;
        evidence: Record<string, unknown>[];
      }[];
    }>("infer_rules", {
      schema_a: {
        alias: state.leftSource!.alias,
        columns: state.schemaLeft!,
      },
      schema_b: {
        alias: state.rightSource!.alias,
        columns: state.schemaRight!,
      },
      sample_a: samplesA,
      sample_b: samplesB,
      mappings: state.fieldMappings,
    })
      .then((result) => {
        dispatch({
          type: "SET_INFERRED_RULES",
          pattern: result.pattern as DetectedPattern,
          primaryKeys: result.primary_keys,
          rules: result.rules.map((r, i) => ({
            ...r,
            id: `rule-${i}`,
            evidence: r.evidence ?? [],
            status: "pending" as const,
          })),
        });
      })
      .catch((err) => {
        dispatch({
          type: "SET_ERROR",
          key: "inferRules",
          error:
            err instanceof Error ? err.message : "Failed to infer rules",
        });
      })
      .finally(() => {
        dispatch({ type: "SET_LOADING", key: "inferRules", value: false });
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const acceptedRules = state.inferredRules.filter(
    (r) => r.status === "accepted",
  );
  const canContinue = acceptedRules.length > 0;

  async function handleContinue() {
    dispatch({ type: "SET_LOADING", key: "buildRecipe", value: true });
    try {
      const result = await callAI<{
        match_sql: string;
        explanation: string;
      }>("build_recipe", {
        rules: acceptedRules.map((r) => ({
          name: r.name,
          sql: r.sql,
          description: r.description,
        })),
        sources: {
          alias_a: state.leftSource!.alias,
          alias_b: state.rightSource!.alias,
        },
        primary_keys: state.primaryKeys!,
        pattern_type: state.detectedPattern!.type,
      });
      dispatch({ type: "SET_RECIPE_SQL", sql: result.match_sql });
      dispatch({ type: "SET_STEP", step: 4 });
    } catch (err) {
      dispatch({
        type: "SET_ERROR",
        key: "buildRecipe",
        error:
          err instanceof Error ? err.message : "Failed to build recipe",
      });
    } finally {
      dispatch({ type: "SET_LOADING", key: "buildRecipe", value: false });
    }
  }

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex flex-col gap-1">
          <h1 className="text-[22px] font-semibold">
            AI Pattern Detection & Rules
          </h1>
          <p className="text-sm text-muted-foreground">
            AI analyzes your sample data to detect matching patterns.
          </p>
        </div>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-1 text-xs font-medium">
          <Sparkles className="h-3 w-3" />
          AI-powered
        </span>
      </div>

      {isLoading ? (
        <div className="flex flex-col items-center justify-center gap-3 py-16">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          <p className="text-sm text-muted-foreground">
            Analyzing sample data...
          </p>
        </div>
      ) : state.errors.inferRules ? (
        <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4">
          <p className="text-sm text-destructive">{state.errors.inferRules}</p>
        </div>
      ) : (
        <>
          <PatternCard />
          <PrimaryKeysCard />

          {/* Rules section */}
          <h2 className="text-base font-semibold">AI-Suggested Rules</h2>
          <div className="flex flex-col gap-4">
            {state.inferredRules.map((rule) => (
              <RuleCard key={rule.id} rule={rule} />
            ))}
          </div>

          <AddCustomRule />
        </>
      )}

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 2 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button
          disabled={!canContinue || state.loading.buildRecipe}
          onClick={handleContinue}
        >
          {state.loading.buildRecipe && (
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          )}
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
