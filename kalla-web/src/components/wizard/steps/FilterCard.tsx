"use client";

import { useEffect } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { callAI } from "@/lib/ai-client";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  SlidersHorizontal,
  Sparkles,
  Calendar,
  DollarSign,
  ArrowRight,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import type { ColumnInfo } from "@/lib/chat-types";
import type {
  FieldMapping,
  SuggestedFilter,
  CommonFilter,
} from "@/lib/wizard-types";

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function rowsToRecords(
  columns: ColumnInfo[],
  rows: string[][],
): Record<string, unknown>[] {
  return rows.map((row) => {
    const record: Record<string, unknown> = {};
    columns.forEach((col, i) => {
      record[col.name] = row[i];
    });
    return record;
  });
}

const FILTER_ICONS: Record<string, LucideIcon> = {
  date_range: Calendar,
  amount_range: DollarSign,
};

const FILTER_LABELS: Record<string, string> = {
  date_range: "Date Range",
  amount_range: "Amount",
};

function buildCommonFilters(suggestedFilters: SuggestedFilter[]): CommonFilter[] {
  return suggestedFilters.map((sf) => ({
    id: sf.type,
    type: sf.type,
    label: FILTER_LABELS[sf.type] || sf.type,
    icon: sf.type,
    field_a: sf.field_a,
    field_b: sf.field_b,
    value: null,
  }));
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export function FilterCard() {
  const { state, dispatch } = useWizard();

  const {
    leftSource,
    rightSource,
    schemaLeft,
    schemaRight,
    previewLeft,
    previewRight,
    fieldMappings,
    commonFilters,
    loading,
    errors,
  } = state;

  /* --- AI detection on mount --- */
  useEffect(() => {
    if (fieldMappings.length > 0) return;
    if (!schemaLeft || !schemaRight) return;

    let cancelled = false;

    async function detect() {
      dispatch({ type: "SET_LOADING", key: "detectMappings", value: true });
      dispatch({ type: "SET_ERROR", key: "detectMappings", error: null });

      try {
        const input: Record<string, unknown> = {
          schema_a: {
            alias: leftSource?.alias ?? "source_a",
            columns: schemaLeft,
          },
          schema_b: {
            alias: rightSource?.alias ?? "source_b",
            columns: schemaRight,
          },
        };

        if (previewLeft && schemaLeft) {
          input.sample_a = rowsToRecords(schemaLeft, previewLeft);
        }
        if (previewRight && schemaRight) {
          input.sample_b = rowsToRecords(schemaRight, previewRight);
        }

        const result = await callAI<{
          mappings: FieldMapping[];
          suggested_filters: SuggestedFilter[];
        }>("detect_field_mappings", input);

        if (cancelled) return;

        dispatch({
          type: "SET_FIELD_MAPPINGS",
          mappings: result.mappings,
          suggestedFilters: result.suggested_filters,
        });

        dispatch({
          type: "SET_COMMON_FILTERS",
          filters: buildCommonFilters(result.suggested_filters),
        });
      } catch (err) {
        if (cancelled) return;
        dispatch({
          type: "SET_ERROR",
          key: "detectMappings",
          error:
            err instanceof Error ? err.message : "Failed to detect field mappings",
        });
      } finally {
        if (!cancelled) {
          dispatch({ type: "SET_LOADING", key: "detectMappings", value: false });
        }
      }
    }

    void detect();
    return () => {
      cancelled = true;
    };
  }, [
    fieldMappings.length,
    schemaLeft,
    schemaRight,
    previewLeft,
    previewRight,
    leftSource,
    rightSource,
    dispatch,
  ]);

  /* --- Render helpers --- */

  const isLoading = loading.detectMappings;
  const error = errors.detectMappings;
  const columnsA = schemaLeft ?? [];
  const columnsB = schemaRight ?? [];
  const aliasA = leftSource?.alias ?? "Source A";
  const aliasB = rightSource?.alias ?? "Source B";

  return (
    <Card>
      <CardContent className="flex flex-col gap-5">
        {/* Filter header */}
        <div className="flex items-center gap-2">
          <SlidersHorizontal className="h-4 w-4" />
          <span className="text-[15px] font-semibold">Filter</span>
        </div>

        {/* Note text */}
        <p className="text-sm italic text-muted-foreground">
          These common fields were detected across both sources. Mappings may
          vary &mdash; adjust or override below.
        </p>

        {/* Common Filters header */}
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-blue-500" />
          <span className="text-sm font-medium">Common Filters</span>
          <Badge
            className="rounded-full bg-blue-50 text-blue-600 border-transparent text-[9px] px-1.5 py-0"
          >
            AI-detected
          </Badge>
        </div>

        {/* Loading skeleton */}
        {isLoading && (
          <div className="flex flex-col gap-3">
            <div className="h-16 animate-pulse rounded-lg bg-muted/60" />
            <div className="h-16 animate-pulse rounded-lg bg-muted/60" />
          </div>
        )}

        {/* Error state */}
        {error && (
          <p className="text-sm text-destructive">{error}</p>
        )}

        {/* Common filter cards */}
        {!isLoading &&
          !error &&
          commonFilters.map((filter) => {
            const Icon = FILTER_ICONS[filter.type] ?? SlidersHorizontal;

            return (
              <div
                key={filter.id}
                className="rounded-lg bg-muted/50 p-4 flex flex-col gap-3"
              >
                {/* Icon + label */}
                <div className="flex items-center gap-2">
                  <Icon className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm font-medium">{filter.label}</span>
                </div>

                {/* Field mapping row */}
                <div className="flex items-end gap-3">
                  {/* Source A column */}
                  <div className="flex flex-col gap-1 flex-1">
                    <span className="text-xs text-muted-foreground">{aliasA}</span>
                    <Select
                      value={filter.field_a}
                      onValueChange={(val) =>
                        dispatch({
                          type: "UPDATE_COMMON_FILTER",
                          id: filter.id,
                          updates: { field_a: val },
                        })
                      }
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue placeholder="Select column" />
                      </SelectTrigger>
                      <SelectContent>
                        {columnsA.map((col) => (
                          <SelectItem key={col.name} value={col.name}>
                            {col.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground mb-2" />

                  {/* Source B column */}
                  <div className="flex flex-col gap-1 flex-1">
                    <span className="text-xs text-muted-foreground">{aliasB}</span>
                    <Select
                      value={filter.field_b}
                      onValueChange={(val) =>
                        dispatch({
                          type: "UPDATE_COMMON_FILTER",
                          id: filter.id,
                          updates: { field_b: val },
                        })
                      }
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue placeholder="Select column" />
                      </SelectTrigger>
                      <SelectContent>
                        {columnsB.map((col) => (
                          <SelectItem key={col.name} value={col.name}>
                            {col.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* Value input */}
                {filter.type === "date_range" && (
                  <div className="flex items-center gap-3">
                    <Input
                      type="date"
                      className="flex-1"
                      value={filter.value?.[0] ?? ""}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_COMMON_FILTER",
                          id: filter.id,
                          updates: {
                            value: [e.target.value, filter.value?.[1] ?? ""],
                          },
                        })
                      }
                    />
                    <span className="text-xs text-muted-foreground">to</span>
                    <Input
                      type="date"
                      className="flex-1"
                      value={filter.value?.[1] ?? ""}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_COMMON_FILTER",
                          id: filter.id,
                          updates: {
                            value: [filter.value?.[0] ?? "", e.target.value],
                          },
                        })
                      }
                    />
                  </div>
                )}

                {filter.type === "amount_range" && (
                  <div className="flex items-center gap-3">
                    <Input
                      type="number"
                      placeholder="Min"
                      className="flex-1"
                      value={filter.value?.[0] ?? ""}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_COMMON_FILTER",
                          id: filter.id,
                          updates: {
                            value: [e.target.value, filter.value?.[1] ?? ""],
                          },
                        })
                      }
                    />
                    <span className="text-xs text-muted-foreground">to</span>
                    <Input
                      type="number"
                      placeholder="Max"
                      className="flex-1"
                      value={filter.value?.[1] ?? ""}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_COMMON_FILTER",
                          id: filter.id,
                          updates: {
                            value: [filter.value?.[0] ?? "", e.target.value],
                          },
                        })
                      }
                    />
                  </div>
                )}
              </div>
            );
          })}

        {/* NL input — added in Task 5 */}
      </CardContent>
    </Card>
  );
}
