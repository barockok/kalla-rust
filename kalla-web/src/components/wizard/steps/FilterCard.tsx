"use client";

import { useEffect, useRef } from "react";
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
import { Button } from "@/components/ui/button";
import {
  SlidersHorizontal,
  Sparkles,
  Calendar as CalendarIcon,
  DollarSign,
  ArrowRight,
  Landmark,
  FileText,
  Search,
  Loader2,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { format, parse } from "date-fns";
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
  date_range: CalendarIcon,
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
/*  NLFilterInput                                                      */
/* ------------------------------------------------------------------ */

function NLFilterInput() {
  const { state, dispatch } = useWizard();
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  function handleChange(text: string) {
    dispatch({ type: "SET_NL_TEXT", text });

    if (timerRef.current) clearTimeout(timerRef.current);
    if (!text.trim()) return;

    timerRef.current = setTimeout(async () => {
      if (!state.schemaLeft || !state.schemaRight || !state.leftSource || !state.rightSource) return;

      dispatch({ type: "SET_LOADING", key: "parseNlFilter", value: true });
      dispatch({ type: "SET_ERROR", key: "parseNlFilter", error: null });

      try {
        const result = await callAI<{
          filters: Array<{ source: string; column: string; op: string; value: unknown }>;
          explanation: string;
        }>("parse_nl_filter", {
          text,
          schema_a: { alias: state.leftSource!.alias, columns: state.schemaLeft },
          schema_b: { alias: state.rightSource!.alias, columns: state.schemaRight },
          current_mappings: state.fieldMappings,
        });

        // Merge NL filter results into common filters
        const updatedFilters = [...state.commonFilters];
        for (const f of result.filters) {
          const col = [...(state.schemaLeft || []), ...(state.schemaRight || [])].find(
            (c) => c.name === f.column,
          );
          if (!col) continue;

          const isDate = col.data_type.includes("date") || col.data_type.includes("timestamp");
          const isNumeric =
            col.data_type.includes("numeric") ||
            col.data_type.includes("decimal") ||
            col.data_type.includes("int");

          const targetType = isDate ? "date_range" : isNumeric ? "amount_range" : null;
          if (!targetType) continue;

          const existing = updatedFilters.find((cf) => cf.type === targetType);
          if (existing && f.op === "between" && Array.isArray(f.value)) {
            existing.value = [String(f.value[0]), String(f.value[1])];
          } else if (existing && (f.op === "gt" || f.op === "gte")) {
            existing.value = [String(f.value), existing.value?.[1] || ""];
          } else if (existing && (f.op === "lt" || f.op === "lte")) {
            existing.value = [existing.value?.[0] || "", String(f.value)];
          }
        }

        dispatch({ type: "SET_NL_RESULT", filters: updatedFilters, explanation: result.explanation });
      } catch (err) {
        dispatch({
          type: "SET_ERROR",
          key: "parseNlFilter",
          error: err instanceof Error ? err.message : "Failed to parse filter",
        });
      } finally {
        dispatch({ type: "SET_LOADING", key: "parseNlFilter", value: false });
      }
    }, 500);
  }

  return (
    <div className="mt-4">
      <div className="flex items-center gap-2 rounded-lg border-[1.5px] border-input px-3 py-2.5">
        <Sparkles className="h-4 w-4 shrink-0 text-blue-500" />
        <input
          type="text"
          className="flex-1 bg-transparent text-xs outline-none placeholder:italic placeholder:text-muted-foreground"
          placeholder="Adjust filters in your own words..."
          value={state.nlFilterText}
          onChange={(e) => handleChange(e.target.value)}
        />
        {state.loading.parseNlFilter && (
          <div className="h-3 w-3 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
        )}
      </div>
      {state.nlFilterExplanation && (
        <p className="mt-1.5 text-[11px] text-muted-foreground">{state.nlFilterExplanation}</p>
      )}
      {state.errors.parseNlFilter && (
        <p className="mt-1.5 text-[11px] text-destructive">{state.errors.parseNlFilter}</p>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  SourceSpecificFilters                                              */
/* ------------------------------------------------------------------ */

function SourceSpecificFilters() {
  const { state } = useWizard();
  const { leftSource, rightSource, schemaLeft, schemaRight } = state;

  if (!leftSource || !rightSource || !schemaLeft || !schemaRight) return null;

  const usedLeft = new Set(state.commonFilters.map((f) => f.field_a));
  const usedRight = new Set(state.commonFilters.map((f) => f.field_b));
  const extraLeft = schemaLeft.filter((c) => !usedLeft.has(c.name));
  const extraRight = schemaRight.filter((c) => !usedRight.has(c.name));

  return (
    <div>
      <h4 className="mb-3 text-[13px] font-semibold">Source-Specific Filters</h4>
      <div className="grid grid-cols-2 gap-6">
        <div>
          <div className="mb-2 flex items-center gap-2">
            <Landmark className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-xs font-semibold">{leftSource.alias}</span>
          </div>
          {extraLeft.slice(0, 2).map((col) => (
            <div key={col.name} className="mb-2">
              <label className="mb-1 block text-[10px] font-medium text-muted-foreground">
                {col.name}
              </label>
              <Select>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder={`All ${col.name}s`} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__" className="text-xs">
                    All
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
          ))}
        </div>
        <div>
          <div className="mb-2 flex items-center gap-2">
            <FileText className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-xs font-semibold">{rightSource.alias}</span>
          </div>
          {extraRight.slice(0, 2).map((col) => (
            <div key={col.name} className="mb-2">
              <label className="mb-1 block text-[10px] font-medium text-muted-foreground">
                {col.name}
              </label>
              <Select>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder={`All ${col.name}s`} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__" className="text-xs">
                    All
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
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

  /* --- Load Sample handler --- */

  async function handleLoadSample() {
    if (!leftSource || !rightSource) return;

    dispatch({ type: "SET_LOADING", key: "loadSample", value: true });
    dispatch({ type: "SET_ERROR", key: "loadSample", error: null });

    try {
      const leftConditions = commonFilters
        .filter((f) => f.value && f.value[0] && f.value[1])
        .map((f) => ({
          column: f.field_a,
          op: "between" as const,
          value: f.value!,
        }));

      const rightConditions = commonFilters
        .filter((f) => f.value && f.value[0] && f.value[1])
        .map((f) => ({
          column: f.field_b,
          op: "between" as const,
          value: f.value!,
        }));

      const [resLeft, resRight] = await Promise.all([
        fetch(`/api/sources/${leftSource.alias}/load-scoped`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ conditions: leftConditions, limit: 50 }),
        }).then((r) => r.json()),
        fetch(`/api/sources/${rightSource.alias}/load-scoped`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ conditions: rightConditions, limit: 50 }),
        }).then((r) => r.json()),
      ]);

      dispatch({
        type: "SET_SAMPLE",
        side: "left",
        data: { columns: resLeft.columns, rows: resLeft.rows, totalRows: resLeft.total_rows },
      });
      dispatch({
        type: "SET_SAMPLE",
        side: "right",
        data: { columns: resRight.columns, rows: resRight.rows, totalRows: resRight.total_rows },
      });
    } catch (err) {
      dispatch({
        type: "SET_ERROR",
        key: "loadSample",
        error: err instanceof Error ? err.message : "Failed to load sample data",
      });
    } finally {
      dispatch({ type: "SET_LOADING", key: "loadSample", value: false });
    }
  }

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
                    <Popover>
                      <PopoverTrigger asChild>
                        <Button
                          variant="outline"
                          className="flex-1 justify-start text-left font-normal"
                        >
                          <CalendarIcon className="mr-2 h-4 w-4" />
                          {filter.value?.[0]
                            ? format(
                                parse(filter.value[0], "yyyy-MM-dd", new Date()),
                                "MMM d, yyyy",
                              )
                            : "Start date"}
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-auto p-0" align="start">
                        <Calendar
                          mode="single"
                          selected={
                            filter.value?.[0]
                              ? parse(filter.value[0], "yyyy-MM-dd", new Date())
                              : undefined
                          }
                          onSelect={(date) =>
                            dispatch({
                              type: "UPDATE_COMMON_FILTER",
                              id: filter.id,
                              updates: {
                                value: [
                                  date ? format(date, "yyyy-MM-dd") : "",
                                  filter.value?.[1] ?? "",
                                ],
                              },
                            })
                          }
                        />
                      </PopoverContent>
                    </Popover>
                    <span className="text-xs text-muted-foreground">to</span>
                    <Popover>
                      <PopoverTrigger asChild>
                        <Button
                          variant="outline"
                          className="flex-1 justify-start text-left font-normal"
                        >
                          <CalendarIcon className="mr-2 h-4 w-4" />
                          {filter.value?.[1]
                            ? format(
                                parse(filter.value[1], "yyyy-MM-dd", new Date()),
                                "MMM d, yyyy",
                              )
                            : "End date"}
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-auto p-0" align="start">
                        <Calendar
                          mode="single"
                          selected={
                            filter.value?.[1]
                              ? parse(filter.value[1], "yyyy-MM-dd", new Date())
                              : undefined
                          }
                          onSelect={(date) =>
                            dispatch({
                              type: "UPDATE_COMMON_FILTER",
                              id: filter.id,
                              updates: {
                                value: [
                                  filter.value?.[0] ?? "",
                                  date ? format(date, "yyyy-MM-dd") : "",
                                ],
                              },
                            })
                          }
                        />
                      </PopoverContent>
                    </Popover>
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

        {/* NL Override Input */}
        {!isLoading && commonFilters.length > 0 && <NLFilterInput />}

        {/* Divider */}
        <div className="my-5 h-px bg-border" />

        {/* Source-Specific Filters */}
        <SourceSpecificFilters />

        {/* Load Sample Button */}
        {state.errors.loadSample && (
          <p className="mt-3 text-sm text-destructive">{state.errors.loadSample}</p>
        )}
        <div className="mt-5">
          <Button
            className="w-full"
            onClick={handleLoadSample}
            disabled={state.loading.loadSample}
          >
            {state.loading.loadSample ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Search className="mr-2 h-4 w-4" />
            )}
            Load Sample
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
