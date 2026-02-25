"use client";

import { useEffect, useRef, useCallback } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { Button } from "@/components/ui/button";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { callAI } from "@/lib/ai-client";
import type { SourceConfig, SampleData, FilterChip } from "@/lib/wizard-types";
import { CollapsedSourcesBar } from "./CollapsedSourcesBar";
import { ExpandedSourceCards } from "./ExpandedSourceCards";
import { SmartFilter } from "./SmartFilter";
import { SamplePreviewV2 } from "./SamplePreviewV2";

/* ------------------------------------------------------------------ */
/*  NL filter AI response shape                                        */
/* ------------------------------------------------------------------ */

interface NLFilterResponse {
  filters: Array<{
    source: string;       // "source_a" | "source_b" | "both"
    column: string;
    op: string;           // "between" | "gte" | "lte" | "like" | "eq" etc.
    value: unknown;
  }>;
  explanation: string;
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export function SampleDataV2() {
  const { state, dispatch } = useWizard();
  const {
    leftSource,
    rightSource,
    sourceConfigLeft,
    sourceConfigRight,
    sourcesExpanded,
    filterChips,
    sampleLeft,
    sampleRight,
    schemaLeft,
    schemaRight,
    fieldMappings,
    loading,
  } = state;

  const autoCollapsedRef = useRef(false);

  const leftAlias = leftSource?.alias ?? "Source A";
  const rightAlias = rightSource?.alias ?? "Source B";
  const canContinue = sampleLeft !== null && sampleRight !== null;

  /* ---------------------------------------------------------------- */
  /*  Source loaded callback                                           */
  /* ---------------------------------------------------------------- */

  const handleSourceLoaded = useCallback(
    (side: "left" | "right", config: SourceConfig, sample: SampleData) => {
      dispatch({ type: "SET_SOURCE_CONFIG", side, config });
      dispatch({ type: "SET_SAMPLE", side, data: sample });
    },
    [dispatch],
  );

  /* ---------------------------------------------------------------- */
  /*  Auto-collapse when both sources are loaded for the first time    */
  /* ---------------------------------------------------------------- */

  useEffect(() => {
    if (
      !autoCollapsedRef.current &&
      sourceConfigLeft?.loaded &&
      sourceConfigRight?.loaded
    ) {
      autoCollapsedRef.current = true;
      dispatch({ type: "TOGGLE_SOURCES_EXPANDED" });
    }
  }, [sourceConfigLeft?.loaded, sourceConfigRight?.loaded, dispatch]);

  /* ---------------------------------------------------------------- */
  /*  NL Filter submit                                                 */
  /* ---------------------------------------------------------------- */

  const handleFilterSubmit = useCallback(
    async (text: string) => {
      dispatch({ type: "SET_LOADING", key: "nlFilter", value: true });
      dispatch({ type: "SET_ERROR", key: "nlFilter", error: null });
      try {
        const result = await callAI<NLFilterResponse>("parse_nl_filter", {
          text,
          schema_a: { alias: leftAlias, columns: schemaLeft },
          schema_b: { alias: rightAlias, columns: schemaRight },
          current_mappings: fieldMappings,
        });

        // Transform AI response filters into FilterChip[]
        const chips: FilterChip[] = result.filters.flatMap((f, i) => {
          // Determine which sides this filter targets
          const sides: Array<{ scope: FilterChip["scope"]; alias: string; fieldKey: "field_a" | "field_b" }> =
            f.source === "source_a" ? [{ scope: "left", alias: leftAlias, fieldKey: "field_a" }]
            : f.source === "source_b" ? [{ scope: "right", alias: rightAlias, fieldKey: "field_b" }]
            : [
                { scope: "left", alias: leftAlias, fieldKey: "field_a" },
                { scope: "right", alias: rightAlias, fieldKey: "field_b" },
              ];

          // Determine icon from data type/op
          const col = [...(schemaLeft ?? []), ...(schemaRight ?? [])].find(
            (c) => c.name === f.column,
          );
          const isDate = col?.data_type?.includes("date") || col?.data_type?.includes("timestamp");
          const isNumeric = col?.data_type?.includes("numeric") || col?.data_type?.includes("decimal") || col?.data_type?.includes("int");
          const icon = isDate ? "calendar" : isNumeric ? "dollar-sign" : "type";

          // Determine chip type
          const type = f.op === "between" ? "date_range"
            : (f.op === "gte" || f.op === "lte" || f.op === "gt" || f.op === "lt") ? "amount_range"
            : "text_match";

          // Build label from op + value
          const label = `${f.column} ${f.op} ${Array.isArray(f.value) ? f.value.join(" – ") : String(f.value ?? "")}`;

          const value = Array.isArray(f.value) && f.value.length === 2
            ? [String(f.value[0]), String(f.value[1])] as [string, string]
            : f.value != null ? String(f.value)
            : null;

          return sides.map((s, j) => ({
            id: `chip-${Date.now()}-${i}-${j}`,
            label,
            icon,
            scope: s.scope,
            type,
            field_a: s.fieldKey === "field_a" ? f.column : undefined,
            field_b: s.fieldKey === "field_b" ? f.column : undefined,
            value,
            op: f.op,
            rawValue: f.value,
            sourceLabel: s.alias,
          }));
        });

        dispatch({ type: "SET_FILTER_CHIPS", chips: [...filterChips, ...chips] });
      } catch (err) {
        dispatch({
          type: "SET_ERROR",
          key: "nlFilter",
          error: err instanceof Error ? err.message : "Filter parsing failed",
        });
      } finally {
        dispatch({ type: "SET_LOADING", key: "nlFilter", value: false });
      }
    },
    [dispatch, schemaLeft, schemaRight, fieldMappings, filterChips, leftAlias, rightAlias],
  );

  /* ---------------------------------------------------------------- */
  /*  Remove chip                                                      */
  /* ---------------------------------------------------------------- */

  const handleRemoveChip = useCallback(
    (chipId: string) => {
      dispatch({ type: "REMOVE_FILTER_CHIP", chipId });
    },
    [dispatch],
  );

  /* ---------------------------------------------------------------- */
  /*  Auto-refresh preview (debounced 500ms)                           */
  /* ---------------------------------------------------------------- */

  useEffect(() => {
    if (!sourceConfigLeft?.activeAlias || !sourceConfigRight?.activeAlias) return;

    const timer = setTimeout(async () => {
      const buildConditions = (side: "left" | "right") => {
        const schema = side === "left" ? schemaLeft : schemaRight;
        const validCols = new Set(schema?.map((c) => c.name) ?? []);
        return filterChips
          .filter((c) => c.scope === "both" || c.scope === side)
          .map((c) => {
            const column = side === "left" ? c.field_a : c.field_b;
            if (!column || !validCols.has(column)) return null;
            // Use original AI op/value when available
            if (c.op && c.rawValue !== undefined) {
              return { column, op: c.op, value: c.rawValue };
            }
            // Fallback: map chip type to load-scoped op
            if (c.type === "date_range" && Array.isArray(c.value)) {
              return { column, op: "between", value: c.value };
            }
            if (c.type === "amount_range" && c.value != null) {
              return { column, op: "gte", value: c.value };
            }
            if (c.type === "text_match" && c.value != null) {
              return { column, op: "like", value: `%${c.value}%` };
            }
            return null;
          })
          .filter(Boolean);
      };

      const loadSide = async (side: "left" | "right", alias: string) => {
        const conditions = buildConditions(side);
        try {
          const res = await fetch(
            `/api/sources/${encodeURIComponent(alias)}/load-scoped`,
            {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ conditions, limit: 200 }),
            },
          );
          if (!res.ok) return;
          const data = await res.json();
          dispatch({
            type: "SET_SAMPLE",
            side,
            data: {
              columns: data.columns,
              rows: data.rows,
              totalRows: data.total_rows,
            },
          });
        } catch {
          // Silently ignore refresh errors
        }
      };

      await Promise.all([
        loadSide("left", sourceConfigLeft.activeAlias),
        loadSide("right", sourceConfigRight.activeAlias),
      ]);
    }, 500);

    return () => clearTimeout(timer);
  }, [filterChips, sourceConfigLeft?.activeAlias, sourceConfigRight?.activeAlias, schemaLeft, schemaRight, dispatch]);

  /* ---------------------------------------------------------------- */
  /*  Toggle sources expansion                                         */
  /* ---------------------------------------------------------------- */

  const handleEditSources = useCallback(() => {
    dispatch({ type: "TOGGLE_SOURCES_EXPANDED" });
  }, [dispatch]);

  /* ---------------------------------------------------------------- */
  /*  Render                                                           */
  /* ---------------------------------------------------------------- */

  return (
    <div className="flex flex-col gap-6">
      {/* Source Configuration */}
      {sourcesExpanded ? (
        <ExpandedSourceCards
          leftAlias={leftAlias}
          rightAlias={rightAlias}
          leftLoaded={sourceConfigLeft?.loaded ?? false}
          rightLoaded={sourceConfigRight?.loaded ?? false}
          onSourceLoaded={handleSourceLoaded}
        />
      ) : sourceConfigLeft && sourceConfigRight ? (
        <CollapsedSourcesBar
          left={sourceConfigLeft}
          right={sourceConfigRight}
          onEdit={handleEditSources}
        />
      ) : null}

      {/* Smart Filter */}
      <SmartFilter
        chips={filterChips}
        onSubmit={handleFilterSubmit}
        onRemoveChip={handleRemoveChip}
        loading={!!loading.nlFilter}
      />

      {/* Sample Preview */}
      <SamplePreviewV2
        sampleLeft={sampleLeft}
        sampleRight={sampleRight}
        leftAlias={leftAlias}
        rightAlias={rightAlias}
      />

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 1 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button
          disabled={!canContinue}
          onClick={() => dispatch({ type: "SET_STEP", step: 3 })}
        >
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
