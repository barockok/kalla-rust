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

interface ParsedFilter {
  id: string;
  label: string;
  icon: string;
  scope: "both" | "left" | "right";
  type: string;
  field_a?: string;
  field_b?: string;
  value: [string, string] | string | null;
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
        const result = await callAI<ParsedFilter[]>("parse_nl_filter", {
          text,
          schema_a: { alias: leftAlias, columns: schemaLeft },
          schema_b: { alias: rightAlias, columns: schemaRight },
          current_mappings: fieldMappings,
        });
        const chips: FilterChip[] = result.map((f) => ({
          id: f.id,
          label: f.label,
          icon: f.icon,
          scope: f.scope,
          type: f.type,
          field_a: f.field_a,
          field_b: f.field_b,
          value: f.value,
        }));
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
    [dispatch, schemaLeft, schemaRight, fieldMappings, filterChips],
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
      const buildConditions = (side: "left" | "right") =>
        filterChips
          .filter((c) => c.scope === "both" || c.scope === side)
          .map((c) => ({
            type: c.type,
            field: side === "left" ? c.field_a : c.field_b,
            value: c.value,
          }))
          .filter((c) => c.field);

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
  }, [filterChips, sourceConfigLeft?.activeAlias, sourceConfigRight?.activeAlias, dispatch]);

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
