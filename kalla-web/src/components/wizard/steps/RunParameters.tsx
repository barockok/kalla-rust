"use client";

import { useEffect } from "react";
import { useWizard } from "@/components/wizard/wizard-context";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  ArrowRight,
  Calendar,
  DollarSign,
  Type,
  SlidersHorizontal,
} from "lucide-react";
import type { ColumnInfo } from "@/lib/chat-types";

function filterIcon(dataType: string) {
  const t = dataType.toLowerCase();
  if (t.includes("date") || t.includes("timestamp")) return <Calendar className="h-3.5 w-3.5 text-muted-foreground" />;
  if (t.includes("decimal") || t.includes("numeric") || t.includes("float") || t.includes("int") || t.includes("money"))
    return <DollarSign className="h-3.5 w-3.5 text-muted-foreground" />;
  return <Type className="h-3.5 w-3.5 text-muted-foreground" />;
}

function sampleValue(
  fieldName: string,
  schema: ColumnInfo[],
  previewRows: string[][] | null,
): string {
  if (!previewRows || previewRows.length === 0) return "—";
  const idx = schema.findIndex((c) => c.name === fieldName);
  if (idx === -1) return "—";
  return previewRows[0][idx] ?? "—";
}

/* ── RuntimeFieldCard ────────────────────── */
function RuntimeFieldCard({
  side,
  sourceName,
  schema,
  previewRows,
  selectedFields,
}: {
  side: "left" | "right";
  sourceName: string;
  schema: ColumnInfo[];
  previewRows: string[][] | null;
  selectedFields: string[];
}) {
  const { dispatch } = useWizard();

  return (
    <div className="rounded-xl border-[1.5px] border-border">
      <div className="flex items-center justify-between px-6 py-4 border-b">
        <div className="flex items-center gap-2">
          <SlidersHorizontal className="h-4 w-4 text-muted-foreground" />
          <h3 className="text-sm font-semibold">{sourceName}</h3>
        </div>
        <Badge variant="outline" className="text-xs">
          {selectedFields.length} field{selectedFields.length !== 1 ? "s" : ""} selected
        </Badge>
      </div>
      <p className="px-6 pt-3 text-[13px] text-muted-foreground">
        Select fields that users can filter on when running this recipe.
      </p>
      <div className="overflow-x-auto px-3 pb-4 pt-3">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left">
              <th className="px-3 py-2 w-10"></th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Field Name</th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Type</th>
              <th className="px-3 py-2 font-medium text-muted-foreground">Sample Value</th>
              <th className="px-3 py-2 font-medium text-muted-foreground w-10">Filter</th>
            </tr>
          </thead>
          <tbody>
            {schema.map((col) => (
              <tr key={col.name} className="border-b last:border-0 hover:bg-muted/30">
                <td className="px-3 py-2">
                  <input
                    type="checkbox"
                    checked={selectedFields.includes(col.name)}
                    onChange={() => dispatch({ type: "TOGGLE_RUNTIME_FIELD", side, field: col.name })}
                    className="h-4 w-4 rounded border-gray-300"
                  />
                </td>
                <td className="px-3 py-2 font-mono text-[13px]">{col.name}</td>
                <td className="px-3 py-2 text-muted-foreground text-[13px]">{col.data_type}</td>
                <td className="px-3 py-2 text-[13px] truncate max-w-[200px]">
                  {sampleValue(col.name, schema, previewRows)}
                </td>
                <td className="px-3 py-2 text-center">{filterIcon(col.data_type)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ── RunParameters (parent) ──────────────── */
export function RunParameters() {
  const { state, dispatch } = useWizard();

  // Pre-check fields from suggestedFilters on mount
  useEffect(() => {
    if (state.runtimeFieldsLeft.length > 0 || state.runtimeFieldsRight.length > 0) return;
    for (const sf of state.suggestedFilters) {
      if (sf.field_a) dispatch({ type: "TOGGLE_RUNTIME_FIELD", side: "left", field: sf.field_a });
      if (sf.field_b) dispatch({ type: "TOGGLE_RUNTIME_FIELD", side: "right", field: sf.field_b });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-[22px] font-semibold">Run Parameters</h1>
        <p className="text-sm text-muted-foreground">
          Select which fields become runtime filters when executing this recipe.
        </p>
      </div>

      {/* Source A field grid */}
      {state.schemaLeft && (
        <RuntimeFieldCard
          side="left"
          sourceName={state.leftSource?.alias ?? "Source A"}
          schema={state.schemaLeft}
          previewRows={state.previewLeft}
          selectedFields={state.runtimeFieldsLeft}
        />
      )}

      {/* Source B field grid */}
      {state.schemaRight && (
        <RuntimeFieldCard
          side="right"
          sourceName={state.rightSource?.alias ?? "Source B"}
          schema={state.schemaRight}
          previewRows={state.previewRight}
          selectedFields={state.runtimeFieldsRight}
        />
      )}

      {/* Footer */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 3 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button onClick={() => dispatch({ type: "SET_STEP", step: 5 })}>
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
