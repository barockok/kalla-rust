"use client";

import { useState, useCallback } from "react";
import { Rows3, Landmark, FileText } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { SampleData } from "@/lib/wizard-types";
import { FieldSelectorPopover } from "./FieldSelectorPopover";
import { ValuePreviewPopover } from "./ValuePreviewPopover";

const DEFAULT_VISIBLE_COLUMNS = 3;

interface Props {
  sampleLeft: SampleData | null;
  sampleRight: SampleData | null;
  leftAlias: string;
  rightAlias: string;
}

function useColumnSelection(sample: SampleData | null) {
  const [selected, setSelected] = useState<string[] | null>(null);

  const columns = sample?.columns ?? [];
  const defaultCols = columns.slice(0, DEFAULT_VISIBLE_COLUMNS).map((c) => c.name);
  const visibleCols = selected ?? defaultCols;

  const toggle = useCallback(
    (colName: string) => {
      setSelected((prev) => {
        const current = prev ?? defaultCols;
        if (current.includes(colName)) {
          return current.filter((n) => n !== colName);
        }
        return [...current, colName];
      });
    },
    [defaultCols],
  );

  return { visibleCols, toggle, columns };
}

function SampleTable({
  sample,
  alias,
  icon: Icon,
  visibleCols,
  onToggleCol,
}: {
  sample: SampleData;
  alias: string;
  icon: React.ComponentType<{ className?: string }>;
  visibleCols: string[];
  onToggleCol: (colName: string) => void;
}) {
  const visibleColumns = sample.columns.filter((col) =>
    visibleCols.includes(col.name),
  );

  const colIndexMap = new Map(
    sample.columns.map((col, idx) => [col.name, idx]),
  );

  return (
    <div className="flex-1 min-w-0 overflow-hidden rounded-xl border-[1.5px] border-border">
      {/* Table header bar */}
      <div className="flex items-center gap-2 border-b border-border px-3 py-2">
        <Icon className="h-4 w-4 text-muted-foreground" />
        <span className="text-sm font-medium">{alias}</span>
        <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
          {sample.totalRows} rows
        </Badge>
        <div className="ml-auto">
          <FieldSelectorPopover
            columns={sample.columns}
            selected={visibleCols}
            onToggle={onToggleCol}
          />
        </div>
      </div>

      {/* Data table */}
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              {visibleColumns.map((col) => (
                <TableHead key={col.name} className="text-xs">
                  <ValuePreviewPopover
                    column={col}
                    values={sample.rows.map(
                      (row) => row[colIndexMap.get(col.name) ?? 0],
                    )}
                  >
                    <button
                      type="button"
                      className="cursor-pointer hover:text-foreground"
                    >
                      {col.name}
                    </button>
                  </ValuePreviewPopover>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {sample.rows.map((row, rowIdx) => (
              <TableRow key={rowIdx}>
                {visibleColumns.map((col) => (
                  <TableCell
                    key={col.name}
                    className="text-xs text-muted-foreground"
                  >
                    {row[colIndexMap.get(col.name) ?? 0]}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

export function SamplePreviewV2({
  sampleLeft,
  sampleRight,
  leftAlias,
  rightAlias,
}: Props) {
  const leftSelection = useColumnSelection(sampleLeft);
  const rightSelection = useColumnSelection(sampleRight);

  if (!sampleLeft && !sampleRight) return null;

  const leftRowCount = sampleLeft?.totalRows ?? 0;
  const rightRowCount = sampleRight?.totalRows ?? 0;

  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="flex items-center gap-2">
        <Rows3 className="h-4 w-4 text-muted-foreground" />
        <span className="text-sm font-medium">Sample Preview</span>
        <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
          Showing {leftRowCount} + {rightRowCount} rows
        </Badge>
      </div>

      {/* Side-by-side tables */}
      <div className="flex gap-4">
        {sampleLeft && (
          <SampleTable
            sample={sampleLeft}
            alias={leftAlias}
            icon={Landmark}
            visibleCols={leftSelection.visibleCols}
            onToggleCol={leftSelection.toggle}
          />
        )}
        {sampleRight && (
          <SampleTable
            sample={sampleRight}
            alias={rightAlias}
            icon={FileText}
            visibleCols={rightSelection.visibleCols}
            onToggleCol={rightSelection.toggle}
          />
        )}
      </div>
    </div>
  );
}
