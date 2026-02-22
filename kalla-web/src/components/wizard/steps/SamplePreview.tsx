"use client";

import { useWizard } from "@/components/wizard/wizard-context";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Rows3, Landmark, FileText, Settings } from "lucide-react";
import type { SampleData } from "@/lib/wizard-types";

interface SourceTableProps {
  title: string;
  icon: typeof Landmark;
  data: SampleData;
}

function SourceTable({ title, icon: Icon, data }: SourceTableProps) {
  const displayCols = data.columns.slice(0, 3);
  const colIndices = displayCols.map((c) =>
    data.columns.findIndex((col) => col.name === c.name),
  );

  return (
    <div className="flex-1 overflow-hidden rounded-xl border-[1.5px]">
      <div className="flex items-center justify-between border-b bg-background px-4 py-2.5">
        <div className="flex items-center gap-2">
          <Icon className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-semibold">{title}</span>
          <Badge variant="secondary" className="text-[10px]">
            {data.rows.length} rows
          </Badge>
        </div>
        <button className="text-muted-foreground hover:text-foreground">
          <Settings className="h-3.5 w-3.5" />
        </button>
      </div>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              {displayCols.map((col) => (
                <TableHead key={col.name} className="text-xs font-medium">
                  {col.name}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.rows.map((row, rowIdx) => (
              <TableRow key={rowIdx}>
                {colIndices.map((colIdx) => (
                  <TableCell key={colIdx} className="text-xs">
                    {row[colIdx] ?? ""}
                  </TableCell>
                ))}
              </TableRow>
            ))}
            {data.rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={displayCols.length} className="py-8 text-center text-xs text-muted-foreground">
                  No data loaded
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

export function SamplePreview() {
  const { state } = useWizard();
  const { sampleLeft, sampleRight, leftSource, rightSource } = state;

  if (!sampleLeft && !sampleRight) return null;

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Rows3 className="h-4 w-4" />
          <span className="text-[15px] font-semibold">Sample Preview</span>
        </div>
        {(sampleLeft || sampleRight) && (
          <Badge variant="secondary" className="text-[10px]">
            Showing {sampleLeft?.rows.length || 0} + {sampleRight?.rows.length || 0} rows
          </Badge>
        )}
      </div>
      <div className="flex gap-4">
        {sampleLeft && leftSource && (
          <SourceTable title={leftSource.alias} icon={Landmark} data={sampleLeft} />
        )}
        {sampleRight && rightSource && (
          <SourceTable title={rightSource.alias} icon={FileText} data={sampleRight} />
        )}
      </div>
    </div>
  );
}
