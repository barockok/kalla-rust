"use client";

import { Landmark, FileText, CheckCircle2, PencilLine } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { SourceConfig } from "@/lib/wizard-types";

interface Props {
  left: SourceConfig;
  right: SourceConfig;
  onEdit: () => void;
}

function SourcePill({ config }: { config: SourceConfig }) {
  const isDb = config.mode === "db";
  const Icon = isDb ? Landmark : FileText;
  const modeLabel = isDb ? "DB" : "CSV";

  const parts = [config.originalAlias, modeLabel];
  if (!isDb && config.csvRowCount != null) {
    parts.push(`${config.csvRowCount} rows`);
  }

  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-border bg-muted/50 px-3 py-1 text-sm">
      <Icon className="h-3.5 w-3.5 text-muted-foreground" />
      <span>{parts.join(" \u00b7 ")}</span>
      <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
    </span>
  );
}

export function CollapsedSourcesBar({ left, right, onEdit }: Props) {
  return (
    <div className="flex items-center justify-between rounded-[10px] border-[1.5px] border-border px-4 py-3">
      <div className="flex items-center gap-2">
        <SourcePill config={left} />
        <SourcePill config={right} />
      </div>
      <Button
        variant="ghost"
        size="sm"
        onClick={onEdit}
        aria-label="Edit"
        className="gap-1.5 text-muted-foreground hover:text-foreground"
      >
        <PencilLine className="h-4 w-4" />
        Edit
      </Button>
    </div>
  );
}
