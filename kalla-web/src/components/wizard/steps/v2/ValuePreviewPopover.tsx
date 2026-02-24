"use client";

import { Badge } from "@/components/ui/badge";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import type { ColumnInfo } from "@/lib/chat-types";

interface Props {
  column: ColumnInfo;
  values: string[];
  children: React.ReactNode;
}

export function ValuePreviewPopover({ column, values, children }: Props) {
  const uniqueValues = [...new Set(values)].slice(0, 8);
  const distinctCount = new Set(values).size;

  return (
    <HoverCard openDelay={300} closeDelay={100}>
      <HoverCardTrigger asChild>{children}</HoverCardTrigger>
      <HoverCardContent className="w-[200px] p-3" align="start" side="right">
        <div className="mb-2 flex items-center gap-2">
          <span className="text-xs font-medium">{column.name}</span>
          <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
            {column.data_type}
          </Badge>
        </div>
        <div className="space-y-1">
          {uniqueValues.map((val, i) => (
            <div
              key={`${val}-${i}`}
              className="truncate rounded bg-muted px-2 py-1 text-xs text-muted-foreground"
            >
              {val}
            </div>
          ))}
        </div>
        <div className="mt-2 text-[10px] text-muted-foreground">
          {distinctCount} distinct values
        </div>
      </HoverCardContent>
    </HoverCard>
  );
}
