"use client";

import { useState } from "react";
import { Settings, Check } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import type { ColumnInfo } from "@/lib/chat-types";

interface Props {
  columns: ColumnInfo[];
  selected: string[];
  onToggle: (colName: string) => void;
}

export function FieldSelectorPopover({ columns, selected, onToggle }: Props) {
  const [search, setSearch] = useState("");

  const filtered = columns.filter((col) =>
    col.name.toLowerCase().includes(search.toLowerCase()),
  );

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label="Select columns"
          className="inline-flex items-center justify-center rounded-md p-1 text-muted-foreground hover:bg-muted hover:text-foreground"
        >
          <Settings className="h-3.5 w-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-[220px] p-2" align="end">
        <Input
          placeholder="Search columns..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="mb-2 h-7 text-xs"
        />
        <div className="max-h-[200px] overflow-y-auto">
          {filtered.map((col) => {
            const isSelected = selected.includes(col.name);
            return (
              <button
                key={col.name}
                type="button"
                onClick={() => onToggle(col.name)}
                className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs hover:bg-muted"
              >
                <span className="flex-1 truncate text-left">{col.name}</span>
                <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
                  {col.data_type}
                </Badge>
                {isSelected && (
                  <Check className="h-3.5 w-3.5 shrink-0 text-green-500" />
                )}
              </button>
            );
          })}
        </div>
      </PopoverContent>
    </Popover>
  );
}
