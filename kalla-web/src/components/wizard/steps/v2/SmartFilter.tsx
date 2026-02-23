"use client";

import { useState } from "react";
import { Sparkles, ArrowRight } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { FilterChipPill } from "./FilterChipPill";
import type { FilterChip } from "@/lib/wizard-types";

interface Props {
  chips: FilterChip[];
  onSubmit: (text: string) => void;
  onRemoveChip: (chipId: string) => void;
  loading: boolean;
}

export function SmartFilter({ chips, onSubmit, onRemoveChip, loading }: Props) {
  const [text, setText] = useState("");

  function handleSubmit() {
    const trimmed = text.trim();
    if (!trimmed || loading) return;
    onSubmit(trimmed);
    setText("");
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter") {
      e.preventDefault();
      handleSubmit();
    }
  }

  return (
    <div className="rounded-xl border-[1.5px] p-4">
      {/* Header */}
      <div className="flex items-center gap-2">
        <Sparkles className="h-4 w-4 text-primary" />
        <span className="text-sm font-semibold">Smart Filter</span>
      </div>

      {/* Description */}
      <p className="mt-1 text-[13px] text-muted-foreground">
        Describe the rows you want to keep and we'll build the filter for you.
      </p>

      {/* Input row */}
      <div className="mt-3 flex items-center gap-2">
        <div className="relative flex-1">
          <Sparkles className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Describe your filter..."
            className="pl-8 text-sm"
          />
        </div>
        <Button
          variant="default"
          size="icon"
          onClick={handleSubmit}
          disabled={loading || !text.trim()}
          aria-label="Submit filter"
        >
          <ArrowRight className="h-4 w-4" />
        </Button>
      </div>

      {/* Chips row */}
      {chips.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-2">
          {chips.map((chip) => (
            <FilterChipPill key={chip.id} chip={chip} onRemove={onRemoveChip} />
          ))}
        </div>
      )}
    </div>
  );
}
