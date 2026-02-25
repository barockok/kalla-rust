"use client";

import { useState, useEffect, useRef } from "react";
import { Sparkles, ArrowRight, Loader2, AlertCircle } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { FilterChipPill } from "./FilterChipPill";
import type { FilterChip } from "@/lib/wizard-types";

interface Props {
  chips: FilterChip[];
  onSubmit: (text: string) => void;
  onRemoveChip: (chipId: string) => void;
  loading: boolean;
  error: string | null;
}

export function SmartFilter({ chips, onSubmit, onRemoveChip, loading, error }: Props) {
  const [text, setText] = useState("");
  const prevLoadingRef = useRef(loading);

  // Clear text only when loading transitions from true → false with no error
  useEffect(() => {
    if (prevLoadingRef.current && !loading && !error) {
      setText("");
    }
    prevLoadingRef.current = loading;
  }, [loading, error]);

  function handleSubmit() {
    const trimmed = text.trim();
    if (!trimmed || loading) return;
    onSubmit(trimmed);
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
        Describe the rows you want to keep and we&apos;ll build the filter for you.
      </p>

      {/* Error message */}
      {error && (
        <div className="mt-2 flex items-center gap-1.5 text-[12px] text-destructive">
          <AlertCircle className="h-3.5 w-3.5 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* Input row */}
      <div className="mt-3 flex items-center gap-2">
        <div className="relative flex-1">
          {loading ? (
            <Loader2 className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 animate-spin text-primary" />
          ) : (
            <Sparkles className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          )}
          <Input
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Describe your filter..."
            readOnly={loading}
            className={`pl-8 text-sm ${loading ? "opacity-70" : ""}`}
          />
        </div>
        <Button
          variant="default"
          size="icon"
          onClick={handleSubmit}
          disabled={loading || !text.trim()}
          aria-label="Submit filter"
        >
          {loading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <ArrowRight className="h-4 w-4" />
          )}
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
