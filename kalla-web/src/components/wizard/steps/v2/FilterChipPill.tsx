"use client";

import { Calendar, DollarSign, Type, X } from "lucide-react";
import type { FilterChip } from "@/lib/wizard-types";

const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  calendar: Calendar,
  "dollar-sign": DollarSign,
  type: Type,
};

const scopeStyles: Record<string, string> = {
  both: "bg-blue-500 text-white",
  left: "bg-orange-500 text-white",
  right: "bg-violet-500 text-white",
};

interface Props {
  chip: FilterChip;
  onRemove: (id: string) => void;
}

export function FilterChipPill({ chip, onRemove }: Props) {
  const Icon = iconMap[chip.icon] ?? Type;
  const scopeLabel = chip.sourceLabel ?? (chip.scope.charAt(0).toUpperCase() + chip.scope.slice(1));

  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-1 text-[11px] font-medium">
      <Icon className="h-3 w-3 shrink-0 text-muted-foreground" />
      <span
        className={`rounded-full px-1.5 py-0.5 text-[10px] font-semibold leading-none ${scopeStyles[chip.scope] ?? scopeStyles.both}`}
      >
        {scopeLabel}
      </span>
      <span>{chip.label}</span>
      <button
        type="button"
        onClick={() => onRemove(chip.id)}
        aria-label={`Remove ${chip.label}`}
        className="ml-0.5 inline-flex items-center justify-center rounded-full p-0.5 hover:bg-muted-foreground/20"
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
}
