"use client";

import { cn } from "@/lib/utils";
import { Check } from "lucide-react";
import { useWizard } from "./wizard-context";
import { WIZARD_STEPS } from "@/lib/wizard-types";

export function StepsSidebar() {
  const { state } = useWizard();
  return (
    <aside className="flex w-[260px] flex-col border-r bg-background px-6 py-8">
      <h2 className="mb-8 text-base font-semibold">New Recipe</h2>
      <div className="flex flex-col">
        {WIZARD_STEPS.map((s, i) => {
          const isCompleted = state.step > s.step;
          const isActive = state.step === s.step;
          const isPending = state.step < s.step;
          const isLast = i === WIZARD_STEPS.length - 1;
          return (
            <div key={s.step} className="flex gap-3">
              <div className="flex flex-col items-center">
                <div className={cn(
                  "flex h-6 w-6 shrink-0 items-center justify-center rounded-full border-2 text-xs",
                  isCompleted && "border-green-500 bg-green-500 text-white",
                  isActive && "border-foreground bg-foreground text-background",
                  isPending && "border-muted-foreground/40 bg-background text-muted-foreground",
                )}>
                  {isCompleted ? <Check className="h-3 w-3" /> : s.step}
                </div>
                {!isLast && (
                  <div className={cn("w-px flex-1 min-h-[40px]", isCompleted ? "bg-green-500" : "bg-border")} />
                )}
              </div>
              <div className="pb-8">
                <p className={cn("text-sm leading-6", isActive ? "font-semibold text-foreground" : "font-medium text-muted-foreground")}>
                  {s.title}
                </p>
                {s.step === 1 && state.leftSource && state.rightSource && (
                  <p className="text-xs text-muted-foreground">
                    {state.leftSource.alias} &amp; {state.rightSource.alias}
                  </p>
                )}
                {s.step !== 1 && s.description && (
                  <p className="text-xs text-muted-foreground">{s.description}</p>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </aside>
  );
}
