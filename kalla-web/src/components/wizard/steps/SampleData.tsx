"use client";

import { useWizard } from "@/components/wizard/wizard-context";
import { Button } from "@/components/ui/button";
import { FilterCard } from "./FilterCard";
import { ArrowLeft, ArrowRight, Landmark, FileText } from "lucide-react";

export function SampleData() {
  const { state, dispatch } = useWizard();

  const aliasA = state.leftSource?.alias ?? "Source A";
  const aliasB = state.rightSource?.alias ?? "Source B";
  const canContinue = state.sampleLeft !== null && state.sampleRight !== null;

  return (
    <div className="flex flex-col gap-6">
      {/* Source summary bar */}
      <div className="flex items-center gap-3">
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-3 py-1 text-sm font-medium">
          <Landmark className="h-3.5 w-3.5" />
          {aliasA}
        </span>
        <ArrowRight className="h-4 w-4 text-muted-foreground" />
        <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-3 py-1 text-sm font-medium">
          <FileText className="h-3.5 w-3.5" />
          {aliasB}
        </span>
      </div>

      {/* Filter card */}
      <FilterCard />

      {/* Sample preview — added in Task 6 */}

      {/* Footer: Back / Continue */}
      <div className="flex justify-between border-t pt-6">
        <Button
          variant="outline"
          onClick={() => dispatch({ type: "SET_STEP", step: 1 })}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button
          disabled={!canContinue}
          onClick={() => dispatch({ type: "SET_STEP", step: 3 })}
        >
          Continue
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
