"use client";

import { WizardProvider, useWizard } from "@/components/wizard/wizard-context";
import { WizardShell } from "@/components/wizard/WizardShell";
import { SelectSources } from "@/components/wizard/steps/SelectSources";

function WizardContent() {
  const { state } = useWizard();
  return (
    <div className="px-12 py-10">
      {state.step === 1 && (
        <div>
          <h1 className="text-[22px] font-semibold">Select Sources</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Choose the two data sources to reconcile.
          </p>
          <div className="mt-6">
            <SelectSources />
          </div>
        </div>
      )}
      {state.step === 2 && (
        <div>
          <h1 className="text-[22px] font-semibold">Sample Data</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Narrow down transactions to build a representative sample for AI analysis.
          </p>
        </div>
      )}
    </div>
  );
}

export default function NewRecipePage() {
  return (
    <WizardProvider>
      <WizardShell>
        <WizardContent />
      </WizardShell>
    </WizardProvider>
  );
}
