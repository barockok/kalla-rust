"use client";

import { AppSidebar } from "./AppSidebar";
import { StepsSidebar } from "./StepsSidebar";
import type { ReactNode } from "react";

export function WizardShell({ children }: { children: ReactNode }) {
  return (
    <div className="fixed inset-0 z-50 flex bg-background">
      <AppSidebar />
      <StepsSidebar />
      <main className="flex-1 overflow-y-auto">{children}</main>
    </div>
  );
}
