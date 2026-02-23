import { render, screen } from "@testing-library/react";
import { SampleDataV2 } from "@/components/wizard/steps/v2/SampleDataV2";
import { WizardProvider } from "@/components/wizard/wizard-context";
import type { ReactNode } from "react";

jest.mock("@/components/wizard/steps/v2/ExpandedSourceCards", () => ({
  ExpandedSourceCards: (props: Record<string, unknown>) => (
    <div data-testid="expanded-cards">{String(props.leftAlias)}</div>
  ),
}));
jest.mock("@/components/wizard/steps/v2/CollapsedSourcesBar", () => ({
  CollapsedSourcesBar: () => <div data-testid="collapsed-bar" />,
}));
jest.mock("@/components/wizard/steps/v2/SmartFilter", () => ({
  SmartFilter: () => <div data-testid="smart-filter" />,
}));
jest.mock("@/components/wizard/steps/v2/SamplePreviewV2", () => ({
  SamplePreviewV2: () => <div data-testid="sample-preview" />,
}));

global.fetch = jest.fn();
jest.mock("@/lib/ai-client", () => ({
  callAI: jest.fn(),
}));

function wrapper({ children }: { children: ReactNode }) {
  return <WizardProvider>{children}</WizardProvider>;
}

describe("SampleDataV2", () => {
  test("renders expanded cards when sourcesExpanded is true (default)", () => {
    render(<SampleDataV2 />, { wrapper });
    expect(screen.getByTestId("expanded-cards")).toBeInTheDocument();
    expect(screen.queryByTestId("collapsed-bar")).not.toBeInTheDocument();
  });

  test("renders Smart Filter section", () => {
    render(<SampleDataV2 />, { wrapper });
    expect(screen.getByTestId("smart-filter")).toBeInTheDocument();
  });

  test("renders Back and Continue buttons", () => {
    render(<SampleDataV2 />, { wrapper });
    expect(screen.getByText("Back")).toBeInTheDocument();
    expect(screen.getByText("Continue")).toBeInTheDocument();
  });

  test("Continue button is disabled when no samples loaded", () => {
    render(<SampleDataV2 />, { wrapper });
    const continueBtn = screen.getByText("Continue").closest("button");
    expect(continueBtn).toBeDisabled();
  });
});
