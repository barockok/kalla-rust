import { render, screen } from "@testing-library/react";
import { SamplePreviewV2 } from "@/components/wizard/steps/v2/SamplePreviewV2";
import type { SampleData } from "@/lib/wizard-types";

const sampleLeft: SampleData = {
  columns: [
    { name: "id", data_type: "integer", nullable: false },
    { name: "date", data_type: "date", nullable: false },
    { name: "amount", data_type: "numeric", nullable: true },
    { name: "description", data_type: "text", nullable: true },
  ],
  rows: [
    ["1", "2026-01-01", "100.00", "Payment A"],
    ["2", "2026-01-02", "250.50", "Payment B"],
  ],
  totalRows: 2,
};

const sampleRight: SampleData = {
  columns: [
    { name: "txn_id", data_type: "integer", nullable: false },
    { name: "txn_date", data_type: "date", nullable: false },
    { name: "value", data_type: "numeric", nullable: true },
  ],
  rows: [
    ["101", "2026-01-01", "100.00"],
    ["102", "2026-01-03", "300.00"],
  ],
  totalRows: 2,
};

describe("SamplePreviewV2", () => {
  test("renders header with row count badge", () => {
    render(
      <SamplePreviewV2
        sampleLeft={sampleLeft}
        sampleRight={sampleRight}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    expect(screen.getByText("Sample Preview")).toBeInTheDocument();
    expect(screen.getByText(/2 \+ 2 rows/i)).toBeInTheDocument();
  });

  test("renders side-by-side tables with source names", () => {
    render(
      <SamplePreviewV2
        sampleLeft={sampleLeft}
        sampleRight={sampleRight}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    expect(screen.getByText("invoices")).toBeInTheDocument();
    expect(screen.getByText("payments")).toBeInTheDocument();
  });

  test("shows first 3 columns by default", () => {
    render(
      <SamplePreviewV2
        sampleLeft={sampleLeft}
        sampleRight={sampleRight}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    expect(screen.getByText("id")).toBeInTheDocument();
    expect(screen.getByText("date")).toBeInTheDocument();
    expect(screen.getByText("amount")).toBeInTheDocument();
    // "description" is the 4th column, should NOT be visible by default
    expect(screen.queryByText("description")).not.toBeInTheDocument();
  });

  test("returns null when no data", () => {
    const { container } = render(
      <SamplePreviewV2
        sampleLeft={null}
        sampleRight={null}
        leftAlias="invoices"
        rightAlias="payments"
      />,
    );
    expect(container.innerHTML).toBe("");
  });
});
