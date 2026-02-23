import { render, screen, fireEvent } from "@testing-library/react";
import { CollapsedSourcesBar } from "@/components/wizard/steps/v2/CollapsedSourcesBar";
import type { SourceConfig } from "@/lib/wizard-types";

const dbConfig: SourceConfig = {
  mode: "db",
  loaded: true,
  originalAlias: "invoices",
  activeAlias: "invoices",
};

const csvConfig: SourceConfig = {
  mode: "csv",
  loaded: true,
  originalAlias: "payments",
  activeAlias: "csv_payments_abc",
  csvFileName: "payments.csv",
  csvRowCount: 150,
  csvColCount: 8,
};

describe("CollapsedSourcesBar", () => {
  test("renders both source pills with names and modes", () => {
    render(<CollapsedSourcesBar left={dbConfig} right={csvConfig} onEdit={() => {}} />);
    expect(screen.getByText(/invoices/i)).toBeInTheDocument();
    expect(screen.getByText(/DB/i)).toBeInTheDocument();
    expect(screen.getByText(/payments/i)).toBeInTheDocument();
    expect(screen.getByText(/CSV/i)).toBeInTheDocument();
  });

  test("shows CSV row count when available", () => {
    render(<CollapsedSourcesBar left={dbConfig} right={csvConfig} onEdit={() => {}} />);
    expect(screen.getByText(/150 rows/i)).toBeInTheDocument();
  });

  test("calls onEdit when Edit button is clicked", () => {
    const onEdit = jest.fn();
    render(<CollapsedSourcesBar left={dbConfig} right={csvConfig} onEdit={onEdit} />);
    fireEvent.click(screen.getByRole("button", { name: /edit/i }));
    expect(onEdit).toHaveBeenCalledTimes(1);
  });
});
