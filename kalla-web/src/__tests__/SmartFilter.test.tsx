import { render, screen, fireEvent } from "@testing-library/react";
import { SmartFilter } from "@/components/wizard/steps/v2/SmartFilter";
import type { FilterChip } from "@/lib/wizard-types";

const chips: FilterChip[] = [
  { id: "c1", label: "Last 30 days", icon: "calendar", scope: "both", type: "date_range", value: ["2026-01-01", "2026-01-31"] },
  { id: "c2", label: "Amount > 100", icon: "dollar-sign", scope: "left", type: "amount_range", field_a: "amount", value: "100" },
];

describe("SmartFilter", () => {
  test("renders header with sparkle icon", () => {
    render(<SmartFilter chips={[]} onSubmit={() => {}} onRemoveChip={() => {}} loading={false} />);
    expect(screen.getByText("Smart Filter")).toBeInTheDocument();
  });

  test("renders NL input and submit button", () => {
    render(<SmartFilter chips={[]} onSubmit={() => {}} onRemoveChip={() => {}} loading={false} />);
    expect(screen.getByPlaceholderText(/describe/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /submit/i })).toBeInTheDocument();
  });

  test("renders chips with scope badges", () => {
    render(<SmartFilter chips={chips} onSubmit={() => {}} onRemoveChip={() => {}} loading={false} />);
    expect(screen.getByText("Last 30 days")).toBeInTheDocument();
    expect(screen.getByText("Amount > 100")).toBeInTheDocument();
    expect(screen.getByText("Both")).toBeInTheDocument();
    expect(screen.getByText("Left")).toBeInTheDocument();
  });

  test("calls onSubmit with input text", () => {
    const onSubmit = jest.fn();
    render(<SmartFilter chips={[]} onSubmit={onSubmit} onRemoveChip={() => {}} loading={false} />);
    const input = screen.getByPlaceholderText(/describe/i);
    fireEvent.change(input, { target: { value: "last 30 days" } });
    fireEvent.click(screen.getByRole("button", { name: /submit/i }));
    expect(onSubmit).toHaveBeenCalledWith("last 30 days");
  });

  test("calls onRemoveChip when X clicked", () => {
    const onRemove = jest.fn();
    render(<SmartFilter chips={chips} onSubmit={() => {}} onRemoveChip={onRemove} loading={false} />);
    const removeButtons = screen.getAllByRole("button", { name: /remove/i });
    fireEvent.click(removeButtons[0]);
    expect(onRemove).toHaveBeenCalledWith("c1");
  });
});
