import type { SourceConfig, FilterChip } from "@/lib/wizard-types";
import { renderHook, act } from "@testing-library/react";
import { WizardProvider, useWizard } from "@/components/wizard/wizard-context";
import type { ReactNode } from "react";

function wrapper({ children }: { children: ReactNode }) {
  return <WizardProvider>{children}</WizardProvider>;
}

describe("V2 reducer cases", () => {
  test("SET_SOURCE_CONFIG sets left config", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const config: SourceConfig = {
      mode: "db",
      loaded: true,
      originalAlias: "invoices",
      activeAlias: "invoices",
    };
    act(() => result.current.dispatch({ type: "SET_SOURCE_CONFIG", side: "left", config }));
    expect(result.current.state.sourceConfigLeft).toEqual(config);
    expect(result.current.state.sourceConfigRight).toBeNull();
  });

  test("SET_SOURCE_CONFIG sets right config", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const config: SourceConfig = {
      mode: "csv",
      loaded: true,
      originalAlias: "payments",
      activeAlias: "tmp_pay_abc",
      csvFileName: "payments.csv",
      csvFileSize: 2048,
      csvRowCount: 100,
      csvColCount: 5,
    };
    act(() => result.current.dispatch({ type: "SET_SOURCE_CONFIG", side: "right", config }));
    expect(result.current.state.sourceConfigRight).toEqual(config);
  });

  test("SET_FILTER_CHIPS replaces all chips", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const chips: FilterChip[] = [
      { id: "c1", label: "Last 30 days", icon: "calendar", scope: "both", type: "date_range", value: ["2026-01-01", "2026-01-31"] },
      { id: "c2", label: "Amount > 100", icon: "dollar-sign", scope: "left", type: "amount_range", field_a: "amount", value: "100" },
    ];
    act(() => result.current.dispatch({ type: "SET_FILTER_CHIPS", chips }));
    expect(result.current.state.filterChips).toHaveLength(2);
    expect(result.current.state.filterChips[0].label).toBe("Last 30 days");
  });

  test("REMOVE_FILTER_CHIP removes by id", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    const chips: FilterChip[] = [
      { id: "c1", label: "Chip 1", icon: "calendar", scope: "both", type: "date_range", value: null },
      { id: "c2", label: "Chip 2", icon: "type", scope: "right", type: "text_match", value: "test" },
    ];
    act(() => result.current.dispatch({ type: "SET_FILTER_CHIPS", chips }));
    act(() => result.current.dispatch({ type: "REMOVE_FILTER_CHIP", chipId: "c1" }));
    expect(result.current.state.filterChips).toHaveLength(1);
    expect(result.current.state.filterChips[0].id).toBe("c2");
  });

  test("TOGGLE_SOURCES_EXPANDED flips boolean", () => {
    const { result } = renderHook(() => useWizard(), { wrapper });
    expect(result.current.state.sourcesExpanded).toBe(true);
    act(() => result.current.dispatch({ type: "TOGGLE_SOURCES_EXPANDED" }));
    expect(result.current.state.sourcesExpanded).toBe(false);
    act(() => result.current.dispatch({ type: "TOGGLE_SOURCES_EXPANDED" }));
    expect(result.current.state.sourcesExpanded).toBe(true);
  });
});
