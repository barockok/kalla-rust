import type { SourceConfig, FilterChip, WizardState, WizardAction } from "@/lib/wizard-types";
import { INITIAL_WIZARD_STATE } from "@/lib/wizard-types";

describe("V2 wizard types", () => {
  test("SourceConfig type is usable", () => {
    const config: SourceConfig = {
      mode: "db",
      loaded: true,
      originalAlias: "invoices",
      activeAlias: "invoices",
    };
    expect(config.mode).toBe("db");
    expect(config.loaded).toBe(true);
  });

  test("SourceConfig csv mode has optional fields", () => {
    const config: SourceConfig = {
      mode: "csv",
      loaded: true,
      originalAlias: "payments",
      activeAlias: "tmp_payments_abc123",
      csvFileName: "payments.csv",
      csvFileSize: 1024,
      csvRowCount: 50,
      csvColCount: 8,
    };
    expect(config.csvFileName).toBe("payments.csv");
  });

  test("FilterChip type is usable", () => {
    const chip: FilterChip = {
      id: "chip-1",
      label: "Last 30 days",
      icon: "calendar",
      scope: "both",
      type: "date_range",
      field_a: "date",
      field_b: "txn_date",
      value: ["2026-01-01", "2026-01-31"],
    };
    expect(chip.scope).toBe("both");
  });

  test("INITIAL_WIZARD_STATE has v2 fields", () => {
    expect(INITIAL_WIZARD_STATE.sourceConfigLeft).toBeNull();
    expect(INITIAL_WIZARD_STATE.sourceConfigRight).toBeNull();
    expect(INITIAL_WIZARD_STATE.filterChips).toEqual([]);
    expect(INITIAL_WIZARD_STATE.sourcesExpanded).toBe(true);
  });

  test("WizardAction union includes v2 actions", () => {
    const a1: WizardAction = { type: "SET_SOURCE_CONFIG", side: "left", config: { mode: "db", loaded: false, originalAlias: "a", activeAlias: "a" } };
    const a2: WizardAction = { type: "SET_FILTER_CHIPS", chips: [] };
    const a3: WizardAction = { type: "REMOVE_FILTER_CHIP", chipId: "c1" };
    const a4: WizardAction = { type: "TOGGLE_SOURCES_EXPANDED" };
    expect(a1.type).toBe("SET_SOURCE_CONFIG");
    expect(a2.type).toBe("SET_FILTER_CHIPS");
    expect(a3.type).toBe("REMOVE_FILTER_CHIP");
    expect(a4.type).toBe("TOGGLE_SOURCES_EXPANDED");
  });
});
