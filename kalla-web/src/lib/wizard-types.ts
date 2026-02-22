import type { ColumnInfo, FilterCondition } from "./chat-types";

export type WizardStep = 1 | 2 | 3 | 4 | 5;

export const WIZARD_STEPS = [
  { step: 1 as const, title: "Select Sources", description: "Choose data sources" },
  { step: 2 as const, title: "Sample Data", description: "Pull ~10 transactions" },
  { step: 3 as const, title: "AI Rules", description: "Pattern detection & rules" },
  { step: 4 as const, title: "Run Parameters", description: "Define runtime filters" },
  { step: 5 as const, title: "Review & Save", description: "Confirm and save recipe" },
] as const;

export interface WizardSource {
  alias: string;
  uri: string;
  source_type: string;
}

export interface FieldMapping {
  field_a: string;
  field_b: string;
  confidence: number;
  reason: string;
}

export interface SuggestedFilter {
  type: string;
  field_a: string;
  field_b: string;
}

export interface CommonFilter {
  id: string;
  type: string;
  label: string;
  icon: string;
  field_a: string;
  field_b: string;
  value: [string, string] | null;
}

export interface SampleData {
  columns: ColumnInfo[];
  rows: string[][];
  totalRows: number;
}

export interface WizardState {
  step: WizardStep;
  leftSource: WizardSource | null;
  rightSource: WizardSource | null;
  schemaLeft: ColumnInfo[] | null;
  schemaRight: ColumnInfo[] | null;
  previewLeft: string[][] | null;
  previewRight: string[][] | null;
  fieldMappings: FieldMapping[];
  suggestedFilters: SuggestedFilter[];
  commonFilters: CommonFilter[];
  sourceFiltersLeft: FilterCondition[];
  sourceFiltersRight: FilterCondition[];
  nlFilterText: string;
  nlFilterExplanation: string;
  sampleLeft: SampleData | null;
  sampleRight: SampleData | null;
  loading: Record<string, boolean>;
  errors: Record<string, string | null>;
}

export const INITIAL_WIZARD_STATE: WizardState = {
  step: 1,
  leftSource: null,
  rightSource: null,
  schemaLeft: null,
  schemaRight: null,
  previewLeft: null,
  previewRight: null,
  fieldMappings: [],
  suggestedFilters: [],
  commonFilters: [],
  sourceFiltersLeft: [],
  sourceFiltersRight: [],
  nlFilterText: "",
  nlFilterExplanation: "",
  sampleLeft: null,
  sampleRight: null,
  loading: {},
  errors: {},
};

export type WizardAction =
  | { type: "SET_STEP"; step: WizardStep }
  | { type: "SET_SOURCES"; left: WizardSource; right: WizardSource }
  | { type: "SET_SCHEMAS"; schemaLeft: ColumnInfo[]; schemaRight: ColumnInfo[]; previewLeft: string[][]; previewRight: string[][] }
  | { type: "SET_FIELD_MAPPINGS"; mappings: FieldMapping[]; suggestedFilters: SuggestedFilter[] }
  | { type: "SET_COMMON_FILTERS"; filters: CommonFilter[] }
  | { type: "UPDATE_COMMON_FILTER"; id: string; updates: Partial<CommonFilter> }
  | { type: "SET_NL_TEXT"; text: string }
  | { type: "SET_NL_RESULT"; filters: CommonFilter[]; explanation: string }
  | { type: "SET_SOURCE_FILTERS_LEFT"; filters: FilterCondition[] }
  | { type: "SET_SOURCE_FILTERS_RIGHT"; filters: FilterCondition[] }
  | { type: "SET_SAMPLE"; side: "left" | "right"; data: SampleData }
  | { type: "SET_LOADING"; key: string; value: boolean }
  | { type: "SET_ERROR"; key: string; error: string | null };
