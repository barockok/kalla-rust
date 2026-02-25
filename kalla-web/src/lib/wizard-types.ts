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

export interface SourceConfig {
  mode: "db" | "csv";
  loaded: boolean;
  originalAlias: string;
  activeAlias: string;
  csvFileName?: string;
  csvFileSize?: number;
  csvRowCount?: number;
  csvColCount?: number;
}

export interface FilterChip {
  id: string;
  label: string;
  icon: string;
  scope: "both" | "left" | "right";
  type: string;
  field_a?: string;
  field_b?: string;
  value: [string, string] | string | null;
  /** Original operator from AI (eq, neq, in, like, between, gt, gte, lt, lte) */
  op?: string;
  /** Original value from AI — preserved for array values (in operator) */
  rawValue?: unknown;
}

export interface SampleData {
  columns: ColumnInfo[];
  rows: string[][];
  totalRows: number;
}

export type PatternType = "1:1" | "1:N" | "N:M";

export interface DetectedPattern {
  type: PatternType;
  description: string;
  confidence: number;
}

export interface PrimaryKeys {
  source_a: string[];
  source_b: string[];
}

export interface InferredRule {
  id: string;
  name: string;
  sql: string;
  description: string;
  confidence: number;
  evidence: Record<string, unknown>[];
}

export type RuleStatus = "pending" | "accepted" | "rejected";

export interface RuleWithStatus extends InferredRule {
  status: RuleStatus;
}

export interface MatchPreviewRow {
  left_row: Record<string, unknown>;
  right_rows: Record<string, unknown>[];
  status: "matched" | "unmatched" | "partial";
}

export interface MatchPreviewSummary {
  total_left: number;
  total_right: number;
  matched: number;
  unmatched: number;
}

export interface MatchPreviewResult {
  matches: MatchPreviewRow[];
  summary: MatchPreviewSummary;
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
  detectedPattern: DetectedPattern | null;
  primaryKeys: PrimaryKeys | null;
  inferredRules: RuleWithStatus[];
  builtRecipeSql: string | null;
  runtimeFieldsLeft: string[];
  runtimeFieldsRight: string[];
  recipeName: string;
  matchPreviewResult: MatchPreviewResult | null;
  sourceConfigLeft: SourceConfig | null;
  sourceConfigRight: SourceConfig | null;
  filterChips: FilterChip[];
  sourcesExpanded: boolean;
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
  detectedPattern: null,
  primaryKeys: null,
  inferredRules: [],
  builtRecipeSql: null,
  runtimeFieldsLeft: [],
  runtimeFieldsRight: [],
  recipeName: "",
  matchPreviewResult: null,
  sourceConfigLeft: null,
  sourceConfigRight: null,
  filterChips: [],
  sourcesExpanded: true,
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
  | { type: "SET_ERROR"; key: string; error: string | null }
  | { type: "SET_INFERRED_RULES"; pattern: DetectedPattern; primaryKeys: PrimaryKeys; rules: RuleWithStatus[] }
  | { type: "ACCEPT_RULE"; id: string }
  | { type: "REJECT_RULE"; id: string }
  | { type: "ADD_CUSTOM_RULE"; rule: RuleWithStatus }
  | { type: "SET_RECIPE_SQL"; sql: string }
  | { type: "TOGGLE_RUNTIME_FIELD"; side: "left" | "right"; field: string }
  | { type: "SET_RECIPE_NAME"; name: string }
  | { type: "SET_MATCH_PREVIEW"; result: MatchPreviewResult }
  | { type: "SET_SOURCE_CONFIG"; side: "left" | "right"; config: SourceConfig }
  | { type: "SET_FILTER_CHIPS"; chips: FilterChip[] }
  | { type: "REMOVE_FILTER_CHIP"; chipId: string }
  | { type: "TOGGLE_SOURCES_EXPANDED" };
