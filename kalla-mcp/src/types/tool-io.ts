import { z } from "zod";
import {
  SourceSchemaSchema,
  FieldMappingSchema,
  SuggestedFilterSchema,
  DetectedPatternSchema,
  PrimaryKeysSchema,
  InferredRuleSchema,
  PatternTypeSchema,
} from "./schemas.js";

// ── detect_field_mappings ─────────────────────────────
export const DetectFieldMappingsInputSchema = z.object({
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  sample_a: z.array(z.record(z.unknown())).optional(),
  sample_b: z.array(z.record(z.unknown())).optional(),
});
export type DetectFieldMappingsInput = z.infer<typeof DetectFieldMappingsInputSchema>;

export const DetectFieldMappingsOutputSchema = z.object({
  mappings: z.array(FieldMappingSchema),
  suggested_filters: z.array(SuggestedFilterSchema),
});
export type DetectFieldMappingsOutput = z.infer<typeof DetectFieldMappingsOutputSchema>;

// ── parse_nl_filter ───────────────────────────────────
export const ParseNlFilterInputSchema = z.object({
  text: z.string(),
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  current_mappings: z.array(FieldMappingSchema),
});
export type ParseNlFilterInput = z.infer<typeof ParseNlFilterInputSchema>;

export const SourceFilterSchema = z.object({
  source: z.string(),
  column: z.string(),
  op: z.string(),
  value: z.union([z.string(), z.number(), z.array(z.string()), z.tuple([z.string(), z.string()])]),
});

export const ParseNlFilterOutputSchema = z.object({
  filters: z.array(SourceFilterSchema),
  explanation: z.string(),
});
export type ParseNlFilterOutput = z.infer<typeof ParseNlFilterOutputSchema>;

// ── infer_rules ───────────────────────────────────────
export const InferRulesInputSchema = z.object({
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  sample_a: z.array(z.record(z.unknown())),
  sample_b: z.array(z.record(z.unknown())),
  mappings: z.array(FieldMappingSchema),
});
export type InferRulesInput = z.infer<typeof InferRulesInputSchema>;

export const InferRulesOutputSchema = z.object({
  pattern: DetectedPatternSchema,
  primary_keys: PrimaryKeysSchema,
  rules: z.array(InferredRuleSchema),
});
export type InferRulesOutput = z.infer<typeof InferRulesOutputSchema>;

// ── build_recipe ──────────────────────────────────────
export const BuildRecipeInputSchema = z.object({
  rules: z.array(z.object({
    name: z.string(),
    sql: z.string(),
    description: z.string(),
  })),
  sources: z.object({
    alias_a: z.string(),
    alias_b: z.string(),
  }),
  primary_keys: PrimaryKeysSchema,
  pattern_type: PatternTypeSchema,
});
export type BuildRecipeInput = z.infer<typeof BuildRecipeInputSchema>;

export const BuildRecipeOutputSchema = z.object({
  match_sql: z.string(),
  explanation: z.string(),
});
export type BuildRecipeOutput = z.infer<typeof BuildRecipeOutputSchema>;

// ── nl_to_sql ─────────────────────────────────────────
export const NlToSqlInputSchema = z.object({
  text: z.string(),
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  mappings: z.array(FieldMappingSchema),
});
export type NlToSqlInput = z.infer<typeof NlToSqlInputSchema>;

export const NlToSqlOutputSchema = z.object({
  name: z.string(),
  sql: z.string(),
  description: z.string(),
  confidence: z.number().min(0).max(1),
});
export type NlToSqlOutput = z.infer<typeof NlToSqlOutputSchema>;

// ── preview_match ────────────────────────────────────
export const PreviewMatchInputSchema = z.object({
  match_sql: z.string(),
  sample_a: z.array(z.record(z.unknown())),
  sample_b: z.array(z.record(z.unknown())),
  schema_a: SourceSchemaSchema,
  schema_b: SourceSchemaSchema,
  primary_keys: PrimaryKeysSchema,
  rules: z.array(z.object({
    name: z.string(),
    sql: z.string(),
    description: z.string(),
  })),
});
export type PreviewMatchInput = z.infer<typeof PreviewMatchInputSchema>;

export const MatchPreviewRowSchema = z.object({
  left_row: z.record(z.unknown()),
  right_rows: z.array(z.record(z.unknown())),
  status: z.enum(["matched", "unmatched", "partial"]),
});

export const MatchPreviewSummarySchema = z.object({
  total_left: z.number(),
  total_right: z.number(),
  matched: z.number(),
  unmatched: z.number(),
});

export const PreviewMatchOutputSchema = z.object({
  matches: z.array(MatchPreviewRowSchema),
  summary: MatchPreviewSummarySchema,
});
export type PreviewMatchOutput = z.infer<typeof PreviewMatchOutputSchema>;
