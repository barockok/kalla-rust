import { z } from "zod";
import {
  SourceSchemaSchema,
  FieldMappingSchema,
  SuggestedFilterSchema,
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
