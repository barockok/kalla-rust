import { z } from "zod";

export const ColumnInfoSchema = z.object({
  name: z.string(),
  data_type: z.string(),
  nullable: z.boolean().optional(),
});
export type ColumnInfo = z.infer<typeof ColumnInfoSchema>;

export const SourceSchemaSchema = z.object({
  alias: z.string(),
  columns: z.array(ColumnInfoSchema),
});
export type SourceSchema = z.infer<typeof SourceSchemaSchema>;

export const FieldMappingSchema = z.object({
  field_a: z.string(),
  field_b: z.string(),
  confidence: z.number().min(0).max(1),
  reason: z.string(),
});
export type FieldMapping = z.infer<typeof FieldMappingSchema>;

export const FilterOpSchema = z.enum([
  "eq", "neq", "gt", "gte", "lt", "lte", "between", "in", "like",
]);
export type FilterOp = z.infer<typeof FilterOpSchema>;

export const FilterConditionSchema = z.object({
  column: z.string(),
  op: FilterOpSchema,
  value: z.union([z.string(), z.number(), z.array(z.string()), z.tuple([z.string(), z.string()])]),
});
export type FilterCondition = z.infer<typeof FilterConditionSchema>;

export const SuggestedFilterSchema = z.object({
  type: z.enum(["date_range", "amount_range", "select"]),
  field_a: z.string(),
  field_b: z.string(),
});
export type SuggestedFilter = z.infer<typeof SuggestedFilterSchema>;
