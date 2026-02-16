export type SourceType = 'postgres' | 'bigquery' | 'elasticsearch' | 'file' | 'csv_upload';

export interface RecipeSource {
  alias: string;
  type: SourceType;
  uri?: string;           // for persistent sources
  schema?: string[];      // expected columns (required for file sources)
  primary_key: string[];
}

export interface RecipeSources {
  left: RecipeSource;
  right: RecipeSource;
}

export interface Recipe {
  recipe_id: string;
  name: string;
  description: string;
  match_sql: string;
  match_description: string;
  sources: RecipeSources;
}

export interface JobPayload {
  run_id: string;
  callback_url: string;
  match_sql: string;
  sources: ResolvedSource[];
  output_path: string;
  primary_keys: Record<string, string[]>;
}

export interface ResolvedSource {
  alias: string;
  uri: string;  // always resolved at execution time
}

export interface WorkerProgress {
  run_id: string;
  stage: 'staging' | 'matching' | 'writing_results';
  source?: string;
  progress?: number;
  matched_count?: number;
  total_left?: number;
  total_right?: number;
}

export interface WorkerComplete {
  run_id: string;
  matched_count: number;
  unmatched_left_count: number;
  unmatched_right_count: number;
  output_paths: {
    matched: string;
    unmatched_left: string;
    unmatched_right: string;
  };
}

export interface WorkerError {
  run_id: string;
  error: string;
  stage: string;
}
