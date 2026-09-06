export interface UserSession {
  username: string;
  display_name: string;
  email: string | null;
  groups: string[];
  is_admin: boolean;
  auth_method: string;
}

export interface ClusterSummary {
  id: string;
  name: string;
  type: 'trino' | 'hive' | 'mock';
  host: string;
  port: number;
  impersonation_enabled: boolean;
  impersonation_method: string;
  catalog: string | null;
  schema_: string | null;
}

export interface ColumnMeta {
  name: string;
  type: string;
}

export interface QueryHistoryItem {
  id: string;
  cluster_id: string;
  cluster_name: string;
  engine_type: string;
  query_text: string;
  status: 'QUEUED' | 'RUNNING' | 'FINISHED' | 'FAILED' | 'CANCELLED';
  rows_count: number;
  execution_time_ms: number;
  has_cached_result: boolean;
  is_in_queue: boolean;
  error_message: string | null;
  created_at: string;
  finished_at: string | null;
}

export interface CachedResultResponse {
  query_id: string;
  columns: ColumnMeta[];
  rows: any[][];
  total_rows: number;
  offset: number;
  limit: number;
}

export interface SavedQueryItem {
  id: string;
  title: string;
  description: string | null;
  cluster_id: string | null;
  query_text: string;
  is_shared: boolean;
  created_at: string;
}

export interface AIIssue {
  line: number;
  column: number;
  end_line?: number;
  end_column?: number;
  severity: 'error' | 'warning' | 'info';
  category: 'syntax' | 'performance' | 'schema' | 'security' | 'dialect';
  message: string;
  rule: string;
  suggestion?: string;
}

export interface AICheckResponse {
  is_valid: boolean;
  issues: AIIssue[];
  summary: string;
  complexity_score?: number;
  complexity_level?: string;
  estimated_notes?: string[];
  model: string;
  provider: string;
  execution_time_ms: number;
  fallback_used: boolean;
}

export interface AIExplainResponse {
  explanation: string;
  summary: string;
  tables_used: string[];
  operations: string[];
  model: string;
  provider: string;
  execution_time_ms: number;
}

export interface AIOptimizeResponse {
  original_sql: string;
  optimized_sql: string;
  optimizations: string[];
  diff_summary: string;
  model: string;
  provider: string;
  execution_time_ms: number;
}

export interface AIFixResponse {
  original_sql: string;
  fixed_sql: string;
  explanation: string;
  model: string;
  provider: string;
  execution_time_ms: number;
}

export interface AIFormatResponse {
  original_sql: string;
  formatted_sql: string;
  dialect: string;
}

export interface AIStatusResponse {
  enabled: boolean;
  provider: string;
  model: string;
  base_url: string;
  available: boolean;
  message: string;
  latency_ms?: number;
}


