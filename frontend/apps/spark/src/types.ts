import type { UserSession } from '@hadoop-explorer/common';
export type { UserSession };

export interface ClusterSummary {
  id: string;
  name: string;
  description: string | null;
  type: string;
  livy_url: string;
  yarn_cluster_id: string | null;
}

export interface PythonEnvItem {
  id: string;
  name: string;
  is_default: boolean;
}

export interface SparkVersionItem {
  id: string;
  name: string;
  is_default: boolean;
  python_versions: PythonEnvItem[];
}

export interface MetastoreItem {
  id: string;
  name: string;
  is_default: boolean;
}

export interface ResourceProfileItem {
  id: string;
  name: string;
  driver_memory: string;
  driver_cores: number;
  executor_memory: string;
  executor_cores: number;
  num_executors: number;
}

export interface ClusterDetailResponse {
  id: string;
  name: string;
  description: string | null;
  type: string;
  yarn_cluster_id: string | null;
  spark_versions: SparkVersionItem[];
  metastores: MetastoreItem[];
  yarn_queues: string[];
  default_queue: string;
  resource_profiles: ResourceProfileItem[];
  default_repositories: string[];
}

export interface SparkSessionItem {
  id: string;
  cluster_id: string;
  spark_version_id: string;
  python_env_id: string | null;
  custom_python_archive?: string | null;
  custom_python_path?: string | null;
  metastore_id: string;
  yarn_queue: string;
  resource_profile: string;
  kind: string;
  status: 'not_started' | 'starting' | 'idle' | 'busy' | 'dead' | 'killed';
  yarn_application_id: string | null;
  created_at: string | null;
  last_activity_at: string | null;
}

export interface CreateSessionPayload {
  cluster_id: string;
  spark_version_id: string;
  metastore_id: string;
  yarn_queue: string;
  resource_profile: string;
  kind: string;
  python_env_id?: string;
  custom_python_archive?: string;
  custom_python_path?: string;
  packages?: string[];
  jars?: string[];
  py_files?: string[];
  spark_conf?: Record<string, string>;
}

export interface ColumnMeta {
  name: string;
  type: string;
}

export interface StatementResultResponse {
  execution_id: string;
  status: string;
  columns: ColumnMeta[];
  rows: any[][];
  total_rows: number;
  offset: number;
  limit: number;
  logs: string | null;
  error_message: string | null;
  execution_time_ms: number;
}

export interface HistoryItem {
  id: string;
  session_id: string;
  cluster_id: string;
  language: string;
  code: string;
  status: string;
  rows_count: number;
  execution_time_ms: number;
  error_message: string | null;
  has_cached_result: boolean;
  created_at: string;
  finished_at: string | null;
}

export interface TabResultData {
  columns: ColumnMeta[];
  rows: any[][];
  totalRows: number;
  logs: string;
  executionTimeMs: number;
  errorMessage: string | null;
  executionId: string | null;
  statusText: string;
  activeResultTab: 'table' | 'logs';
}

export interface Tab {
  id: string;
  title: string;
  language: 'pyspark' | 'scalaspark' | 'sql';
  code: string;
  codeBuffers?: {
    pyspark: string;
    scalaspark: string;
    sql: string;
  };
  resultBuffers?: {
    pyspark: TabResultData;
    scalaspark: TabResultData;
    sql: TabResultData;
  };
  columns: ColumnMeta[];
  rows: any[][];
  totalRows: number;
  logs: string;
  isRunning: boolean;
  statusText: string;
  executionTimeMs: number;
  errorMessage: string | null;
  executionId: string | null;
  activeResultTab: 'table' | 'logs';
}

