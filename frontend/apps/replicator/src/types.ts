import type { UserSession } from '@hadoop-explorer/common';

export type JobStatus = 'SCHEDULED' | 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';

export interface JobRun {
  id: string;
  job_id: string;
  run_number: number;
  trigger_type: 'MANUAL' | 'SCHEDULED';
  status: JobStatus;
  total_bytes: number;
  copied_bytes: number;
  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;
  average_speed_mb_s?: number;
  total_objects?: number;
  transferred_objects?: number;
  skipped_objects?: number;
  failed_objects?: number;
  error_message?: string;
  message?: string;
  triggered_by?: string;
  created_at?: string;
}

export interface Job {
  id: string;
  source_cluster_id: string;
  target_cluster_id: string;
  source_path: string;
  target_path: string;
  status: JobStatus;
  progress_percent?: number;
  copied_bytes: number;
  total_bytes: number;
  total_objects?: number;
  transferred_objects?: number;
  skipped_objects?: number;
  failed_objects?: number;
  transfer_speed_mb_s?: number;
  average_speed_mb_s?: number;
  error_message?: string;
  message?: string;
  created_by?: string;
  execution_principal?: string;
  run_as_service_account?: boolean;
  is_scheduled?: boolean;
  cron_expression?: string;
  next_run_at?: string;
  history_retention_runs?: number;
  active_run_id?: string;
  runs_count?: number;
  created_at?: string;
  updated_at?: string;
  started_at?: string;
  completed_at?: string;
  estimated_completion_at?: string;
}

export interface CreateJobPayload {
  source_cluster_id: string;
  target_cluster_id: string;
  source_path: string;
  target_path: string;
  run_as_service_account?: boolean;
  execution_principal?: string;
  is_scheduled?: boolean;
  cron_expression?: string;
  history_retention_runs?: number;
}

export interface EditJobPayload {
  source_cluster_id?: string;
  target_cluster_id?: string;
  source_path?: string;
  target_path?: string;
  execution_principal?: string;
  is_scheduled?: boolean;
  cron_expression?: string;
  history_retention_runs?: number;
}

export interface ClusterInfo {
  id: string;
  name: string;
  dc_id: string;
  namenode_host: string;
  port: number;
  default_path?: string;
}

export interface DatacenterInfo {
  id: string;
  name: string;
  location: string;
  description: string;
}

export interface DcLimitItem {
  source_dc: string;
  target_dc: string;
  limit_mb_per_sec: number;
  limit_bytes_per_sec: number;
}

export interface HdfsLimitItem {
  source_cluster: string;
  target_cluster: string;
  limit_mb_per_sec: number;
  limit_bytes_per_sec: number;
}

export interface TopologyData {
  datacenters: DatacenterInfo[];
  clusters: ClusterInfo[];
  global_limit_bytes_per_sec: number;
  global_limit_mb_per_sec: number;
  dc_limits: DcLimitItem[];
  hdfs_limits: HdfsLimitItem[];
}

export type TopologyResponse = TopologyData;

export interface AgentInfo {
  agent_id: string;
  cluster_id?: string;
  dc_id?: string;
  mode: string;
  grpc_address?: string;
  hostname?: string;
  status: 'online' | 'stale' | 'offline';
  active_transfers: number;
  registered_at: string;
  last_heartbeat_at: string;
  heartbeat_age_seconds: number;
  version: string;
  max_bandwidth_mb_s?: number | null;
}

export interface HmsReplicationJob {
  id: string;
  source_cluster_id: string;
  target_cluster_id: string;
  source_db_name: string;
  target_db_name: string;
  table_include_pattern?: string;
  table_exclude_pattern?: string;
  table_pattern?: string;
  status: 'BOOTSTRAPPING' | 'ACTIVE' | 'PAUSED' | 'ERROR';
  bootstrap_event_id?: number;
  last_processed_event_id?: number;
  event_lag?: number;
  total_tables: number;
  replicated_tables: number;
  total_partitions: number;
  replicated_partitions: number;
  message?: string;
  drop_extraneous_tables?: boolean;
  drop_extraneous_partitions?: boolean;
  created_by: string;
  execution_principal?: string;
  created_at: string;
  updated_at: string;
  last_sync_at?: string;
}

export interface HmsEventLog {
  id: string;
  hms_job_id: string;
  event_id?: number;
  event_type: string;
  table_name: string;
  partition_name?: string;
  source_uri?: string;
  target_uri?: string;
  subjob_id?: string;
  status: 'PENDING_DATA' | 'DATA_COPIED' | 'APPLIED' | 'SKIPPED_ACID' | 'FAILED';
  error_message?: string;
  created_at: string;
}

export interface CreateHmsJobPayload {
  source_cluster_id: string;
  target_cluster_id: string;
  source_db: string;
  target_db?: string;
  table_pattern?: string;
  drop_extraneous_tables?: boolean;
  drop_extraneous_partitions?: boolean;
  execution_principal?: string;
}
