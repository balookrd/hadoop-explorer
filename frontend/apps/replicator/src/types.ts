import type { UserSession } from '@hadoop-explorer/common';

export type JobStatus = 'SCHEDULED' | 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'STREAMING';

export type SyncMode = 'MANUAL' | 'SCHEDULED' | 'STREAMING_INOTIFY';

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
  sync_mode?: SyncMode;
  last_processed_txid?: number;
  txid_lag?: number;
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
  sync_mode?: 'MANUAL' | 'SCHEDULED' | 'STREAMING_INOTIFY';
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
  streaming_enabled?: boolean;
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

export interface StreamingLeaseStatus {
  status: 'ACTIVE' | 'STANDBY' | 'EXPIRED' | 'DISABLED';
  epoch: number;
  active_agent_id: string;
  active_streamer_id?: string;
  lease_expires_at: string;
  registered_streamers_count: number;
  redundancy_warning: boolean;
  last_committed_txid: number;
}

export interface DrDcStatus {
  id: string;
  name: string;
  status: 'ONLINE' | 'DEGRADED' | 'OFFLINE' | 'UNKNOWN';
  role: 'PRIMARY' | 'STANDBY' | 'PROMOTED_PRIMARY';
  online_agents: number;
  total_agents: number;
  bandwidth_limit_mb_s: number;
  is_fenced: boolean;
}

export interface DrClusterStatus {
  id: string;
  name: string;
  dc_id: string;
  status: 'ONLINE' | 'OFFLINE';
  active_jobs_count: number;
  queued_jobs_count: number;
  failed_jobs_count: number;
  completed_jobs_count: number;
}

export interface DrSummary {
  active_source_dc: string;
  active_target_dc: string;
  primary_dc_online: boolean;
  standby_dc_online: boolean;
  total_active_jobs: number;
  total_frozen_jobs: number;
  total_failed_jobs: number;
  unreplicated_bytes: number;
  unreplicated_events: number;
}

export interface DrRouteItem {
  id: string;
  type: 'HDFS' | 'HMS';
  name: string;
  source_cluster_id: string;
  target_cluster_id: string;
  source_path: string;
  target_path: string;
  status: string;
  is_scheduled: boolean;
  cron_expression?: string;
  lag_bytes_or_events: number;
  has_reverse_job: boolean;
  reverse_job_id?: string;
  reverse_job_status?: string;
  is_reverse_replica?: boolean;
  message?: string;
}

export interface DrStatusResponse {
  datacenters: DrDcStatus[];
  clusters: DrClusterStatus[];
  summary: DrSummary;
  hdfs_routes: DrRouteItem[];
  hms_routes: DrRouteItem[];
}

export interface DrEmergencyStopPayload {
  cluster_id: string;
  reason?: string;
  fence_network?: boolean;
}

export interface DrReversePayload {
  from_cluster_id: string;
  to_cluster_id: string;
  include_hdfs?: boolean;
  include_hms?: boolean;
  auto_start?: boolean;
}

export interface DrActionResponse {
  success: boolean;
  message: string;
  stopped_hdfs_jobs: number;
  stopped_hms_jobs: number;
  reversed_hdfs_jobs: number;
  reversed_hms_jobs: number;
  created_job_ids: string[];
}
