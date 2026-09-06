export type Role = 'reader' | 'writer' | 'admin';

export interface UserSession {
  username: string;
  display_name: string;
  email?: string;
  groups: string[];
  auth_method: string;
  is_admin: boolean;
  system_role: Role;
}

export interface ClusterSummary {
  id: string;
  name: string;
  description?: string;
  active_rm_url?: string;
  kerberos_enabled: boolean;
  impersonation_enabled: boolean;
  partitions: string[];
  default_partition: string;
  resource_mode: string;
  total_resources: { memory_mb: number; vcores: number };
  user_role: Role;
  can_write: boolean;
  can_admin: boolean;
}

export interface ResourceAllocation {
  memory_mb: number;
  vcores: number;
}

export interface PartitionResourceConfig {
  partition_name: string;
  capacity: number;
  max_capacity: number;
  is_elastic: boolean;
  elasticity_ratio: number;
  memory_mb?: number;
  vcores?: number;
  max_memory_mb?: number;
  max_vcores?: number;
  memory_percent?: number;
  vcore_percent?: number;
  max_memory_percent?: number;
  max_vcore_percent?: number;
  absolute_resources?: ResourceAllocation;
  absolute_max_resources?: ResourceAllocation;
}

export interface QueueNode {
  name: string;
  path: string;
  parent_path?: string;
  is_leaf: boolean;
  state: 'RUNNING' | 'STOPPED' | 'DRAINING';
  resource_mode?: 'percentage' | 'absolute' | string;
  user_limit_factor?: number;
  ordering_policy?: 'fifo' | 'fair' | string;
  max_applications?: number;
  max_am_resource_percent?: number;
  max_parallel_apps?: number;
  max_application_lifetime?: number;
  accessible_node_labels?: string[];
  default_node_label_expression?: string;
  partitions: Record<string, PartitionResourceConfig>;
  current_used_resources: ResourceAllocation;
  allocated_resources: ResourceAllocation;
  current_used_percent: number;
  num_applications: number;
  num_active_applications: number;
  num_pending_applications: number;
  children: QueueNode[];
}

export interface ClusterMetrics {
  total_memory_mb: number;
  total_vcores: number;
  allocated_memory_mb: number;
  allocated_vcores: number;
  available_memory_mb: number;
  available_vcores: number;
  active_nodes: number;
  unhealthy_nodes: number;
  total_containers: number;
  running_apps: number;
  partitions: string[];
}

export interface BranchBalance {
  parent_path: string;
  partition: string;
  total_children_capacity: number;
  unallocated_capacity: number;
  is_balanced: boolean;
  status: string;
  message: string;
  total_children_memory_mb?: number;
  unallocated_memory_mb?: number;
  total_children_vcores?: number;
  unallocated_vcores?: number;
  ram_is_balanced?: boolean;
  vcpu_is_balanced?: boolean;
}

export interface QueueTreeResponse {
  cluster_id: string;
  cluster_name: string;
  resource_mode: string;
  default_partition: string;
  partitions: string[];
  root_queue: QueueNode;
  cluster_metrics: ClusterMetrics;
  balances: BranchBalance[];
  queue_mappings?: string;
  queue_mappings_override?: boolean;
}

export interface DraftQueueItem {
  path: string;
  name: string;
  parent_path?: string;
  action: 'modify' | 'create' | 'delete';
  is_leaf: boolean;
  state: 'RUNNING' | 'STOPPED' | 'DRAINING';
  resource_mode?: 'percentage' | 'absolute' | string;
  user_limit_factor?: number;
  ordering_policy?: 'fifo' | 'fair' | string;
  max_applications?: number;
  max_am_resource_percent?: number;
  max_parallel_apps?: number;
  max_application_lifetime?: number;
  accessible_node_labels?: string[];
  default_node_label_expression?: string;
  partitions: Record<string, PartitionResourceConfig>;
}

export interface QueueMappingsDiff {
  live?: string;
  draft?: string;
  override_live?: boolean;
  override_draft?: boolean;
}

export interface DiffItem {
  path: string;
  name: string;
  parent_path?: string;
  partition: string;
  action: string;
  live_capacity?: number;
  draft_capacity?: number;
  delta_capacity?: number;
  live_max_capacity?: number;
  draft_max_capacity?: number;
  delta_max_capacity?: number;
  live_memory_mb?: number;
  draft_memory_mb?: number;
  delta_memory_mb?: number;
  live_vcores?: number;
  draft_vcores?: number;
  delta_vcores?: number;
  live_type?: string;
  draft_type?: string;
  live_state?: string;
  draft_state?: string;
  live_resource_mode?: string;
  draft_resource_mode?: string;
  live_user_limit_factor?: number;
  draft_user_limit_factor?: number;
  live_ordering_policy?: string;
  draft_ordering_policy?: string;
  live_max_applications?: number;
  draft_max_applications?: number;
  live_max_am_resource_percent?: number;
  draft_max_am_resource_percent?: number;
  live_max_parallel_apps?: number;
  draft_max_parallel_apps?: number;
  live_max_application_lifetime?: number;
  draft_max_application_lifetime?: number;
}

export interface TokenResponse {
  access_token: string;
  token_type: string;
  user: UserSession;
}

export interface ChangeRequestSummary {
  id: number;
  cluster_id: string;
  title: string;
  status: 'SUBMITTED' | 'APPROVED' | 'REJECTED' | 'CANCELLED';
  author: string;
  changes_count: number;
  created_at: string;
  updated_at: string;
  reviewer?: string;
  reviewed_at?: string;
}

export interface ChangeRequestResponse {
  id: number;
  cluster_id: string;
  title: string;
  description: string;
  status: 'SUBMITTED' | 'APPROVED' | 'REJECTED' | 'CANCELLED';
  author: string;
  created_at: string;
  updated_at: string;
  reviewer?: string;
  review_comment?: string;
  reviewed_at?: string;
  changes: DraftQueueItem[];
  diffs: DiffItem[];
  xml_content?: string;
}

export interface ChangeRequestCreate {
  cluster_id: string;
  title: string;
  description?: string;
  changes: DraftQueueItem[];
}

