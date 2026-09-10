import type { UserSession } from '@hadoop-explorer/common';
export type UserInfo = UserSession;

export interface ClusterPublicInfo {
  id: string;
  name: string;
  description: string;
  default_path: string;
  is_read_only: boolean;
  is_admin: boolean;
}

export interface HdfsFileStatus {
  pathSuffix: string;
  type: 'FILE' | 'DIRECTORY';
  length: number;
  owner: string;
  group: string;
  permission: string;
  accessTime: number;
  modificationTime: number;
  blockSize: number;
  replication: number;
  childrenNum?: number;
}

export interface DirectoryListingResponse {
  cluster_id: string;
  path: string;
  parent_path?: string | null;
  files: HdfsFileStatus[];
  total_files: number;
  total_directories: number;
  total_size: number;
  can_write: boolean;
  can_read: boolean;
}

export interface FilePreviewResponse {
  cluster_id: string;
  path: string;
  file_type: 'text' | 'json' | 'csv' | 'parquet' | 'orc' | 'binary';
  size: number;
  truncated: boolean;
  content?: string;
  columns?: string[];
  rows?: any[][];
  row_count?: number;
  error?: string;
}

export interface FileActionResponse {
  success: boolean;
  message: string;
  path?: string;
}

export interface CrossClusterCopyRequest {
  source_cluster_id: string;
  source_path: string;
  target_cluster_id: string;
  target_path: string;
  overwrite?: boolean;
}

export interface CrossClusterCopyResponse {
  success: boolean;
  message: string;
  source_cluster_id: string;
  source_path: string;
  target_cluster_id: string;
  target_path: string;
  copied_files: number;
  copied_bytes: number;
}

export interface BatchDeleteRequest {
  paths: string[];
  recursive?: boolean;
}

export interface BatchDeleteFailedItem {
  path: string;
  error: string;
}

export interface BatchDeleteResponse {
  deleted: string[];
  failed: BatchDeleteFailedItem[];
  total_requested: number;
  success: boolean;
}

export interface BatchDownloadRequest {
  paths: string[];
}

