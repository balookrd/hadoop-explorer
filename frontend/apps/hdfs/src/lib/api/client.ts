import { BaseApiClient } from '@hadoop-explorer/common';
import type {
  UserInfo,
  ClusterPublicInfo,
  DirectoryListingResponse,
  FilePreviewResponse,
  FileActionResponse,
  CrossClusterCopyRequest,
  CrossClusterCopyResponse,
} from '../types';

export class HdfsApiClient extends BaseApiClient {
  constructor() {
    super('/api/v1');
  }

  // Clusters
  async getClusters(): Promise<ClusterPublicInfo[]> {
    return this.request('/clusters');
  }

  // Files
  async listFiles(clusterId: string, path: string = '/'): Promise<DirectoryListingResponse> {
    const query = new URLSearchParams({ path }).toString();
    return this.request(`/clusters/${clusterId}/files?${query}`);
  }

  async previewFile(clusterId: string, path: string): Promise<FilePreviewResponse> {
    const query = new URLSearchParams({ path }).toString();
    return this.request(`/clusters/${clusterId}/files/preview?${query}`);
  }

  getDownloadUrl(clusterId: string, path: string): string {
    const query = new URLSearchParams({ path }).toString();
    return `/api/v1/clusters/${clusterId}/files/download?${query}`;
  }

  async uploadFile(
    clusterId: string,
    path: string,
    file: File,
    overwrite: boolean = true,
    relativePath?: string
  ): Promise<FileActionResponse> {
    const formData = new FormData();
    formData.append('path', path);
    formData.append('file', file);
    formData.append('overwrite', String(overwrite));
    if (relativePath) {
      formData.append('relative_path', relativePath);
    }

    return this.request(`/clusters/${clusterId}/files/upload`, {
      method: 'POST',
      body: formData,
    });
  }

  async uploadChunk(
    clusterId: string,
    formData: FormData
  ): Promise<FileActionResponse> {
    return this.request(`/clusters/${clusterId}/files/upload-chunk`, {
      method: 'POST',
      body: formData,
    });
  }

  async getChunkUploadStatus(
    clusterId: string,
    uploadId: string,
    totalChunks: number
  ): Promise<{ upload_id: string; received_chunks: number[]; total_chunks: number; is_complete: boolean }> {
    const query = new URLSearchParams({ upload_id: uploadId, total_chunks: String(totalChunks) }).toString();
    return this.request(`/clusters/${clusterId}/files/upload-chunk/status?${query}`);
  }

  async uploadArchive(
    clusterId: string,
    path: string,
    file: File,
    overwrite: boolean = true
  ): Promise<FileActionResponse> {
    const formData = new FormData();
    formData.append('path', path);
    formData.append('file', file);
    formData.append('overwrite', String(overwrite));

    return this.request(`/clusters/${clusterId}/files/upload-archive`, {
      method: 'POST',
      body: formData,
    });
  }

  async createDirectory(clusterId: string, path: string): Promise<FileActionResponse> {
    const query = new URLSearchParams({ path }).toString();
    return this.request(`/clusters/${clusterId}/files/mkdir?${query}`, {
      method: 'POST',
    });
  }

  async renamePath(clusterId: string, src: string, dst: string): Promise<FileActionResponse> {
    const query = new URLSearchParams({ src, dst }).toString();
    return this.request(`/clusters/${clusterId}/files/rename?${query}`, {
      method: 'POST',
    });
  }

  async deletePath(clusterId: string, path: string, recursive: boolean = false): Promise<FileActionResponse> {
    const query = new URLSearchParams({ path, recursive: String(recursive) }).toString();
    return this.request(`/clusters/${clusterId}/files/delete?${query}`, {
      method: 'DELETE',
    });
  }

  async copyCrossCluster(req: CrossClusterCopyRequest): Promise<CrossClusterCopyResponse> {
    return this.request('/clusters/cross-copy', {
      method: 'POST',
      body: JSON.stringify(req),
    });
  }
}

export const api = new HdfsApiClient();
