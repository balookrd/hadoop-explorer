import { BaseApiClient } from '@hadoop-explorer/common';
import type {
  ClusterSummary,
  QueueTreeResponse,
  DraftQueueItem,
  DiffItem,
  ChangeRequestSummary,
  ChangeRequestResponse,
  ChangeRequestCreate,
  DeployResponse,
  DirectDeployXmlResponse,
} from '../types';

export class YarnApiClient extends BaseApiClient {
  constructor() {
    super('/api/v1');
  }

  async getClusters(): Promise<ClusterSummary[]> {
    return this.request<ClusterSummary[]>('/clusters');
  }

  async getQueueTree(clusterId: string): Promise<QueueTreeResponse> {
    return this.request<QueueTreeResponse>(`/clusters/${clusterId}/queues`);
  }

  async validateDraft(
    clusterId: string,
    queues: DraftQueueItem[],
    partition: string,
    queueMappings?: string,
    queueMappingsOverride?: boolean
  ) {
    return this.request<{ is_valid: boolean; balances: any[]; errors: string[]; warnings: string[] }>(
      `/clusters/${clusterId}/validate`,
      {
        method: 'POST',
        body: JSON.stringify({
          cluster_id: clusterId,
          selected_partition: partition,
          queues,
          queue_mappings: queueMappings,
          queue_mappings_override: queueMappingsOverride,
        }),
      }
    );
  }

  async getDiff(
    clusterId: string,
    queues: DraftQueueItem[],
    partition: string,
    queueMappings?: string,
    queueMappingsOverride?: boolean
  ) {
    return this.request<{ cluster_id: string; has_changes: boolean; diffs: DiffItem[]; queue_mappings_diff?: any }>(
      `/clusters/${clusterId}/diff`,
      {
        method: 'POST',
        body: JSON.stringify({
          cluster_id: clusterId,
          selected_partition: partition,
          queues,
          queue_mappings: queueMappings,
          queue_mappings_override: queueMappingsOverride,
        }),
      }
    );
  }

  async generateXml(
    clusterId: string,
    queues: DraftQueueItem[],
    comment?: string,
    resourceModeOverride?: string,
    queueMappings?: string,
    queueMappingsOverride?: boolean
  ) {
    return this.request<{
      cluster_id: string;
      filename: string;
      xml_content: string;
      applied_by: string;
      generated_at: string;
      instructions: string;
    }>(`/clusters/${clusterId}/generate-xml`, {
      method: 'POST',
      body: JSON.stringify({
        cluster_id: clusterId,
        queues,
        proposal_comment: comment,
        resource_mode_override: resourceModeOverride,
        queue_mappings: queueMappings,
        queue_mappings_override: queueMappingsOverride,
      }),
    });
  }

  async listChangeRequests(clusterId?: string, status?: string) {
    const params = new URLSearchParams();
    if (clusterId) params.append('cluster_id', clusterId);
    if (status) params.append('status', status);
    const qs = params.toString() ? `?${params.toString()}` : '';
    return this.request<ChangeRequestSummary[]>(`/change-requests${qs}`);
  }

  async getPendingCount(clusterId?: string) {
    const qs = clusterId ? `?cluster_id=${clusterId}` : '';
    return this.request<{ pending_count: number }>(`/change-requests/pending-count${qs}`);
  }

  async getChangeRequest(id: number) {
    return this.request<ChangeRequestResponse>(`/change-requests/${id}`);
  }

  async createChangeRequest(data: ChangeRequestCreate) {
    return this.request<ChangeRequestResponse>('/change-requests', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  async approveChangeRequest(id: number, comment?: string) {
    return this.request<ChangeRequestResponse>(`/change-requests/${id}/approve`, {
      method: 'POST',
      body: JSON.stringify({ comment: comment || '' }),
    });
  }

  async rejectChangeRequest(id: number, comment?: string) {
    return this.request<ChangeRequestResponse>(`/change-requests/${id}/reject`, {
      method: 'POST',
      body: JSON.stringify({ comment: comment || '' }),
    });
  }

  async cancelChangeRequest(id: number) {
    return this.request<ChangeRequestResponse>(`/change-requests/${id}/cancel`, {
      method: 'POST',
    });
  }

  async previewChangeRequestXml(id: number) {
    return this.request<{ cr_id: number; title: string; filename: string; xml_content: string }>(
      `/change-requests/${id}/preview-xml`
    );
  }

  async deployChangeRequest(crId: number, wait: boolean = true) {
    return this.request<DeployResponse>(`/change-requests/${crId}/deploy?wait=${wait}`, {
      method: 'POST',
    });
  }

  async getDeployStatus(crId: number) {
    return this.request<DeployResponse>(`/change-requests/${crId}/deploy-status`);
  }

  async deployXmlDirect(clusterId: string, xmlContent: string, comment?: string) {
    return this.request<DirectDeployXmlResponse>(`/clusters/${clusterId}/deploy-xml`, {
      method: 'POST',
      body: JSON.stringify({
        xml_content: xmlContent,
        comment: comment || 'Manual direct XML deployment',
      }),
    });
  }
}

export const apiClient = new YarnApiClient();
export const api = apiClient;
