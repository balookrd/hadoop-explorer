import { BaseApiClient } from '@hadoop-explorer/common';
import type {
  Job,
  JobRun,
  CreateJobPayload,
  EditJobPayload,
  TopologyData,
  ClusterInfo,
  DatacenterInfo,
  AgentInfo,
} from '../types';

export class ReplicatorApiClient extends BaseApiClient {
  constructor() {
    super('/api/v1');
  }

  // --- Задачи репликации (Jobs) ---

  async getJobs(filters?: { status?: string; author?: string }): Promise<Job[]> {
    const params = new URLSearchParams();
    if (filters?.status && filters.status !== 'ALL') {
      params.set('status', filters.status);
    }
    if (filters?.author && filters.author !== 'ALL') {
      params.set('author', filters.author);
    }
    const query = params.toString() ? `?${params.toString()}` : '';
    return this.request<Job[]>(`/jobs${query}`);
  }

  async getJob(jobId: string): Promise<Job> {
    return this.request<Job>(`/jobs/${encodeURIComponent(jobId)}`);
  }

  async createJob(payload: CreateJobPayload): Promise<Job> {
    return this.request<Job>('/jobs', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  async updateJob(jobId: string, payload: EditJobPayload): Promise<Job> {
    return this.request<Job>(`/jobs/${encodeURIComponent(jobId)}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
  }

  async patchJob(jobId: string, payload: Partial<Job>): Promise<Job> {
    return this.request<Job>(`/jobs/${encodeURIComponent(jobId)}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    });
  }

  async deleteJob(jobId: string): Promise<{ status: string; id: string; message: string }> {
    return this.request(`/jobs/${encodeURIComponent(jobId)}`, {
      method: 'DELETE',
    });
  }

  async startJob(jobId: string): Promise<Job> {
    return this.request<Job>(`/jobs/${encodeURIComponent(jobId)}/start`, {
      method: 'POST',
    });
  }

  async stopJob(jobId: string): Promise<Job> {
    return this.request<Job>(`/jobs/${encodeURIComponent(jobId)}/stop`, {
      method: 'POST',
    });
  }

  async cancelJob(jobId: string): Promise<Job> {
    return this.request<Job>(`/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  // --- История запусков (Job Runs) ---

  async getJobRuns(jobId: string): Promise<JobRun[]> {
    return this.request<JobRun[]>(`/jobs/${encodeURIComponent(jobId)}/runs`);
  }

  async updateJobRetention(jobId: string, historyRetentionRuns: number): Promise<Job> {
    return this.updateJob(jobId, { history_retention_runs: historyRetentionRuns });
  }

  // --- Топология и Кластеры ---

  async getTopology(): Promise<TopologyData> {
    return this.request<TopologyData>('/topology');
  }

  async getClusters(): Promise<ClusterInfo[]> {
    return this.request<ClusterInfo[]>('/clusters');
  }

  async getDatacenters(): Promise<DatacenterInfo[]> {
    return this.request<DatacenterInfo[]>('/datacenters');
  }

  // --- Агенты ---

  async getAgents(includeOffline: boolean = true): Promise<AgentInfo[]> {
    return this.request<AgentInfo[]>(`/agents?include_offline=${includeOffline}`);
  }

  // --- Управление полосой пропускания (Throttling / Limits) ---

  async saveGlobalLimit(limitBytesPerSec: number): Promise<any> {
    return this.request('/limits/global', {
      method: 'POST',
      body: JSON.stringify({ limit_bytes_per_sec: limitBytesPerSec }),
    });
  }

  async saveDcLimit(sourceDc: string, targetDc: string, limitMbPerSec: number): Promise<any> {
    return this.request('/limits/dc-dc', {
      method: 'POST',
      body: JSON.stringify({
        source_dc: sourceDc,
        target_dc: targetDc,
        limit_mb_per_sec: limitMbPerSec,
      }),
    });
  }

  async saveHdfsLimit(sourceCluster: string, targetCluster: string, limitMbPerSec: number): Promise<any> {
    return this.request('/limits/hdfs-hdfs', {
      method: 'POST',
      body: JSON.stringify({
        source_cluster: sourceCluster,
        target_cluster: targetCluster,
        limit_mb_per_sec: limitMbPerSec,
      }),
    });
  }
}

export const api = new ReplicatorApiClient();
