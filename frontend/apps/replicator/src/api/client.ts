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
  HmsReplicationJob,
  HmsJobPage,
  HmsJobListingSummary,
  HmsEventLog,
  HmsDeadLetterEvent,
  HmsCdcState,
  JobListingSummary,
  JobPage,
  HmsSchemaDriftReport,
  CreateHmsJobPayload,
  CreateKafkaJobPayload,
  StreamingLeaseStatus,
  DrStatusResponse,
  DrEmergencyStopPayload,
  DrEmergencyRollbackPayload,
  DrReversePayload,
  DrActionResponse,
} from '../types';

/** Предел выборки заданий за запрос: сервер жестко ограничивает размер страницы 1000. */
const JOBS_PAGE_SIZE = 50;
const HMS_JOBS_PAGE_SIZE = 50;

/** Общие параметры отбора заданий метастора: страница и сводка обязаны отбирать одно и то же. */
function hmsJobFilterParams(filters?: { status?: string; search?: string; author?: string }): URLSearchParams {
  const params = new URLSearchParams();
  if (filters?.status && filters.status !== 'ALL') params.set('status', filters.status);
  if (filters?.search?.trim()) params.set('search', filters.search.trim());
  if (filters?.author) params.set('author', filters.author);
  return params;
}

/** Общие параметры отбора заданий: одинаковы для страницы и для сводки. */
function jobFilterParams(filters?: { status?: string; author?: string; search?: string }): URLSearchParams {
  const params = new URLSearchParams();
  if (filters?.status && filters.status !== 'ALL') {
    params.set('status', filters.status);
  }
  if (filters?.author && filters.author !== 'ALL') {
    params.set('author', filters.author);
  }
  if (filters?.search && filters.search.trim()) {
    params.set('search', filters.search.trim());
  }
  return params;
}

export class ReplicatorApiClient extends BaseApiClient {
  constructor() {
    super('/api/v1');
  }

  // --- Задачи репликации (Jobs) ---

  async getJobs(filters?: {
    status?: string;
    author?: string;
    search?: string;
    page?: number;
    size?: number;
  }): Promise<JobPage> {
    const params = jobFilterParams(filters);
    params.set('page', String(filters?.page ?? 0));
    params.set('size', String(filters?.size ?? JOBS_PAGE_SIZE));
    return this.request<JobPage>(`/jobs?${params.toString()}`);
  }

  /** Счетчики статусов и список авторов по всему набору — для фильтров и чипов. */
  async getJobsSummary(filters?: { author?: string; search?: string }): Promise<JobListingSummary> {
    const params = jobFilterParams(filters);
    const query = params.toString();
    return this.request<JobListingSummary>(`/jobs/summary${query ? `?${query}` : ''}`);
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

  // --- Репликация Hive Metastore (HMS Replication) ---

  async getHmsJobs(filters?: {
    status?: string;
    search?: string;
    author?: string;
    page?: number;
    size?: number;
  }): Promise<HmsJobPage> {
    const params = hmsJobFilterParams(filters);
    params.set('page', String(filters?.page ?? 0));
    params.set('size', String(filters?.size ?? HMS_JOBS_PAGE_SIZE));
    return this.request<HmsJobPage>(`/hms/jobs?${params.toString()}`);
  }

  /** Счетчики и авторы по всему отобранному набору: на странице их не посчитать. */
  async getHmsJobsSummary(filters?: { search?: string; author?: string }): Promise<HmsJobListingSummary> {
    const params = hmsJobFilterParams(filters);
    const query = params.toString();
    return this.request<HmsJobListingSummary>(`/hms/jobs/summary${query ? `?${query}` : ''}`);
  }

  async getHmsJob(id: string): Promise<HmsReplicationJob> {
    return this.request<HmsReplicationJob>(`/hms/jobs/${encodeURIComponent(id)}`);
  }

  async createHmsJob(payload: CreateHmsJobPayload): Promise<HmsReplicationJob> {
    return this.request<HmsReplicationJob>('/hms/jobs', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  async triggerHmsSync(id: string): Promise<HmsCdcState> {
    return this.request(`/hms/jobs/${encodeURIComponent(id)}/sync`, {
      method: 'POST',
    });
  }

  async getHmsEvents(id: string, limit = 100): Promise<HmsEventLog[]> {
    return this.request<HmsEventLog[]>(`/hms/jobs/${encodeURIComponent(id)}/events?limit=${limit}`);
  }

  // Сервер отдает подзадачи страницами (не больше 1000 за раз): у таблицы в 200k партиций их столько же
  async getHmsSubtasks(id: string, size = 200): Promise<Job[]> {
    return this.request<Job[]>(`/hms/jobs/${encodeURIComponent(id)}/subtasks?page=0&size=${size}`);
  }

  async pauseHmsJob(id: string): Promise<HmsReplicationJob> {
    return this.request<HmsReplicationJob>(`/hms/jobs/${encodeURIComponent(id)}/pause`, {
      method: 'POST',
    });
  }

  async resumeHmsJob(id: string): Promise<HmsReplicationJob> {
    return this.request<HmsReplicationJob>(`/hms/jobs/${encodeURIComponent(id)}/resume`, {
      method: 'POST',
    });
  }

  async rebootstrapHmsJob(id: string): Promise<HmsReplicationJob> {
    return this.request<HmsReplicationJob>(`/hms/jobs/${encodeURIComponent(id)}/rebootstrap`, {
      method: 'POST',
    });
  }

  async deleteHmsJob(id: string): Promise<{ success: boolean; message: string }> {
    return this.request<{ success: boolean; message: string }>(`/hms/jobs/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
  }

  async getHmsDlq(id: string, status?: string, limit: number = 100): Promise<HmsDeadLetterEvent[]> {
    const params = new URLSearchParams();
    if (status && status !== 'ALL') {
      params.set('status', status);
    }
    params.set('limit', String(limit));
    return this.request<HmsDeadLetterEvent[]>(`/hms/jobs/${encodeURIComponent(id)}/dlq?${params.toString()}`);
  }

  async skipHmsDlqEvent(id: string, dlqId: string): Promise<{ success: boolean; message: string }> {
    return this.request<{ success: boolean; message: string }>(
      `/hms/jobs/${encodeURIComponent(id)}/dlq/${encodeURIComponent(dlqId)}/skip`,
      { method: 'POST' }
    );
  }

  async replayHmsDlqEvent(id: string, dlqId: string): Promise<{ success: boolean; message: string }> {
    return this.request<{ success: boolean; message: string }>(
      `/hms/jobs/${encodeURIComponent(id)}/dlq/${encodeURIComponent(dlqId)}/replay`,
      { method: 'POST' }
    );
  }

  async getHmsSchemaDrift(id: string): Promise<HmsSchemaDriftReport[]> {
    return this.request<HmsSchemaDriftReport[]>(`/hms/jobs/${encodeURIComponent(id)}/drift`);
  }

  // --- HDFS Inotify Streaming & Streamer HA ---

  async getStreamingLeaseStatus(clusterId = 'dc1'): Promise<StreamingLeaseStatus> {
    return this.request<StreamingLeaseStatus>(`/streaming/lease/status?clusterId=${encodeURIComponent(clusterId)}`);
  }

  async getStreamingLeaseStatuses(): Promise<Record<string, StreamingLeaseStatus>> {
    return this.request<Record<string, StreamingLeaseStatus>>('/streaming/lease/all');
  }

  // --- Disaster Recovery (DR) & Failover ---

  async getDrStatus(): Promise<DrStatusResponse> {
    return this.request<DrStatusResponse>('/dr/status');
  }

  async emergencyStop(payload: DrEmergencyStopPayload): Promise<DrActionResponse> {
    return this.request<DrActionResponse>('/dr/emergency-stop', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  async rollbackEmergencyStop(payload?: DrEmergencyRollbackPayload): Promise<DrActionResponse> {
    return this.request<DrActionResponse>('/dr/emergency-stop/rollback', {
      method: 'POST',
      body: JSON.stringify(payload || {}),
    });
  }

  async reverseReplication(payload: DrReversePayload): Promise<DrActionResponse> {
    return this.request<DrActionResponse>('/dr/reverse', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  async reverseSingleJob(jobId: string): Promise<DrActionResponse> {
    return this.request<DrActionResponse>(`/dr/jobs/${encodeURIComponent(jobId)}/reverse`, {
      method: 'POST',
    });
  }

  async undoReverse(jobId: string): Promise<DrActionResponse> {
    return this.request<DrActionResponse>(`/dr/jobs/${encodeURIComponent(jobId)}/undo-reverse`, {
      method: 'POST',
    });
  }

  // --- Kafka Replicator ---

  async getKafkaDiscoveryClusters(): Promise<any[]> {
    return this.request<any[]>('/replicator/kafka/discovery/clusters');
  }

  async getKafkaDiscoveryTopics(dc = 'dc1', search?: string): Promise<any[]> {
    const q = search ? `&search=${encodeURIComponent(search)}` : '';
    return this.request<any[]>(`/replicator/kafka/discovery/topics?dc=${encodeURIComponent(dc)}${q}`);
  }

  async getKafkaDiscoveryGroups(dc = 'dc1', search?: string): Promise<any[]> {
    const q = search ? `&search=${encodeURIComponent(search)}` : '';
    return this.request<any[]>(`/replicator/kafka/discovery/groups?dc=${encodeURIComponent(dc)}${q}`);
  }

  async getKafkaJobs(): Promise<any[]> {
    return this.request<any[]>('/replicator/kafka/jobs');
  }

  async createKafkaJob(payload: CreateKafkaJobPayload): Promise<any> {
    return this.request<any>('/replicator/kafka/jobs', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  async pauseKafkaJob(id: string): Promise<any> {
    return this.request<any>(`/replicator/kafka/jobs/${encodeURIComponent(id)}/pause`, {
      method: 'POST',
    });
  }

  async resumeKafkaJob(id: string): Promise<any> {
    return this.request<any>(`/replicator/kafka/jobs/${encodeURIComponent(id)}/resume`, {
      method: 'POST',
    });
  }

  async deleteKafkaJob(id: string): Promise<void> {
    await this.request<void>(`/replicator/kafka/jobs/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
  }

  /** Изменение настроек задачи: незаданные поля не меняются. */
  async updateKafkaJob(id: string, payload: Record<string, unknown>): Promise<any> {
    return this.request<any>(`/replicator/kafka/jobs/${encodeURIComponent(id)}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
  }

  /** Повторная проверка и автосоздание топиков задачи в целевом кластере. */
  async provisionKafkaJobTopics(id: string): Promise<any> {
    return this.request<any>(`/replicator/kafka/jobs/${encodeURIComponent(id)}/provision-topics`, {
      method: 'POST',
    });
  }

  /** Сброс смещений группы задачи на начало или конец топиков; задача должна стоять на паузе. */
  async resetKafkaJobOffsets(id: string, position: 'LATEST' | 'EARLIEST'): Promise<any> {
    return this.request<any>(`/replicator/kafka/jobs/${encodeURIComponent(id)}/reset-offsets`, {
      method: 'POST',
      body: JSON.stringify({ position }),
    });
  }

  async triggerKafkaFailover(id: string): Promise<any> {
    return this.request<any>(`/replicator/kafka/jobs/${encodeURIComponent(id)}/failover`, {
      method: 'POST',
    });
  }

  async getKafkaReceiverEndpoints(clusterId = 'DC2'): Promise<string[]> {
    return this.request<string[]>(`/replicator/kafka/agents/receivers?clusterId=${encodeURIComponent(clusterId)}`);
  }

  async getKafkaSenderEndpoints(clusterId = 'DC1'): Promise<string[]> {
    return this.request<string[]>(`/replicator/kafka/agents/senders?clusterId=${encodeURIComponent(clusterId)}`);
  }

  async getKafkaSenderDetails(clusterId = 'DC1'): Promise<any[]> {
    return this.request<any[]>(`/replicator/kafka/agents/senders/details?clusterId=${encodeURIComponent(clusterId)}`);
  }

  async getKafkaQosCalculation(sourceDc: string, targetDc: string): Promise<any> {
    return this.request<any>(`/replicator/kafka/qos-calculation?sourceDc=${encodeURIComponent(sourceDc)}&targetDc=${encodeURIComponent(targetDc)}`);
  }
}

export const api = new ReplicatorApiClient();

