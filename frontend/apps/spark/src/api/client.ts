import { BaseApiClient } from '@hadoop-explorer/common';
import type {
  UserSession,
  ClusterSummary,
  ClusterDetailResponse,
  SparkSessionItem,
  CreateSessionPayload,
  StatementResultResponse,
  HistoryItem,
  ColumnMeta
} from '../types';

export class SparkApiClient extends BaseApiClient {
  constructor() {
    super('/api/v1');
  }

  async getClusters(): Promise<ClusterSummary[]> {
    return this.request<ClusterSummary[]>('/clusters');
  }

  async getClusterDetails(clusterId: string): Promise<ClusterDetailResponse> {
    return this.request<ClusterDetailResponse>(`/clusters/${clusterId}`);
  }

  async getSessions(): Promise<SparkSessionItem[]> {
    return this.request<SparkSessionItem[]>('/sessions');
  }

  async createSession(payload: CreateSessionPayload): Promise<SparkSessionItem> {
    return this.request<SparkSessionItem>('/sessions', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  }

  async stopSession(sessionId: string): Promise<void> {
    await this.request(`/sessions/${sessionId}`, { method: 'DELETE' });
  }

  async executeCode(sessionId: string, code: string, language: string): Promise<{ execution_id: string; status: string }> {
    return this.request('/statements/execute', {
      method: 'POST',
      body: JSON.stringify({ session_id: sessionId, code, language })
    });
  }

  async cancelStatement(executionId: string): Promise<{ status: string; message: string }> {
    return this.request(`/statements/${executionId}/cancel`, {
      method: 'POST'
    });
  }

  streamExecution(executionId: string, onEvent: (event: any) => void): () => void {
    const url = `${API_BASE}/statements/${executionId}/stream`;
    // EventSource автоматически передает HttpOnly cookies
    const es = new EventSource(url, { withCredentials: true });
    es.onmessage = (e) => {
      try {
        const parsed = JSON.parse(e.data);
        onEvent(parsed);
        if (parsed.type === 'finished' || parsed.type === 'stream_end') {
          es.close();
        }
      } catch (err) {
        console.error('Ошибка парсинга SSE сообщения:', err);
      }
    };
    es.onerror = () => {
      es.close();
    };
    return () => {
      es.close();
    };
  }

  async getResult(executionId: string, offset: number = 0, limit: number = 100): Promise<StatementResultResponse> {
    return this.request<StatementResultResponse>(`/statements/${executionId}/result?offset=${offset}&limit=${limit}`);
  }

  async getDatabases(clusterId: string, metastoreId?: string): Promise<string[]> {
    const url = metastoreId
      ? `/catalog/${clusterId}/databases?metastore_id=${encodeURIComponent(metastoreId)}`
      : `/catalog/${clusterId}/databases`;
    return this.request<string[]>(url);
  }

  async getTables(clusterId: string, database: string, metastoreId?: string): Promise<string[]> {
    const params = new URLSearchParams({ database });
    if (metastoreId) params.append('metastore_id', metastoreId);
    return this.request<string[]>(`/catalog/${clusterId}/tables?${params.toString()}`);
  }

  async getColumns(clusterId: string, database: string, table: string, metastoreId?: string): Promise<ColumnMeta[]> {
    const params = new URLSearchParams({ database, table });
    if (metastoreId) params.append('metastore_id', metastoreId);
    return this.request<ColumnMeta[]>(`/catalog/${clusterId}/columns?${params.toString()}`);
  }

  async getHistory(limit: number = 50): Promise<HistoryItem[]> {
    return this.request<HistoryItem[]>(`/history?limit=${limit}`);
  }

  async getWorkspace(): Promise<{ username: string; state: any; updated_at?: string } | null> {
    return this.request<{ username: string; state: any; updated_at?: string } | null>('/workspace');
  }

  async saveWorkspace(state: any): Promise<void> {
    await this.request('/workspace', {
      method: 'PUT',
      body: JSON.stringify({ state })
    });
  }

  async clearWorkspace(): Promise<void> {
    await this.request('/workspace', {
      method: 'DELETE'
    });
  }
}

export const api = new SparkApiClient();

