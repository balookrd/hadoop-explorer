import { BaseApiClient } from '@hadoop-explorer/common';
import type {
  UserSession,
  ClusterSummary,
  QueryHistoryItem,
  SavedQueryItem,
  ColumnMeta,
  CachedResultResponse,
  AICheckResponse,
  AIExplainResponse,
  AIOptimizeResponse,
  AIFixResponse,
  AIFormatResponse,
  AIGenerateResponse,
  AIStatusResponse
} from '../types';

class ApiClient extends BaseApiClient {
  constructor() {
    super('/api/v1');
  }

  async getClusters(): Promise<ClusterSummary[]> {
    return this.request<ClusterSummary[]>('/clusters');
  }

  async getCatalogs(clusterId: string): Promise<string[]> {
    return this.request<string[]>(`/catalog/${clusterId}/catalogs`);
  }

  async getSchemas(clusterId: string, catalog: string = 'hive'): Promise<string[]> {
    return this.request<string[]>(`/catalog/${clusterId}/schemas?catalog=${encodeURIComponent(catalog)}`);
  }

  async getTables(clusterId: string, catalog: string = 'hive', schema: string = 'default'): Promise<string[]> {
    return this.request<string[]>(`/catalog/${clusterId}/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`);
  }

  async getColumns(clusterId: string, catalog: string, schema: string, table: string): Promise<ColumnMeta[]> {
    return this.request<ColumnMeta[]>(`/catalog/${clusterId}/columns?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`);
  }

  async executeQuery(clusterId: string, query: string): Promise<{ query_id: string; status: string; message: string }> {
    return this.request('/queries/execute', {
      method: 'POST',
      body: JSON.stringify({ cluster_id: clusterId, query })
    });
  }

  async getQueue(): Promise<QueryHistoryItem[]> {
    return this.request<QueryHistoryItem[]>('/queries/queue');
  }

  async deleteFromQueue(queryId: string): Promise<void> {
    await this.request(`/queries/queue/${queryId}`, { method: 'DELETE' });
  }

  async getQueryResult(queryId: string, offset = 0, limit = 500): Promise<CachedResultResponse> {
    return this.request<CachedResultResponse>(`/queries/${queryId}/result?offset=${offset}&limit=${limit}`);
  }

  async cancelQuery(queryId: string): Promise<void> {
    await this.request(`/queries/${queryId}/cancel`, { method: 'POST' });
  }

  async getHistory(): Promise<QueryHistoryItem[]> {
    return this.request<QueryHistoryItem[]>('/queries/history');
  }

  async saveQuery(title: string, queryText: string, clusterId?: string): Promise<SavedQueryItem> {
    return this.request<SavedQueryItem>('/queries/saved', {
      method: 'POST',
      body: JSON.stringify({ title, query_text: queryText, cluster_id: clusterId })
    });
  }

  async getSavedQueries(): Promise<SavedQueryItem[]> {
    return this.request<SavedQueryItem[]>('/queries/saved');
  }

  streamQueryEvents(queryId: string, onEvent: (event: any) => void, onError?: (err: any) => void): () => void {
    return this.subscribeSse(`/queries/${queryId}/stream`, onEvent, {
      onError,
      shouldClose: (data) =>
        data.type === 'stream_end' || data.type === 'error' || ['FINISHED', 'FAILED', 'CANCELLED'].includes(data.status),
    });
  }

  listenUserNotifications(onEvent: (event: any) => void): () => void {
    return this.subscribeSse('/queries/notifications/stream', onEvent, {
      reconnect: true,
      reconnectDelayMs: 5000,
    });
  }

  // --- Методы ИИ-ассистента ---

  async getAiStatus(): Promise<AIStatusResponse> {
    return this.request<AIStatusResponse>('/ai/status');
  }

  async checkSql(sql: string, clusterId?: string, dialect?: string, catalogContext?: any): Promise<AICheckResponse> {
    return this.request<AICheckResponse>('/ai/check', {
      method: 'POST',
      body: JSON.stringify({
        sql,
        cluster_id: clusterId,
        dialect,
        catalog_context: catalogContext
      })
    });
  }

  async explainSql(sql: string, clusterId?: string, dialect?: string): Promise<AIExplainResponse> {
    return this.request<AIExplainResponse>('/ai/explain', {
      method: 'POST',
      body: JSON.stringify({
        sql,
        cluster_id: clusterId,
        dialect
      })
    });
  }

  async optimizeSql(sql: string, clusterId?: string, dialect?: string, catalogContext?: any): Promise<AIOptimizeResponse> {
    return this.request<AIOptimizeResponse>('/ai/optimize', {
      method: 'POST',
      body: JSON.stringify({
        sql,
        cluster_id: clusterId,
        dialect,
        catalog_context: catalogContext
      })
    });
  }

  async fixSql(sql: string, errorMessage: string, clusterId?: string, dialect?: string): Promise<AIFixResponse> {
    return this.request<AIFixResponse>('/ai/fix', {
      method: 'POST',
      body: JSON.stringify({
        sql,
        error_message: errorMessage,
        cluster_id: clusterId,
        dialect
      })
    });
  }

  async formatSql(sql: string, clusterId?: string, dialect?: string): Promise<AIFormatResponse> {
    return this.request<AIFormatResponse>('/ai/format', {
      method: 'POST',
      body: JSON.stringify({
        sql,
        cluster_id: clusterId,
        dialect
      })
    });
  }

  async generateSql(prompt: string, clusterId?: string, dialect?: string, catalogContext?: any): Promise<AIGenerateResponse> {
    return this.request<AIGenerateResponse>('/ai/generate', {
      method: 'POST',
      body: JSON.stringify({
        prompt,
        cluster_id: clusterId,
        dialect,
        catalog_context: catalogContext
      })
    });
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

export const api = new ApiClient();


