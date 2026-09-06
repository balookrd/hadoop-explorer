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
  AIStatusResponse
} from '../types';

const API_BASE = '/api/v1';

class ApiClient {
  // Токен хранится только в оперативной памяти JS для текущей сессии,
  // предотвращая постоянную компрометацию через XSS / LocalStorage.
  // Основная аутентификация в браузере выполняется через безопасные HttpOnly Cookie.
  private token: string | null = null;

  setToken(token: string | null) {
    this.token = token;
    // Очищаем legacy-токен из localStorage, если он был записан ранее
    try {
      localStorage.removeItem('access_token');
    } catch (_) {}
  }

  getToken(): string | null {
    return this.token;
  }

  private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'X-Requested-With': 'XMLHttpRequest',
      ...(options.headers as Record<string, string> || {}),
    };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    const response = await fetch(`${API_BASE}${endpoint}`, {
      ...options,
      headers,
      credentials: 'include'
    });

    if (!response.ok) {
      let errorMsg = `Ошибка сервера (${response.status})`;
      try {
        const errJson = await response.json();
        if (errJson.detail) {
          errorMsg = typeof errJson.detail === 'string' ? errJson.detail : JSON.stringify(errJson.detail);
        }
      } catch (_) {}
      throw new Error(errorMsg);
    }

    return response.json();
  }

  async login(username: string, password: string): Promise<{ access_token: string; user: UserSession }> {
    const data = await this.request<{ access_token: string; user: UserSession }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password })
    });
    this.setToken(data.access_token);
    return data;
  }

  async kerberosNegotiate(): Promise<{ access_token: string; user: UserSession }> {
    const data = await this.request<{ access_token: string; user: UserSession }>('/auth/sso');
    this.setToken(data.access_token);
    return data;
  }

  async getMe(): Promise<UserSession> {
    return this.request<UserSession>('/auth/me');
  }

  async logout(): Promise<void> {
    try {
      await this.request('/auth/logout', { method: 'POST' });
    } finally {
      this.setToken(null);
    }
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
    const eventSource = new EventSource(`${API_BASE}/queries/${queryId}/stream`, {
      withCredentials: true
    });

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        onEvent(data);
        if (data.type === 'stream_end' || data.type === 'error' || ['FINISHED', 'FAILED', 'CANCELLED'].includes(data.status)) {
          eventSource.close();
        }
      } catch (err) {
        console.error('Ошибка парсинга SSE события', err);
      }
    };

    eventSource.onerror = (err) => {
      if (onError) onError(err);
      eventSource.close();
    };

    return () => {
      eventSource.close();
    };
  }

  listenUserNotifications(onEvent: (event: any) => void): () => void {
    const eventSource = new EventSource(`${API_BASE}/queries/notifications/stream`, {
      withCredentials: true
    });

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        onEvent(data);
      } catch (err) {
        console.error('Ошибка парсинга уведомления', err);
      }
    };

    return () => {
      eventSource.close();
    };
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
}

export const api = new ApiClient();


