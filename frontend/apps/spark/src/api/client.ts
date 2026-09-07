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

class SparkApiClient {
  private unauthorizedCallbacks: ((msg: string) => void)[] = [];

  onUnauthorized(cb: (msg: string) => void) {
    this.unauthorizedCallbacks.push(cb);
    return () => {
      this.unauthorizedCallbacks = this.unauthorizedCallbacks.filter((c) => c !== cb);
    };
  }

  private handle401(msg: string) {
    this.unauthorizedCallbacks.forEach((cb) => cb(msg));
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const res = await fetch(path, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers
      }
    });

    const isAuthEndpoint = path.includes('/auth/login') || path.includes('/auth/logout') || path.includes('/auth/me');
    if (res.status === 401) {
      if (!isAuthEndpoint) {
        this.handle401('Сессия истекла. Пожалуйста, войдите снова.');
      }
      throw new Error('Unauthorized');
    }

    if (!res.ok) {
      let errMsg = `Ошибка запроса (${res.status})`;
      try {
        const errJson = await res.json();
        errMsg = errJson.detail || errMsg;
      } catch {}
      throw new Error(errMsg);
    }

    return res.json();
  }

  async getMe(): Promise<UserSession> {
    return this.request<UserSession>('/api/auth/me');
  }

  async login(username: string, password: string):Promise<{ access_token: string; user: UserSession }> {
    return this.request('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password })
    });
  }

  async logout(): Promise<void> {
    await this.request('/api/auth/logout', { method: 'POST' });
  }

  async getClusters(): Promise<ClusterSummary[]> {
    return this.request<ClusterSummary[]>('/api/clusters');
  }

  async getClusterDetails(clusterId: string): Promise<ClusterDetailResponse> {
    return this.request<ClusterDetailResponse>(`/api/clusters/${clusterId}`);
  }

  async getSessions(): Promise<SparkSessionItem[]> {
    return this.request<SparkSessionItem[]>('/api/sessions');
  }

  async createSession(payload: CreateSessionPayload): Promise<SparkSessionItem> {
    return this.request<SparkSessionItem>('/api/sessions', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  }

  async stopSession(sessionId: string): Promise<void> {
    await this.request(`/api/sessions/${sessionId}`, { method: 'DELETE' });
  }

  async executeCode(sessionId: string, code: string, language: string): Promise<{ execution_id: string; status: string }> {
    return this.request('/api/statements/execute', {
      method: 'POST',
      body: JSON.stringify({ session_id: sessionId, code, language })
    });
  }

  async cancelStatement(executionId: string): Promise<{ status: string; message: string }> {
    return this.request(`/api/statements/${executionId}/cancel`, {
      method: 'POST'
    });
  }

  streamExecution(executionId: string, onEvent: (event: any) => void): () => void {
    const url = this.token
      ? `/api/statements/${executionId}/stream?token=${encodeURIComponent(this.token)}`
      : `/api/statements/${executionId}/stream`;
    const es = new EventSource(url);
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
    return this.request<StatementResultResponse>(`/api/statements/${executionId}/result?offset=${offset}&limit=${limit}`);
  }

  async getDatabases(clusterId: string, metastoreId?: string): Promise<string[]> {
    const url = metastoreId
      ? `/api/catalog/${clusterId}/databases?metastore_id=${encodeURIComponent(metastoreId)}`
      : `/api/catalog/${clusterId}/databases`;
    return this.request<string[]>(url);
  }

  async getTables(clusterId: string, database: string, metastoreId?: string): Promise<string[]> {
    const params = new URLSearchParams({ database });
    if (metastoreId) params.append('metastore_id', metastoreId);
    return this.request<string[]>(`/api/catalog/${clusterId}/tables?${params.toString()}`);
  }

  async getColumns(clusterId: string, database: string, table: string, metastoreId?: string): Promise<ColumnMeta[]> {
    const params = new URLSearchParams({ database, table });
    if (metastoreId) params.append('metastore_id', metastoreId);
    return this.request<ColumnMeta[]>(`/api/catalog/${clusterId}/columns?${params.toString()}`);
  }

  async getHistory(limit: number = 50): Promise<HistoryItem[]> {
    return this.request<HistoryItem[]>(`/api/history?limit=${limit}`);
  }

  async getWorkspace(): Promise<{ username: string; state: any; updated_at?: string } | null> {
    return this.request<{ username: string; state: any; updated_at?: string } | null>('/api/workspace');
  }

  async saveWorkspace(state: any): Promise<void> {
    await this.request('/api/workspace', {
      method: 'PUT',
      body: JSON.stringify({ state })
    });
  }

  async clearWorkspace(): Promise<void> {
    await this.request('/api/workspace', {
      method: 'DELETE'
    });
  }
}

export const api = new SparkApiClient();
