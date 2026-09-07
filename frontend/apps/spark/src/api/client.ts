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

const API_BASE = '/api/v1';

type UnauthorizedHandler = (message: string) => void;

class SparkApiClient {
  // Токен хранится только в оперативной памяти JS для текущей сессии (Zero LocalStorage),
  // предотвращая постоянную компрометацию через XSS.
  // Основная аутентификация в браузере опирается на безопасные HttpOnly Cookie (Cookie-first).
  private token: string | null = null;
  private unauthorizedHandlers: Set<UnauthorizedHandler> = new Set();
  private isAttemptingSso: boolean = false;

  constructor() {
    this.clearLegacyStorage();
  }

  private clearLegacyStorage() {
    if (typeof localStorage !== 'undefined') {
      try {
        localStorage.removeItem('access_token');
      } catch (_) {
        // ignore
      }
    }
  }

  public onUnauthorized(handler: UnauthorizedHandler): () => void {
    this.unauthorizedHandlers.add(handler);
    return () => this.unauthorizedHandlers.delete(handler);
  }

  private notifyUnauthorized(message: string = 'Сессия истекла или сервер был перезагружен. Пожалуйста, выполните вход.') {
    this.token = null;
    this.clearLegacyStorage();
    for (const handler of this.unauthorizedHandlers) {
      try {
        handler(message);
      } catch (e) {
        console.error('Error in unauthorized handler', e);
      }
    }
  }

  public setToken(token: string | null) {
    this.token = token;
    this.clearLegacyStorage();
  }

  public getToken(): string | null {
    return this.token;
  }

  public async tryAutoLogin(): Promise<UserSession | null> {
    try {
      const ssoRes = await this.kerberosNegotiate();
      if (ssoRes && (ssoRes.user || ssoRes.access_token)) {
        return ssoRes.user || (await this.getMe());
      }
    } catch {
      // SSO не сработал
    }
    return null;
  }

  public async kerberosNegotiate(): Promise<{ user?: UserSession; access_token?: string } | null> {
    try {
      const res = await fetch(`${API_BASE}/auth/sso`, {
        method: 'GET',
        headers: {
          'X-Requested-With': 'XMLHttpRequest',
        },
        credentials: 'include',
      });
      if (res.ok) {
        const data = await res.json();
        if (data.access_token) {
          this.setToken(data.access_token);
        }
        return data;
      }
    } catch (e) {
      // Negotiate failed
    }
    return null;
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const url = path.startsWith('/api') ? path : `${API_BASE}${path.startsWith('/') ? path : `/${path}`}`;
    const headers = new Headers(options.headers || {});

    // Защита от CSRF
    headers.set('X-Requested-With', 'XMLHttpRequest');

    if (this.token && !headers.has('Authorization')) {
      headers.set('Authorization', `Bearer ${this.token}`);
    }

    if (!headers.has('Content-Type') && !(options.body instanceof FormData)) {
      headers.set('Content-Type', 'application/json');
    }

    let res: Response;
    try {
      res = await fetch(url, {
        ...options,
        headers,
        credentials: 'include', // Отправка HttpOnly cookies
      });
    } catch (netErr: any) {
      throw new Error(`Сервер недоступен или перезагружается (${netErr.message || 'сетевая ошибка'})`);
    }

    const isAuthEndpoint = path.includes('/auth/login') || path.includes('/auth/sso') || path.includes('/auth/logout') || path.includes('/auth/me');

    if (res.status === 401) {
      if (!isAuthEndpoint && !this.isAttemptingSso) {
        this.isAttemptingSso = true;
        try {
          const ssoRes = await this.kerberosNegotiate();
          if (ssoRes && (ssoRes.access_token || ssoRes.user)) {
            this.isAttemptingSso = false;
            return await this.request<T>(path, options);
          }
        } catch {
          // SSO не удался
        } finally {
          this.isAttemptingSso = false;
        }

        this.notifyUnauthorized();
        throw new Error('Unauthorized');
      } else {
        this.token = null;
        let errMsg = 'Требуется авторизация';
        try {
          const errJson = await res.json();
          errMsg = errJson.detail || errMsg;
        } catch {}
        throw new Error(errMsg);
      }
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
    return this.request<UserSession>('/auth/me');
  }

  async login(username: string, password: string): Promise<{ access_token: string; user: UserSession }> {
    const res = await this.request<{ access_token: string; user: UserSession }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password })
    });
    if (res && res.access_token) {
      this.setToken(res.access_token);
    }
    return res;
  }

  async logout(): Promise<void> {
    try {
      await this.request('/auth/logout', { method: 'POST' });
    } finally {
      this.token = null;
      this.clearLegacyStorage();
    }
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

