import type {
  UserSession, ClusterSummary, QueueTreeResponse,
  DraftQueueItem, DiffItem, TokenResponse
} from '../types';

const API_BASE = '/api/v1';

// Токен хранится только в оперативной памяти JS для текущей сессии (Zero LocalStorage),
// предотвращая кражу через XSS. Основная авторизация в браузере опирается на HttpOnly Cookie.
let memoryToken: string | null = null;
type UnauthorizedHandler = (message: string) => void;
const unauthorizedHandlers: Set<UnauthorizedHandler> = new Set();
let isAttemptingSso = false;

try {
  localStorage.removeItem('access_token');
} catch (_) {}

function getToken(): string | null {
  return memoryToken;
}

function setToken(token: string | null) {
  memoryToken = token;
}

function clearToken() {
  memoryToken = null;
}

function notifyUnauthorized(message: string = 'Сессия истекла или сервер был перезагружен. Пожалуйста, выполните вход.') {
  clearToken();
  for (const handler of unauthorizedHandlers) {
    try {
      handler(message);
    } catch (e) {
      console.error('Error in unauthorized handler', e);
    }
  }
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
    ...(options.headers as Record<string, string> || {}),
  };
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  let resp: Response;
  try {
    resp = await fetch(`${API_BASE}${path}`, {
      ...options,
      headers,
      credentials: 'include',
    });
  } catch (netErr: any) {
    throw new Error(`Сервер недоступен или перезагружается (${netErr.message || 'сетевая ошибка'})`);
  }

  const isAuthEndpoint = path.includes('/auth/login') || path.includes('/auth/sso') || path.includes('/auth/logout') || path.includes('/auth/me');

  if (resp.status === 401) {
    if (!isAuthEndpoint && !isAttemptingSso) {
      isAttemptingSso = true;
      try {
        const ssoRes = await api.kerberosNegotiate();
        if (ssoRes) {
          isAttemptingSso = false;
          return await request<T>(path, options);
        }
      } catch {
        // SSO не сработал
      } finally {
        isAttemptingSso = false;
      }

      const errDetail = 'Сессия истекла или сервер был перезагружен. Пожалуйста, выполните вход.';
      notifyUnauthorized(errDetail);
      throw new Error(errDetail);
    } else {
      clearToken();
      let errorMsg = 'Требуется авторизация';
      try {
        const errBody = await resp.json();
        if (errBody && errBody.detail) {
          errorMsg = typeof errBody.detail === 'string' ? errBody.detail : JSON.stringify(errBody.detail);
        }
      } catch (_) {}
      throw new Error(errorMsg);
    }
  }

  if (!resp.ok) {
    let errorMsg = resp.statusText || `HTTP ${resp.status}`;
    try {
      const errBody = await resp.json();
      if (errBody && errBody.detail) {
        errorMsg = typeof errBody.detail === 'string' ? errBody.detail : JSON.stringify(errBody.detail);
      }
    } catch (_) {}
    throw new Error(errorMsg);
  }

  return resp.json();
}

export const api = {
  onUnauthorized(handler: UnauthorizedHandler): () => void {
    unauthorizedHandlers.add(handler);
    return () => unauthorizedHandlers.delete(handler);
  },

  async tryAutoLogin(): Promise<UserSession | null> {
    try {
      const resp = await this.kerberosNegotiate();
      return resp;
    } catch {
      return null;
    }
  },

  async kerberosNegotiate(): Promise<UserSession> {
    const data = await request<TokenResponse>('/auth/sso', { method: 'GET' });
    if (data.access_token) {
      setToken(data.access_token);
    }
    return data.user;
  },
  async login(username: string, password: string): Promise<UserSession> {
    const data = await request<TokenResponse>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    setToken(data.access_token);
    return data.user;
  },

  async getMe(): Promise<UserSession> {
    return request<UserSession>('/auth/me');
  },

  async logout(): Promise<void> {
    await request('/auth/logout', { method: 'POST' }).catch(() => {});
    clearToken();
  },

  async getClusters(): Promise<ClusterSummary[]> {
    return request<ClusterSummary[]>('/clusters');
  },

  async getQueueTree(clusterId: string): Promise<QueueTreeResponse> {
    return request<QueueTreeResponse>(`/clusters/${clusterId}/queues`);
  },

  async validateDraft(clusterId: string, queues: DraftQueueItem[], partition: string, queueMappings?: string, queueMappingsOverride?: boolean) {
    return request<{ is_valid: boolean; balances: any[]; errors: string[]; warnings: string[] }>(
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
  },

  async getDiff(clusterId: string, queues: DraftQueueItem[], partition: string, queueMappings?: string, queueMappingsOverride?: boolean) {
    return request<{ cluster_id: string; has_changes: boolean; diffs: DiffItem[]; queue_mappings_diff?: any }>(
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
  },

  async generateXml(clusterId: string, queues: DraftQueueItem[], comment?: string, resourceModeOverride?: string, queueMappings?: string, queueMappingsOverride?: boolean) {
    return request<{
      cluster_id: string; filename: string; xml_content: string;
      applied_by: string; generated_at: string; instructions: string;
    }>(
      `/clusters/${clusterId}/generate-xml`,
      {
        method: 'POST',
        body: JSON.stringify({
          cluster_id: clusterId,
          queues,
          proposal_comment: comment,
          resource_mode_override: resourceModeOverride,
          queue_mappings: queueMappings,
          queue_mappings_override: queueMappingsOverride,
        }),
      }
    );
  },

  // Change Requests API
  async listChangeRequests(clusterId?: string, status?: string) {
    const params = new URLSearchParams();
    if (clusterId) params.append('cluster_id', clusterId);
    if (status) params.append('status', status);
    const qs = params.toString() ? `?${params.toString()}` : '';
    return request<import('../types').ChangeRequestSummary[]>(`/change-requests${qs}`);
  },

  async getPendingCount(clusterId?: string) {
    const qs = clusterId ? `?cluster_id=${clusterId}` : '';
    return request<{ pending_count: number }>(`/change-requests/pending-count${qs}`);
  },

  async getChangeRequest(id: number) {
    return request<import('../types').ChangeRequestResponse>(`/change-requests/${id}`);
  },

  async createChangeRequest(data: import('../types').ChangeRequestCreate) {
    return request<import('../types').ChangeRequestResponse>('/change-requests', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  async approveChangeRequest(id: number, comment?: string) {
    return request<import('../types').ChangeRequestResponse>(`/change-requests/${id}/approve`, {
      method: 'POST',
      body: JSON.stringify({ comment: comment || '' }),
    });
  },

  async rejectChangeRequest(id: number, comment?: string) {
    return request<import('../types').ChangeRequestResponse>(`/change-requests/${id}/reject`, {
      method: 'POST',
      body: JSON.stringify({ comment: comment || '' }),
    });
  },

  async cancelChangeRequest(id: number) {
    return request<import('../types').ChangeRequestResponse>(`/change-requests/${id}/cancel`, {
      method: 'POST',
    });
  },

  async previewChangeRequestXml(id: number) {
    return request<{ cr_id: number; title: string; filename: string; xml_content: string }>(`/change-requests/${id}/preview-xml`);
  },
};

