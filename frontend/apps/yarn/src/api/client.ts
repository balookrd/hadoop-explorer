import type {
  UserSession, ClusterSummary, QueueTreeResponse,
  DraftQueueItem, DiffItem, TokenResponse
} from '../types';

const API_BASE = '/api/v1';

// Токен хранится только в оперативной памяти JS для текущей сессии (Zero LocalStorage),
// предотвращая кражу через XSS. Основная авторизация в браузере опирается на HttpOnly Cookie.
let memoryToken: string | null = null;

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

  const resp = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
    credentials: 'include',
  });

  if (resp.status === 401) {
    clearToken();
    throw new Error('UNAUTHORIZED');
  }

  if (!resp.ok) {
    const errBody = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(errBody.detail || `HTTP ${resp.status}`);
  }

  return resp.json();
}

export const api = {
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
};
