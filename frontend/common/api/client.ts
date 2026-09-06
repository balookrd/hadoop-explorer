import type { AuthResponse, UserSession } from '../types/auth';

export class BaseApiClient {
  protected token: string | null = null;
  protected baseUrl: string;

  constructor(baseUrl: string = '/api/v1') {
    this.baseUrl = baseUrl;
    this.token = typeof localStorage !== 'undefined' ? localStorage.getItem('access_token') : null;
  }

  public setToken(token: string | null) {
    this.token = token;
    if (typeof localStorage !== 'undefined') {
      if (token) {
        localStorage.setItem('access_token', token);
      } else {
        localStorage.removeItem('access_token');
      }
    }
  }

  public getToken(): string | null {
    return this.token;
  }

  public async request<T = any>(path: string, options: RequestInit = {}): Promise<T> {
    const url = `${this.baseUrl}${path.startsWith('/') ? path : `/${path}`}`;
    const headers = new Headers(options.headers || {});

    // Защита от CSRF
    headers.set('X-Requested-With', 'XMLHttpRequest');

    if (this.token && !headers.has('Authorization')) {
      headers.set('Authorization', `Bearer ${this.token}`);
    }

    if (!headers.has('Content-Type') && !(options.body instanceof FormData)) {
      headers.set('Content-Type', 'application/json');
    }

    const response = await fetch(url, {
      ...options,
      headers,
      credentials: 'include',
    });

    if (response.status === 401) {
      this.setToken(null);
      throw new Error('Требуется авторизация');
    }

    if (!response.ok) {
      let errDetail = `HTTP ${response.status} ${response.statusText}`;
      try {
        const errorJson = await response.json();
        errDetail = errorJson.detail || errorJson.message || errDetail;
      } catch {
        // ignore json parse error
      }
      throw new Error(errDetail);
    }

    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      return response.json();
    }
    return response.text() as unknown as T;
  }

  public async login(username: string, password: string): Promise<AuthResponse> {
    const res = await this.request<AuthResponse>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    if (res.access_token) {
      this.setToken(res.access_token);
    }
    return res;
  }

  public async kerberosNegotiate(): Promise<AuthResponse> {
    const res = await this.request<AuthResponse>('/auth/sso', {
      method: 'GET',
    });
    if (res.access_token) {
      this.setToken(res.access_token);
    }
    return res;
  }

  public async logout(): Promise<void> {
    try {
      await this.request('/auth/logout', { method: 'POST' });
    } finally {
      this.setToken(null);
    }
  }

  public async getMe(): Promise<UserSession> {
    return this.request<UserSession>('/auth/me');
  }
}

export const baseApiClient = new BaseApiClient();
