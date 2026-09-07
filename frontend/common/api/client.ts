type UnauthorizedHandler = (message: string) => void;

export class BaseApiClient {
  // Токен хранится только в оперативной памяти JS для текущей сессии (Zero LocalStorage),
  // предотвращая постоянную компрометацию через XSS.
  // Основная аутентификация в браузере опирается на безопасные HttpOnly Cookie (Cookie-first).
  protected token: string | null = null;
  protected baseUrl: string;
  private unauthorizedHandlers: Set<UnauthorizedHandler> = new Set();
  private isAttemptingSso: boolean = false;

  constructor(baseUrl: string = '/api/v1') {
    this.baseUrl = baseUrl;
    this.token = null;
    this.clearLegacyStorage();
  }

  private clearLegacyStorage() {
    if (typeof localStorage !== 'undefined') {
      try {
        localStorage.removeItem('access_token');
      } catch (_) {
        // ignore localStorage access restrictions
      }
    }
  }

  public onUnauthorized(handler: UnauthorizedHandler): () => void {
    this.unauthorizedHandlers.add(handler);
    return () => this.unauthorizedHandlers.delete(handler);
  }

  public notifyUnauthorized(message: string = 'Сессия истекла или сервер был перезагружен. Пожалуйста, выполните вход.') {
    this.clearToken();
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

  public clearToken() {
    this.token = null;
    this.clearLegacyStorage();
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

    let response: Response;
    try {
      response = await fetch(url, {
        ...options,
        headers,
        credentials: 'include', // Отправка HttpOnly cookies
      });
    } catch (netErr: any) {
      throw new Error(`Сервер недоступен или перезагружается (${netErr.message || 'сетевая ошибка'})`);
    }

    const isAuthEndpoint = path.includes('/auth/login') || path.includes('/auth/sso') || path.includes('/auth/logout') || path.includes('/auth/me');

    if (response.status === 401) {
      if (!isAuthEndpoint && !this.isAttemptingSso) {
        this.isAttemptingSso = true;
        try {
          const ssoRes = await this.kerberosNegotiate();
          if (ssoRes && (ssoRes.access_token || ssoRes.user)) {
            this.isAttemptingSso = false;
            return await this.request<T>(path, options);
          }
        } catch {
          // SSO не сработал
        } finally {
          this.isAttemptingSso = false;
        }

        const errDetail = 'Сессия истекла или сервер был перезагружен. Пожалуйста, выполните вход.';
        this.notifyUnauthorized(errDetail);
        throw new Error(errDetail);
      } else {
        this.clearToken();
        let errDetail = 'Требуется авторизация';
        try {
          const errorJson = await response.json();
          if (errorJson.detail) {
            errDetail = typeof errorJson.detail === 'string' ? errorJson.detail : JSON.stringify(errorJson.detail);
          }
        } catch {}
        throw new Error(errDetail);
      }
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
      this.clearToken();
    }
  }

  public async getMe(): Promise<UserSession> {
    return this.request<UserSession>('/auth/me');
  }
}

export const baseApiClient = new BaseApiClient();
