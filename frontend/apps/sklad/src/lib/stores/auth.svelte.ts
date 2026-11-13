import { api } from '../api/client';
import type { UserInfo } from '../types';

class AuthState {
  user = $state<UserInfo | null>(null);
  loading = $state<boolean>(true);
  error = $state<string | null>(null);

  constructor() {
    api.onUnauthorized((msg) => {
      this.user = null;
      this.error = msg;
    });
  }

  get isAuthenticated(): boolean {
    return this.user !== null;
  }

  async init() {
    this.loading = true;
    this.error = null;
    try {
      this.user = await api.getMe();
    } catch {
      try {
        const auto = await api.tryAutoLogin();
        this.user = auto ? ((auto as any).user || auto) : null;
      } catch {
        this.user = null;
      }
    } finally {
      this.loading = false;
    }
  }

  async login(username: string, pass: string): Promise<boolean> {
    this.loading = true;
    this.error = null;
    try {
      const resp = await api.login(username, pass);
      this.user = resp.user || (await api.getMe());
      return true;
    } catch (err: any) {
      this.error = err.message || 'Ошибка входа';
      return false;
    } finally {
      this.loading = false;
    }
  }

  async logout() {
    this.loading = true;
    this.error = null;
    try {
      await api.logout();
    } catch (e) {
      console.error(e);
    } finally {
      this.user = null;
      this.loading = false;
    }
  }
}

export const authStore = new AuthState();
