import { describe, it, expect, vi, beforeEach } from 'vitest';
import { BaseApiClient } from '../api/client';

/**
 * Имитация логики Auth State для всех 4 приложений:
 * - HDFS (authStore)
 * - SQL (App.svelte state)
 * - YARN (App.svelte state)
 * - Spark (App.svelte state)
 */
class AppAuthLifecycleSimulator {
  public user: any = null;
  public authLoading: boolean = true;
  public authErrorMessage: string | null = null;
  private unsubscribe: (() => void) | null = null;

  constructor(private client: BaseApiClient) {
    this.unsubscribe = this.client.onUnauthorized((msg) => {
      this.user = null;
      this.authErrorMessage = msg;
    });
  }

  public async onMount() {
    this.authLoading = true;
    this.authErrorMessage = null;
    try {
      this.user = await this.client.getMe();
    } catch {
      try {
        const auto = await this.client.tryAutoLogin();
        this.user = auto ? (auto.user || auto) : null;
      } catch {
        this.user = null;
      }
    } finally {
      this.authLoading = false;
    }
  }

  public async handleLogin(u: string, p: string) {
    this.authLoading = true;
    this.authErrorMessage = null;
    try {
      const res = await this.client.login(u, p);
      this.user = res.user;
      return true;
    } catch (err: any) {
      this.authErrorMessage = err.message || 'Ошибка входа';
      return false;
    } finally {
      this.authLoading = false;
    }
  }

  public async handleLogout() {
    this.authLoading = true;
    try {
      await this.client.logout();
    } catch (_) {
    } finally {
      this.user = null;
      this.authErrorMessage = null;
      this.authLoading = false;
    }
  }

  public destroy() {
    if (this.unsubscribe) {
      this.unsubscribe();
      this.unsubscribe = null;
    }
  }
}

describe('Frontend Unified Auth Lifecycle (HDFS, SQL, YARN, Spark)', () => {
  let client: BaseApiClient;

  beforeEach(() => {
    client = new BaseApiClient('/api/v1');
    vi.restoreAllMocks();
  });

  it('Сценарий 1: Чистый начальный вход (F5 / первый визит неавторизованным) - нет ошибки', async () => {
    // getMe возвращает 401
    global.fetch = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ detail: 'Требуется авторизация' }),
    } as any);

    const app = new AppAuthLifecycleSimulator(client);
    await app.onMount();

    expect(app.authLoading).toBe(false);
    expect(app.user).toBeNull();
    // ГЛАВНОЕ: ошибка ДОЛЖНА быть null, чистая форма логина
    expect(app.authErrorMessage).toBeNull();
  });

  it('Сценарий 2: Фоновое истечение сессии / рестарт сервера при открытом приложении - показывает warning', async () => {
    const app = new AppAuthLifecycleSimulator(client);
    // Пользователь авторизован
    app.user = { username: 'admin_user', role: 'admin' };
    app.authLoading = false;

    // Внезапно фоновый запрос к /clusters возвращает 401
    global.fetch = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ detail: 'Token expired' }),
    } as any);

    await expect(client.request('/clusters')).rejects.toThrow();

    // Пользователь сброшен
    expect(app.user).toBeNull();
    // ГЛАВНОЕ: отображается предупреждение об истечении сессии
    expect(app.authErrorMessage).toBe('Сессия истекла или сервер был перезагружен. Пожалуйста, выполните вход.');
  });

  it('Сценарий 3: Успешная авторизация по логину и паролю', async () => {
    const app = new AppAuthLifecycleSimulator(client);
    app.authErrorMessage = 'Старая ошибка';

    global.fetch = vi.fn().mockResolvedValue({
      status: 200,
      ok: true,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ access_token: 'valid-jwt', user: { username: 'de_user' } }),
    } as any);

    const ok = await app.handleLogin('de_user', 'password123');
    expect(ok).toBe(true);
    expect(app.user).toEqual({ username: 'de_user' });
    expect(app.authErrorMessage).toBeNull();
  });

  it('Сценарий 4: Ошибка авторизации (неверный пароль) выставляет текст ошибки', async () => {
    const app = new AppAuthLifecycleSimulator(client);

    global.fetch = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ detail: 'Неверное имя пользователя или пароль' }),
    } as any);

    const ok = await app.handleLogin('wrong_user', 'wrong_pass');
    expect(ok).toBe(false);
    expect(app.user).toBeNull();
    expect(app.authErrorMessage).toBe('Неверное имя пользователя или пароль');
  });

  it('Сценарий 5: Штатный Logout очищает пользователя и ошибку', async () => {
    const app = new AppAuthLifecycleSimulator(client);
    app.user = { username: 'admin_user' };
    app.authErrorMessage = 'Какая-то ошибка';

    global.fetch = vi.fn().mockResolvedValue({
      status: 200,
      ok: true,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ success: true }),
    } as any);

    await app.handleLogout();
    expect(app.user).toBeNull();
    expect(app.authErrorMessage).toBeNull();
    expect(app.authLoading).toBe(false);
  });

  it('Сценарий 6: После штатного logout фоновый поллинг отменяется и не вызывает ложного warning', async () => {
    vi.useFakeTimers();
    const app = new AppAuthLifecycleSimulator(client);
    app.user = { username: 'spark_user' };

    let pollingTimer: any = null;
    let backgroundRequestsCount = 0;

    const schedulePoll = () => {
      if (pollingTimer) clearTimeout(pollingTimer);
      if (!app.user) return;
      pollingTimer = setTimeout(async () => {
        if (!app.user) return;
        backgroundRequestsCount++;
        await client.request('/sessions');
        if (app.user) schedulePoll();
      }, 5000);
    };

    schedulePoll();

    // Пользователь нажимает Logout
    global.fetch = vi.fn().mockResolvedValue({
      status: 200,
      ok: true,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ success: true }),
    } as any);

    if (pollingTimer) {
      clearTimeout(pollingTimer);
      pollingTimer = null;
    }
    await app.handleLogout();

    // Перематываем время на 10 секунд вперед
    vi.advanceTimersByTime(10000);

    // Никаких фоновых запросов не должно было уйти
    expect(backgroundRequestsCount).toBe(0);
    expect(app.authErrorMessage).toBeNull();
    expect(app.user).toBeNull();
    vi.useRealTimers();
  });
});
