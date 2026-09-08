import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BaseApiClient } from '../api/client';

describe('BaseApiClient - Auth & Session Expiration Flow', () => {
  let client: BaseApiClient;
  const originalFetch = global.fetch;

  beforeEach(() => {
    client = new BaseApiClient('/api/v1');
    vi.restoreAllMocks();
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('1. Не вызывает onUnauthorized при 401 на эндпоинте /auth/me (чистый начальный вход)', async () => {
    const unauthorizedSpy = vi.fn();
    client.onUnauthorized(unauthorizedSpy);

    global.fetch = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ detail: 'Требуется авторизация' }),
    } as any);

    await expect(client.getMe()).rejects.toThrow('Требуется авторизация');
    expect(unauthorizedSpy).not.toHaveBeenCalled();
  });

  it('2. Не вызывает onUnauthorized при 401 на эндпоинте /auth/login (неверный пароль)', async () => {
    const unauthorizedSpy = vi.fn();
    client.onUnauthorized(unauthorizedSpy);

    global.fetch = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ detail: 'Неверный логин или пароль' }),
    } as any);

    await expect(client.login('bad_user', 'bad_pass')).rejects.toThrow('Неверный логин или пароль');
    expect(unauthorizedSpy).not.toHaveBeenCalled();
  });

  it('3. Вызывает onUnauthorized с предупреждением об истечении сессии при 401 на эндпоинтах данных (/clusters, /files, /sessions)', async () => {
    const unauthorizedSpy = vi.fn();
    client.onUnauthorized(unauthorizedSpy);

    global.fetch = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ detail: 'Токен истек' }),
    } as any);

    await expect(client.request('/clusters')).rejects.toThrow('Сессия истекла или сервер был перезагружен');
    expect(unauthorizedSpy).toHaveBeenCalledTimes(1);
    expect(unauthorizedSpy).toHaveBeenCalledWith('Сессия истекла или сервер был перезагружен. Пожалуйста, выполните вход.');
  });

  it('4. Отписка через unsubscribe callback корректно удаляет слушателя', async () => {
    const unauthorizedSpy = vi.fn();
    const unsubscribe = client.onUnauthorized(unauthorizedSpy);

    unsubscribe();

    global.fetch = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ detail: 'Токен истек' }),
    } as any);

    await expect(client.request('/sessions')).rejects.toThrow();
    expect(unauthorizedSpy).not.toHaveBeenCalled();
  });

  it('5. Всегда передает заголовок защиты от CSRF X-Requested-With: XMLHttpRequest', async () => {
    let capturedHeaders: Headers | null = null;
    global.fetch = vi.fn().mockImplementation((url, options) => {
      capturedHeaders = new Headers(options.headers);
      return Promise.resolve({
        status: 200,
        ok: true,
        headers: new Headers({ 'content-type': 'application/json' }),
        json: async () => ({ status: 'ok' }),
      });
    });

    await client.request('/status');
    expect(capturedHeaders).not.toBeNull();
    expect((capturedHeaders as unknown as Headers).get('X-Requested-With')).toBe('XMLHttpRequest');
  });

  it('6. Успешный login сохраняет токен и передает его в заголовке Authorization', async () => {
    global.fetch = vi.fn()
      .mockResolvedValueOnce({
        status: 200,
        ok: true,
        headers: new Headers({ 'content-type': 'application/json' }),
        json: async () => ({ access_token: 'mock-jwt-token-123', user: { username: 'admin_user' } }),
      } as any)
      .mockImplementationOnce((url, options) => {
        const headers = new Headers(options.headers);
        expect(headers.get('Authorization')).toBe('Bearer mock-jwt-token-123');
        return Promise.resolve({
          status: 200,
          ok: true,
          headers: new Headers({ 'content-type': 'application/json' }),
          json: async () => ({ data: 'secret' }),
        });
      });

    const res = await client.login('admin_user', 'password123');
    expect(res.access_token).toBe('mock-jwt-token-123');
    expect(client.getToken()).toBe('mock-jwt-token-123');

    await client.request('/secure-data');
  });

  it('7. logout сбрасывает токен', async () => {
    client.setToken('existing-token');
    expect(client.getToken()).toBe('existing-token');

    global.fetch = vi.fn().mockResolvedValue({
      status: 200,
      ok: true,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ success: true }),
    } as any);

    await client.logout();
    expect(client.getToken()).toBeNull();
  });
});
