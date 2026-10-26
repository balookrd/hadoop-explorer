import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ReplicatorApp from '../src/App.svelte';

const mockUser = {
  username: 'admin_user',
  display_name: 'Александр Админов',
  groups: ['hadoop-admins'],
  is_admin: true,
  auth_method: 'ldap',
  system_role: 'admin'
};

const TOTAL_HMS_JOBS = 120;
const PAGE_SIZE = 50;

function mockJsonResponse(data: unknown, status = 200) {
  return Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers({ 'content-type': 'application/json' }),
    json: async () => data,
    text: async () => JSON.stringify(data)
  });
}

function hmsJobsPage(page: number, size: number, status?: string | null, search?: string | null) {
  // Сервер отбирает сам: при фильтре набор другой, и это ровно то, что проверяется
  const total = status || search ? 1 : TOTAL_HMS_JOBS;
  const from = page * size;
  const count = Math.max(0, Math.min(size, total - from));
  return {
    content: Array.from({ length: count }, (_, i) => ({
      id: `hms-job-${from + i}`,
      source_cluster_id: 'dc1',
      target_cluster_id: 'dc2',
      source_db_name: `analytics_${from + i}`,
      target_db_name: `analytics_${from + i}_replica`,
      table_pattern: '*',
      status: 'ACTIVE',
      created_by: 'alice',
      replicated_tables: 1,
      replicated_partitions: 2
    })),
    total_elements: total,
    total_pages: Math.ceil(total / size),
    number: page,
    size
  };
}

/**
 * Навигация по страницам списка заданий метастора.
 *
 * <p>Эндпоинт отдавал весь список разом. Теперь он страничный, поэтому переход по страницам —
 * запрос с другим `page`, а счетчики в шапке приходят отдельной сводкой: по странице их не
 * посчитать.
 */
describe('Навигация по страницам заданий метастора', () => {
  let requestedHmsUrls: string[] = [];
  let summaryRequests = 0;

  beforeEach(() => {
    localStorage.clear();
    window.location.hash = '';
    requestedHmsUrls = [];
    summaryRequests = 0;

    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((url: string) => {
        if (url.includes('/auth/me') || url.includes('/auth/sso')) {
          return mockJsonResponse(mockUser);
        }
        if (url.includes('/api/v1/hms/jobs/summary')) {
          summaryRequests += 1;
          return mockJsonResponse({
            total: TOTAL_HMS_JOBS,
            by_status: { ACTIVE: 100, PAUSED: 20 },
            replicated_tables: 480,
            replicated_partitions: 9600
          });
        }
        if (url.includes('/api/v1/hms/jobs')) {
          requestedHmsUrls.push(url);
          const params = new URLSearchParams(url.split('?')[1] ?? '');
          return mockJsonResponse(hmsJobsPage(
            Number(params.get('page') ?? 0),
            Number(params.get('size') ?? PAGE_SIZE),
            params.get('status'),
            params.get('search')
          ));
        }
        if (url.includes('/api/v1/jobs/summary')) {
          return mockJsonResponse({
            total: 0, running: 0, queued: 0, scheduled: 0,
            completed: 0, failed: 0, streaming: 0, authors: []
          });
        }
        if (url.includes('/api/v1/jobs')) {
          return mockJsonResponse({ content: [], total_elements: 0, total_pages: 0, number: 0, size: PAGE_SIZE });
        }
        return mockJsonResponse([]);
      })
    );
  });

  async function renderHmsTab() {
    render(ReplicatorApp);
    const hmsTab = await screen.findByRole('button', { name: /HMS Replication/i });
    await fireEvent.click(hmsTab);
    await waitFor(() => expect(requestedHmsUrls.length).toBeGreaterThan(0));
    return screen.findByText(`1–${PAGE_SIZE} из ${TOTAL_HMS_JOBS}`);
  }

  it('первая страница показывает свой диапазон и блокирует переход назад', async () => {
    await renderHmsTab();

    expect(screen.getByRole('button', { name: 'Назад' })).toBeDisabled();
    expect(screen.getByText('1 / 3')).toBeTruthy();
    expect(requestedHmsUrls.some((url) => url.includes('page=0'))).toBe(true);
  });

  it('кнопка «Вперед» запрашивает следующую страницу у сервера', async () => {
    await renderHmsTab();

    await fireEvent.click(screen.getByRole('button', { name: 'Вперед' }));

    await waitFor(() => expect(screen.getByText('2 / 3')).toBeTruthy());
    expect(requestedHmsUrls.some((url) => url.includes('page=1'))).toBe(true);
    expect(await screen.findByText(`51–100 из ${TOTAL_HMS_JOBS}`)).toBeTruthy();
  });

  it('счетчики в шапке берутся из сводки, а не из загруженной страницы', async () => {
    await renderHmsTab();

    await waitFor(() => expect(summaryRequests).toBeGreaterThan(0));
    // На странице 50 заданий, а в сводке 120: в шапке должно стоять число по всему набору
    expect(await screen.findByText(String(TOTAL_HMS_JOBS))).toBeTruthy();
    expect(screen.getByText('9600')).toBeTruthy();
  });

  it('фильтр по статусу уходит на сервер, а не применяется в браузере', async () => {
    await renderHmsTab();

    await fireEvent.click(screen.getByRole('button', { name: /На паузе/i }));

    await waitFor(() => expect(requestedHmsUrls.some((url) => url.includes('status=PAUSED'))).toBe(true));
    // Набор пришел другой, значит отбирал сервер
    expect(await screen.findByText('1–1 из 1')).toBeTruthy();
  });

  it('поиск уходит на сервер отдельным запросом', async () => {
    await renderHmsTab();

    const search = screen.getByPlaceholderText(/Поиск/i);
    await fireEvent.input(search, { target: { value: 'finance' } });

    await waitFor(
      () => expect(requestedHmsUrls.some((url) => url.includes('search=finance'))).toBe(true),
      { timeout: 2000 }
    );
  });
});
