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

const TOTAL_JOBS = 120;
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

function jobsPage(page: number, size: number) {
  const from = page * size;
  const count = Math.max(0, Math.min(size, TOTAL_JOBS - from));
  return {
    content: Array.from({ length: count }, (_, i) => ({
      id: `job-${from + i}`,
      source_cluster_id: 'prod-a',
      target_cluster_id: 'prod-b',
      source_path: `/warehouse/part-${from + i}`,
      target_path: `/backup/warehouse/part-${from + i}`,
      status: 'COMPLETED',
      created_by: 'alice',
      total_bytes: 0,
      copied_bytes: 0,
      sync_mode: 'MANUAL'
    })),
    total_elements: TOTAL_JOBS,
    total_pages: Math.ceil(TOTAL_JOBS / size),
    number: page,
    size
  };
}

/**
 * Навигация по страницам списка заданий.
 *
 * <p>Список приходит с сервера постранично, поэтому переход по страницам — это запрос с другим
 * `page`, а не отбор в браузере. Тест следит именно за этим: что нажатия меняют запрашиваемую
 * страницу и что крайние кнопки выключены на границах.
 */
describe('Навигация по страницам заданий', () => {
  let requestedJobUrls: string[] = [];

  beforeEach(() => {
    localStorage.clear();
    window.location.hash = '';
    requestedJobUrls = [];

    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((url: string) => {
        if (url.includes('/auth/me') || url.includes('/auth/sso')) {
          return mockJsonResponse(mockUser);
        }
        if (url.includes('/api/v1/jobs/summary')) {
          return mockJsonResponse({
            total: TOTAL_JOBS, running: 0, queued: 0, scheduled: 0,
            completed: TOTAL_JOBS, failed: 0, streaming: 0, authors: ['alice']
          });
        }
        if (url.includes('/api/v1/jobs')) {
          requestedJobUrls.push(url);
          const params = new URLSearchParams(url.split('?')[1] ?? '');
          return mockJsonResponse(jobsPage(Number(params.get('page') ?? 0), Number(params.get('size') ?? PAGE_SIZE)));
        }
        return mockJsonResponse([]);
      })
    );
  });

  async function renderJobsTab() {
    render(ReplicatorApp);
    await waitFor(() => expect(requestedJobUrls.length).toBeGreaterThan(0));
    const jobsTab = await screen.findByRole('button', { name: /HDFS Replication/i });
    await fireEvent.click(jobsTab);
    return screen.findByText(`1–${PAGE_SIZE} из ${TOTAL_JOBS}`);
  }

  it('первая страница показывает свой диапазон и блокирует переход назад', async () => {
    await renderJobsTab();

    const back = screen.getByRole('button', { name: 'Назад' });
    expect(back).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Вперед' })).not.toBeDisabled();
    expect(screen.getByText('1 / 3')).toBeTruthy();
  });

  it('кнопка «Вперед» запрашивает следующую страницу у сервера', async () => {
    await renderJobsTab();

    await fireEvent.click(screen.getByRole('button', { name: 'Вперед' }));

    await waitFor(() => expect(screen.getByText('2 / 3')).toBeTruthy());
    expect(requestedJobUrls.some((url) => url.includes('page=1'))).toBe(true);
    expect(await screen.findByText(`51–100 из ${TOTAL_JOBS}`)).toBeTruthy();
  });

  it('на последней странице показан остаток и выключен переход вперед', async () => {
    await renderJobsTab();

    await fireEvent.click(screen.getByRole('button', { name: 'Последняя страница' }));

    await waitFor(() => expect(screen.getByText('3 / 3')).toBeTruthy());
    expect(await screen.findByText(`101–${TOTAL_JOBS} из ${TOTAL_JOBS}`)).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Вперед' })).toBeDisabled();
  });

  it('смена размера страницы возвращает на первую страницу', async () => {
    await renderJobsTab();
    await fireEvent.click(screen.getByRole('button', { name: 'Вперед' }));
    await waitFor(() => expect(screen.getByText('2 / 3')).toBeTruthy());

    const sizeSelect = screen.getByTitle('Сколько задач показывать на странице');
    await fireEvent.change(sizeSelect, { target: { value: '25' } });

    await waitFor(() => expect(screen.getByText('1 / 5')).toBeTruthy());
    expect(requestedJobUrls.some((url) => url.includes('size=25') && url.includes('page=0'))).toBe(true);
  });
});
