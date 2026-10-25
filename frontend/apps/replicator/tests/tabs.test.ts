import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ReplicatorApp from '../src/App.svelte';
import { api } from '../src/api/client';

const mockUser = {
  username: 'admin_user',
  display_name: 'Александр Админов',
  groups: ['hadoop-admins', 'hadoop-devs'],
  is_admin: true,
  auth_method: 'ldap',
  system_role: 'admin'
};

const mockTopology = {
  datacenters: [],
  clusters: [],
  global_limit_bytes_per_sec: 0,
  global_limit_mb_per_sec: 0,
  dc_limits: [],
  hdfs_limits: []
};

function mockJsonResponse(data: any, status = 200) {
  return Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers({ 'content-type': 'application/json' }),
    json: async () => data,
    text: async () => JSON.stringify(data)
  });
}

describe('Replicator Tabs Navigation Order', () => {
  beforeEach(() => {
    localStorage.clear();
    window.location.hash = '';

    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((url: string) => {
        if (url.includes('/auth/me') || url.includes('/auth/sso')) {
          return mockJsonResponse(mockUser);
        }
        if (url.includes('/api/v1/topology')) {
          return mockJsonResponse(mockTopology);
        }
        if (url.includes('/api/v1/hms/jobs')) {
          return mockJsonResponse([]);
        }
        if (url.includes('/api/v1/jobs/summary')) {
          // Счетчики статусов и авторы приходят сводкой по всему набору
          return mockJsonResponse({
            total: 0, running: 0, queued: 0, scheduled: 0, completed: 0, failed: 0, streaming: 0, authors: []
          });
        }
        if (url.includes('/api/v1/jobs')) {
          // Список заданий отдается страницей с обязательным пределом выборки
          return mockJsonResponse({ content: [], total_elements: 0, total_pages: 0, number: 0, size: 50 });
        }
        if (url.includes('/api/v1/agents')) {
          return mockJsonResponse([]);
        }
        if (url.includes('/api/v1/streaming/status')) {
          return mockJsonResponse({});
        }
        if (url.includes('/api/v1/dr/status')) {
          return mockJsonResponse({ emergency_stop_active: false, routes: [] });
        }
        return mockJsonResponse({});
      })
    );
  });

  it('renders HMS Replication as the 1st tab and HDFS Replication as the 2nd tab', async () => {
    render(ReplicatorApp);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /HMS Replication/i })).toBeInTheDocument();
    });

    const hmsTabButton = screen.getByRole('button', { name: /HMS Replication/i });
    const hdfsTabButton = screen.getByRole('button', { name: /HDFS Replication/i });
    const topologyTabButton = screen.getByRole('button', { name: /Топология ЦОД и Полоса/i });
    const drTabButton = screen.getByRole('button', { name: /Disaster Recovery/i });

    expect(hmsTabButton).toBeInTheDocument();
    expect(hdfsTabButton).toBeInTheDocument();
    expect(topologyTabButton).toBeInTheDocument();
    expect(drTabButton).toBeInTheDocument();

    // Проверяем взаимное расположение в DOM дереве (HMS идет раньше HDFS)
    const positionCompare = hmsTabButton.compareDocumentPosition(hdfsTabButton);
    expect(positionCompare & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    // Проверяем, что по умолчанию активна вкладка HMS Replication (bg-sky-600)
    expect(hmsTabButton.className).toContain('bg-sky-600');
    expect(hdfsTabButton.className).not.toContain('bg-sky-600');
  });

  it('switches to HDFS Replication tab on click and updates active styling', async () => {
    render(ReplicatorApp);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /HDFS Replication/i })).toBeInTheDocument();
    });

    const hmsTabButton = screen.getByRole('button', { name: /HMS Replication/i });
    const hdfsTabButton = screen.getByRole('button', { name: /HDFS Replication/i });

    await fireEvent.click(hdfsTabButton);

    await waitFor(() => {
      expect(hdfsTabButton.className).toContain('bg-sky-600');
      expect(hmsTabButton.className).not.toContain('bg-sky-600');
    });
  });
});
