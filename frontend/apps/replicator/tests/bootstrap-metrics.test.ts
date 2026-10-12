import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import HmsReplicationView from '../src/components/HmsReplicationView.svelte';

const mockUser = {
  username: 'admin_user',
  display_name: 'Александр Админов',
  groups: ['hadoop-admins'],
  is_admin: true,
  auth_method: 'ldap',
  system_role: 'admin'
};

const mockTopology = {
  datacenters: [
    { id: 'dc1', name: 'Датацентр 1', location: 'Москва' },
    { id: 'dc2', name: 'Датацентр 2', location: 'СПб' }
  ],
  clusters: [
    { id: 'dc1', name: 'Cluster DC1', dc_id: 'dc1', dc_name: 'Москва', active_nodes: 5, total_nodes: 5, is_active: true },
    { id: 'dc2', name: 'Cluster DC2', dc_id: 'dc2', dc_name: 'СПб', active_nodes: 5, total_nodes: 5, is_active: true }
  ],
  global_limit_bytes_per_sec: 0,
  global_limit_mb_per_sec: 0,
  dc_limits: [],
  hdfs_limits: []
};

const now = new Date();
const startTime = new Date(now.getTime() - 1000 * 60 * 10).toISOString(); // 10 минут назад
const etaTime = new Date(now.getTime() + 1000 * 60 * 15).toISOString(); // через 15 минут

const mockHmsJobs = [
  {
    id: 'hms-job-bootstrap-01',
    source_cluster_id: 'dc1',
    target_cluster_id: 'dc2',
    source_db_name: 'sales_source_db',
    target_db_name: 'sales_target_db',
    table_pattern: '*',
    status: 'BOOTSTRAPPING',
    last_processed_event_id: 0,
    event_lag: 0,
    total_tables: 10,
    replicated_tables: 4,
    total_partitions: 50,
    replicated_partitions: 20,
    execution_principal: 'admin_user',
    created_at: startTime,
    started_at: startTime,
    completed_at: null,
    estimated_completion_at: etaTime,
    average_speed_mb_s: 28.5,
    total_bytes: 1024 * 1024 * 1024 * 20, // 20 GB
    copied_bytes: 1024 * 1024 * 1024 * 8, // 8 GB
    progress_percent: 40,
    subtasks_count: 5,
    active_subtasks_count: 3
  }
];

function mockJsonResponse(data: any, status = 200) {
  return Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers({ 'content-type': 'application/json' }),
    json: async () => data,
    text: async () => JSON.stringify(data)
  });
}

describe('HMS Replication Bootstrap Sync Metrics', () => {
  beforeEach(() => {
    localStorage.clear();

    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((url: string) => {
        if (url.includes('/api/v1/topology')) {
          return mockJsonResponse(mockTopology);
        }
        if (url.includes('/api/v1/hms/jobs')) {
          return mockJsonResponse(mockHmsJobs);
        }
        if (url.includes('/events')) {
          return mockJsonResponse([]);
        }
        if (url.includes('/subtasks')) {
          return mockJsonResponse([]);
        }
        return mockJsonResponse({});
      })
    );
  });

  it('displays overall replication speed, start time, and estimated completion time in table row', async () => {
    render(HmsReplicationView, {
      props: {
        topology: mockTopology as any,
        user: mockUser as any
      }
    });

    // Ожидаем отрисовки задачи репликации
    await waitFor(() => {
      expect(screen.getByText('sales_source_db')).toBeInTheDocument();
    });

    // Проверяем статус BOOTSTRAPPING
    expect(screen.getByText('BOOTSTRAPPING')).toBeInTheDocument();

    // Проверяем наличие блока метрик: Ср. скорость, Старт, Окончание
    expect(screen.getByText('Ср. скорость:')).toBeInTheDocument();
    expect(screen.getByText(/28\.5 МБ\/с/)).toBeInTheDocument();

    expect(screen.getByText('Старт:')).toBeInTheDocument();
    expect(screen.getByText('Окончание:')).toBeInTheDocument();
  });
});
