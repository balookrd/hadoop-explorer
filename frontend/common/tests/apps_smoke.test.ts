import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import HdfsApp from '../../apps/hdfs/src/App.svelte';
import SparkApp from '../../apps/spark/src/App.svelte';
import SqlApp from '../../apps/sql/src/App.svelte';
import YarnApp from '../../apps/yarn/src/App.svelte';

// Глобальный мок для fetch API во всех SPA приложениях
const mockUser = {
  username: 'admin_user',
  display_name: 'Александр Админов',
  groups: ['hadoop-admins', 'hadoop-devs'],
  is_admin: true,
  auth_method: 'ldap',
  system_role: 'admin'
};

const mockClusters = [
  { id: 'cluster-1', name: 'Primary Cluster', type: 'trino', default_path: '/user/hadoop', is_admin: true }
];

describe('All 4 SPA Applications Smoke Mounting Tests', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((url: string) => {
        if (url.includes('/auth/me') || url.includes('/auth/sso')) {
          return Promise.resolve({
            ok: true,
            status: 200,
            json: async () => mockUser
          });
        }
        if (url.includes('/clusters/')) {
          return Promise.resolve({
            ok: true,
            status: 200,
            json: async () => ({ id: 'cluster-1', name: 'Primary Cluster', type: 'spark', metastores: [], queues: [] })
          });
        }
        if (url.includes('/clusters')) {
          return Promise.resolve({
            ok: true,
            status: 200,
            json: async () => mockClusters
          });
        }
        if (url.includes('/sessions') || url.includes('/queues') || url.includes('/files') || url.includes('/catalogs')) {
          return Promise.resolve({
            ok: true,
            status: 200,
            json: async () => []
          });
        }
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({})
        });
      })
    );
  });

  it('mounts HDFS Explorer App without runtime exceptions', async () => {
    const { container } = render(HdfsApp);
    expect(container).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText(/HDFS Explorer/i)).toBeInTheDocument();
    });
  });

  it('mounts Spark Explorer App without runtime exceptions', async () => {
    const { container } = render(SparkApp);
    expect(container).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText(/Spark Explorer/i)).toBeInTheDocument();
    });
  });

  it('mounts SQL Explorer App without runtime exceptions', async () => {
    const { container } = render(SqlApp);
    expect(container).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getAllByText(/SQL/i).length).toBeGreaterThan(0);
    });
  });

  it('mounts YARN Explorer App without runtime exceptions', async () => {
    const { container } = render(YarnApp);
    expect(container).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText(/YARN Explorer/i)).toBeInTheDocument();
    });
  });
});
