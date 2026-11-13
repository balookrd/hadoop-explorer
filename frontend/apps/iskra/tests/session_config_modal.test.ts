import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import SessionConfigModal from '../src/components/SessionConfigModal.svelte';
import type { ClusterDetailResponse } from '../src/types';

describe('Spark SessionConfigModal Component', () => {
  const mockCluster: ClusterDetailResponse = {
    id: 'cluster-spark-01',
    name: 'Spark Analytical Cluster',
    description: 'YARN-managed Spark 3.5',
    type: 'spark',
    yarn_cluster_id: 'yarn-prod',
    spark_versions: [
      {
        id: 'spark-3.5',
        name: 'Apache Spark 3.5.0',
        is_default: true,
        python_versions: [
          { id: 'py310', name: 'Python 3.10', is_default: true }
        ]
      }
    ],
    metastores: [
      { id: 'hive-meta', name: 'Hive Metastore (default)', is_default: true }
    ],
    yarn_queues: ['default', 'analytics', 'batch'],
    default_queue: 'analytics',
    resource_profiles: [
      { id: 'small', name: 'Small (1 driver, 2 executors)', driver_memory_mb: 2048, executor_memory_mb: 4096, executor_cores: 2 }
    ],
    default_repositories: []
  };

  it('does not render when isOpen is false', () => {
    render(SessionConfigModal, {
      props: {
        isOpen: false,
        clusterDetails: mockCluster,
        initialValues: {},
        onClose: vi.fn(),
        onSave: vi.fn()
      }
    });

    expect(screen.queryByText('Параметры сессии Spark')).not.toBeInTheDocument();
  });

  it('renders modal with cluster details and confirms with onSave', async () => {
    const handleSave = vi.fn();
    const handleClose = vi.fn();

    render(SessionConfigModal, {
      props: {
        isOpen: true,
        clusterDetails: mockCluster,
        initialValues: {},
        onClose: handleClose,
        onSave: handleSave
      }
    });

    expect(screen.getByText('Параметры сессии Spark')).toBeInTheDocument();
    expect(screen.getByText('Spark Analytical Cluster')).toBeInTheDocument();

    const saveBtn = screen.getByRole('button', { name: /Применить и подключить/i });
    await fireEvent.click(saveBtn);

    expect(handleSave).toHaveBeenCalled();
    const payload = handleSave.mock.calls[0][0];
    expect(payload.spark_version_id).toBe('spark-3.5');
    expect(payload.yarn_queue).toBe('analytics');
    expect(payload.resource_profile).toBe('small');
  });

  it('calls onClose when clicking cancel button', async () => {
    const handleClose = vi.fn();

    render(SessionConfigModal, {
      props: {
        isOpen: true,
        clusterDetails: mockCluster,
        initialValues: {},
        onClose: handleClose,
        onSave: vi.fn()
      }
    });

    const cancelBtn = screen.getByRole('button', { name: /Отмена/i });
    await fireEvent.click(cancelBtn);

    expect(handleClose).toHaveBeenCalled();
  });
});
