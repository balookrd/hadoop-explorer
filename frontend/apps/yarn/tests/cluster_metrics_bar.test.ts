import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import ClusterMetricsBar from '../src/components/ClusterMetricsBar.svelte';
import type { ClusterMetrics } from '../src/types';

describe('YARN ClusterMetricsBar Component', () => {
  const mockMetrics: ClusterMetrics = {
    total_memory_mb: 2097152, // 2.0 TB
    allocated_memory_mb: 1048576, // 1.0 TB
    available_memory_mb: 1048576,
    total_vcores: 128,
    allocated_vcores: 64,
    available_vcores: 64,
    active_nodes: 16,
    unhealthy_nodes: 1,
    total_containers: 42,
    running_apps: 8,
    partitions: ['[default]', 'gpu']
  };

  it('renders cluster metrics with formatted values when metrics are provided', () => {
    render(ClusterMetricsBar, {
      props: { metrics: mockMetrics }
    });

    expect(screen.getByText('Total Memory')).toBeInTheDocument();
    expect(screen.getByText('2.0 TB')).toBeInTheDocument();
    expect(screen.getByText(/Allocated: 1.0 TB/)).toBeInTheDocument();

    expect(screen.getByText('Total Vcores')).toBeInTheDocument();
    expect(screen.getByText('128')).toBeInTheDocument();

    expect(screen.getByText('Available Memory')).toBeInTheDocument();
    expect(screen.getByText('1.0 TB')).toBeInTheDocument();

    expect(screen.getByText('Available Vcores')).toBeInTheDocument();
    expect(screen.getByText('64')).toBeInTheDocument();

    expect(screen.getByText('Active Nodes')).toBeInTheDocument();
    expect(screen.getByText('16')).toBeInTheDocument();

    expect(screen.getByText('Running Apps')).toBeInTheDocument();
    expect(screen.getByText('8')).toBeInTheDocument();
    expect(screen.getByText('Containers: 42')).toBeInTheDocument();
  });

  it('does not render anything when metrics is null', () => {
    render(ClusterMetricsBar, {
      props: { metrics: null }
    });

    expect(screen.queryByText('Total Memory')).not.toBeInTheDocument();
  });

  it('formats smaller memory correctly in GB and MB', () => {
    const smallMetrics: ClusterMetrics = {
      ...mockMetrics,
      total_memory_mb: 8192, // 8.0 GB
      available_memory_mb: 512 // 512 MB
    };

    render(ClusterMetricsBar, {
      props: { metrics: smallMetrics }
    });

    expect(screen.getByText('8.0 GB')).toBeInTheDocument();
    expect(screen.getByText('512 MB')).toBeInTheDocument();
  });
});
