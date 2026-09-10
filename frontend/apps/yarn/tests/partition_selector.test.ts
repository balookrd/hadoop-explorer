import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import PartitionSelector from '../src/components/PartitionSelector.svelte';

describe('YARN PartitionSelector Component', () => {
  it('renders all partition buttons and highlights active partition', () => {
    const partitions = ['[default]', 'gpu-nodes', 'high-mem'];
    render(PartitionSelector, {
      props: {
        partitions,
        selectedPartition: '[default]'
      }
    });

    expect(screen.getByText('Partition:')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '[default]' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'gpu-nodes' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'high-mem' })).toBeInTheDocument();

    const activeBtn = screen.getByRole('button', { name: '[default]' });
    expect(activeBtn.className).toContain('bg-sky-600');

    const inactiveBtn = screen.getByRole('button', { name: 'gpu-nodes' });
    expect(inactiveBtn.className).not.toContain('bg-sky-600');
  });

  it('allows switching active partition on button click', async () => {
    const partitions = ['[default]', 'gpu'];
    let current = '[default]';

    const { rerender } = render(PartitionSelector, {
      props: {
        partitions,
        selectedPartition: current
      }
    });

    const gpuBtn = screen.getByRole('button', { name: 'gpu' });
    await fireEvent.click(gpuBtn);

    // В Svelte 5 с bindable props компонент обновляет локальное состояние
    expect(gpuBtn.className).toContain('bg-sky-600');
  });
});
