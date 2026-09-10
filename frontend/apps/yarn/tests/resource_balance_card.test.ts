import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import ResourceBalanceCard from '../src/components/ResourceBalanceCard.svelte';
import type { BranchBalance } from '../src/types';

describe('YARN ResourceBalanceCard Component', () => {
  const balancedItem: BranchBalance = {
    parent_path: 'root',
    partition: '[default]',
    total_children_capacity: 100,
    unallocated_capacity: 0,
    is_balanced: true,
    status: 'balanced',
    message: 'All capacity allocated'
  };

  const underallocatedItem: BranchBalance = {
    parent_path: 'root.users',
    partition: '[default]',
    total_children_capacity: 75,
    unallocated_capacity: 25,
    is_balanced: false,
    status: 'underallocated',
    message: '25% unallocated'
  };

  const overallocatedItem: BranchBalance = {
    parent_path: 'root.analytics',
    partition: '[default]',
    total_children_capacity: 120,
    unallocated_capacity: -20,
    is_balanced: false,
    status: 'overallocated',
    message: 'Capacity exceeded by 20%'
  };

  it('renders balanced branches with green badge and 100%', () => {
    render(ResourceBalanceCard, {
      props: {
        balances: [balancedItem],
        resourceMode: 'percentage',
        displayMode: 'percentage'
      }
    });

    expect(screen.getByText('root:')).toBeInTheDocument();
    expect(screen.getByText('100.0%')).toBeInTheDocument();
  });

  it('renders underallocated and overallocated branches with warnings', () => {
    render(ResourceBalanceCard, {
      props: {
        balances: [underallocatedItem, overallocatedItem],
        resourceMode: 'percentage',
        displayMode: 'percentage'
      }
    });

    expect(screen.getByText('root.users:')).toBeInTheDocument();
    expect(screen.getByText(/остаток: 25.0%/)).toBeInTheDocument();

    expect(screen.getByText('root.analytics:')).toBeInTheDocument();
    expect(screen.getByText(/120.0%/)).toBeInTheDocument();
  });

  it('renders absolute resource mode with memory and vcores', () => {
    const absoluteItem: BranchBalance = {
      ...balancedItem,
      total_children_memory_mb: 65536, // 64 GB
      total_children_vcores: 32
    };

    render(ResourceBalanceCard, {
      props: {
        balances: [absoluteItem],
        resourceMode: 'absolute',
        displayMode: 'absolute'
      }
    });

    expect(screen.getByText('root:')).toBeInTheDocument();
    expect(screen.getByText(/64 GB|65536 MB/)).toBeInTheDocument();
    expect(screen.getByText(/32/)).toBeInTheDocument();
  });
});
