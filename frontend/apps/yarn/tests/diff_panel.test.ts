import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import DiffPanel from '../src/components/DiffPanel.svelte';
import type { DiffItem } from '../src/types';

describe('YARN DiffPanel Component', () => {
  const mockDiffs: DiffItem[] = [
    {
      path: 'root.users.alice',
      name: 'alice',
      partition: '[default]',
      action: 'modified',
      live_capacity: 20,
      draft_capacity: 30
    },
    {
      path: 'root.users.bob',
      name: 'bob',
      partition: '[default]',
      action: 'created',
      live_capacity: 0,
      draft_capacity: 10
    }
  ];

  it('does not render when isOpen is false', () => {
    render(DiffPanel, {
      props: {
        diffs: mockDiffs,
        canAdmin: true,
        isOpen: false,
        onGenerateXml: vi.fn()
      }
    });

    expect(screen.queryByText(/Changes Review/i)).not.toBeInTheDocument();
  });

  it('renders changes and change counts when isOpen is true', () => {
    render(DiffPanel, {
      props: {
        diffs: mockDiffs,
        canAdmin: true,
        isOpen: true,
        onGenerateXml: vi.fn()
      }
    });

    expect(screen.getByText(/Changes Review/i)).toBeInTheDocument();
    expect(screen.getByText(/2 change\(s\)/i)).toBeInTheDocument();
    expect(screen.getByText('root.users.alice')).toBeInTheDocument();
    expect(screen.getByText('root.users.bob')).toBeInTheDocument();
    expect(screen.getByText('modified')).toBeInTheDocument();
    expect(screen.getByText('created')).toBeInTheDocument();
  });

  it('triggers onGenerateXml callback', async () => {
    const handleXml = vi.fn();
    render(DiffPanel, {
      props: {
        diffs: mockDiffs,
        canAdmin: true,
        isOpen: true,
        onGenerateXml: handleXml
      }
    });

    const xmlBtn = screen.getByRole('button', { name: /Сгенерировать XML/i });
    await fireEvent.click(xmlBtn);
    expect(handleXml).toHaveBeenCalled();
  });

  it('renders partition badges and node label changes', () => {
    const labelDiffs: DiffItem[] = [
      {
        path: 'root.prod.spark',
        name: 'spark',
        partition: 'GPU',
        action: 'modified',
        live_capacity: 60,
        draft_capacity: 75,
        live_accessible_node_labels: ['DEFAULT'],
        draft_accessible_node_labels: ['DEFAULT', 'GPU'],
        live_default_node_label_expression: '',
        draft_default_node_label_expression: 'GPU'
      }
    ];

    render(DiffPanel, {
      props: {
        diffs: labelDiffs,
        canAdmin: true,
        isOpen: true,
        onGenerateXml: vi.fn()
      }
    });

    expect(screen.getByText('root.prod.spark')).toBeInTheDocument();
    expect(screen.getByText('GPU')).toBeInTheDocument();
    expect(screen.getByText(/Метки узлов:/i)).toBeInTheDocument();
    expect(screen.getByText(/DEFAULT → DEFAULT, GPU/i)).toBeInTheDocument();
    expect(screen.getByText(/Дефолтная метка:/i)).toBeInTheDocument();
  });
});
