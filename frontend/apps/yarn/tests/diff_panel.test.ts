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
});
