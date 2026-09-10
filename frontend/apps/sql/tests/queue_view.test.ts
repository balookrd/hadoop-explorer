import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/svelte';
import QueueView from '../src/components/QueueView.svelte';
import { api } from '../src/api/client';
import type { QueryHistoryItem } from '../src/types';

describe('SQL QueueView Component', () => {
  const mockQueue: QueryHistoryItem[] = [
    {
      id: 'q-101',
      cluster_name: 'Trino-Analytics',
      query_text: 'SELECT count(*) FROM hive.sales.orders',
      status: 'RUNNING',
      start_time: '2026-09-10T12:00:00Z',
      execution_time_ms: 1200,
      rows_count: 0
    },
    {
      id: 'q-102',
      cluster_name: 'Trino-Analytics',
      query_text: 'SELECT * FROM hive.users.profiles LIMIT 10',
      status: 'FINISHED',
      start_time: '2026-09-10T11:58:00Z',
      execution_time_ms: 340,
      rows_count: 10
    }
  ];

  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('renders queue items and active count after loading', async () => {
    vi.spyOn(api, 'getQueue').mockResolvedValue(mockQueue);

    render(QueueView, {
      props: {
        onLoadResult: vi.fn(),
        onInsertQuery: vi.fn()
      }
    });

    await waitFor(() => {
      expect(screen.getByText(/Очередь задач \(1 активных\)/)).toBeInTheDocument();
      expect(screen.getByText('Исполняется...')).toBeInTheDocument();
      expect(screen.getByText('Завершен')).toBeInTheDocument();
    });

    expect(screen.getAllByText('Trino-Analytics').length).toBe(2);
    expect(screen.getByText('SELECT count(*) FROM hive.sales.orders')).toBeInTheDocument();
  });

  it('renders empty queue message when no tasks are present', async () => {
    vi.spyOn(api, 'getQueue').mockResolvedValue([]);

    render(QueueView, {
      props: {
        onLoadResult: vi.fn(),
        onInsertQuery: vi.fn()
      }
    });

    await waitFor(() => {
      expect(screen.getByText(/В очереди нет задач/)).toBeInTheDocument();
    });
  });

  it('refreshes queue on refresh button click', async () => {
    const getQueueSpy = vi.spyOn(api, 'getQueue').mockResolvedValue([]);

    render(QueueView, {
      props: {
        onLoadResult: vi.fn(),
        onInsertQuery: vi.fn()
      }
    });

    const refreshBtn = screen.getByTitle('Обновить очередь');
    await fireEvent.click(refreshBtn);

    expect(getQueueSpy).toHaveBeenCalled();
  });
});
