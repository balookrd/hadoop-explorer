import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import ResultsView from '../src/components/ResultsView.svelte';
import type { ColumnMeta } from '../src/types';

describe('Spark ResultsView Component', () => {
  const mockColumns: ColumnMeta[] = [
    { name: 'app_id', type: 'string' },
    { name: 'records_processed', type: 'long' }
  ];

  const mockRows = [
    ['app_spark_001', 150000],
    ['app_spark_002', 84000]
  ];

  it('renders DataFrame table columns, rows and row numbers', () => {
    render(ResultsView, {
      props: {
        columns: mockColumns,
        rows: mockRows,
        totalRows: 2,
        logs: '',
        errorMessage: null,
        executionTimeMs: 1200,
        activeTab: 'table'
      }
    });

    expect(screen.getByText('app_id')).toBeInTheDocument();
    expect(screen.getByText('records_processed')).toBeInTheDocument();
    expect(screen.getByText('app_spark_001')).toBeInTheDocument();
    expect(screen.getByText('150000')).toBeInTheDocument();
    expect(screen.getByText('app_spark_002')).toBeInTheDocument();
  });

  it('switches between DataFrame table and Spark logs tabs', async () => {
    const handleTabChange = vi.fn();
    const mockLogs = 'INFO DAGScheduler: Submitting 4 missing tasks from ResultStage 0';

    render(ResultsView, {
      props: {
        columns: mockColumns,
        rows: mockRows,
        totalRows: 2,
        logs: mockLogs,
        errorMessage: null,
        executionTimeMs: 0,
        activeTab: 'table',
        onTabChange: handleTabChange
      }
    });

    const logsTabBtn = screen.getByRole('button', { name: /Консоль и логи Spark/i });
    await fireEvent.click(logsTabBtn);

    expect(handleTabChange).toHaveBeenCalledWith('logs');
  });

  it('renders error banner when execution fails', () => {
    render(ResultsView, {
      props: {
        columns: [],
        rows: [],
        totalRows: 0,
        logs: '',
        errorMessage: 'org.apache.spark.SparkException: Job aborted due to stage failure: Task failed',
        executionTimeMs: 400,
        activeTab: 'table'
      }
    });

    expect(screen.getByText('Ошибка исполнения задачи Spark')).toBeInTheDocument();
    expect(screen.getByText(/Job aborted due to stage failure/)).toBeInTheDocument();
  });

  it('displays empty data state when rows are empty and no error', () => {
    render(ResultsView, {
      props: {
        columns: [],
        rows: [],
        totalRows: 0,
        logs: '',
        errorMessage: null,
        executionTimeMs: 0,
        activeTab: 'table'
      }
    });

    expect(screen.getByText('Нет данных для отображения')).toBeInTheDocument();
  });
});
