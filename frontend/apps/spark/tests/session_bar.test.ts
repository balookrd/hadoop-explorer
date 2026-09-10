import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import SessionBar from '../src/components/SessionBar.svelte';

describe('Spark SessionBar Component', () => {
  it('renders Run button and switches languages', async () => {
    const handleRun = vi.fn();
    const handleLangChange = vi.fn();

    render(SessionBar, {
      props: {
        language: 'pyspark',
        isRunning: false,
        onRun: handleRun,
        onLanguageChange: handleLangChange
      }
    });

    const runBtn = screen.getByRole('button', { name: /Выполнить/i });
    expect(runBtn).toBeInTheDocument();
    await fireEvent.click(runBtn);
    expect(handleRun).toHaveBeenCalled();

    const scalaBtn = screen.getByRole('button', { name: /Scala Spark/i });
    await fireEvent.click(scalaBtn);
    expect(handleLangChange).toHaveBeenCalledWith('scalaspark');

    const sqlBtn = screen.getByRole('button', { name: /Spark SQL/i });
    await fireEvent.click(sqlBtn);
    expect(handleLangChange).toHaveBeenCalledWith('sql');
  });

  it('renders Cancel button and status text when running', async () => {
    const handleCancel = vi.fn();

    render(SessionBar, {
      props: {
        language: 'pyspark',
        isRunning: true,
        statusText: 'Вычисление DAG графа...',
        executionTimeMs: 2500,
        rowsCount: 500,
        onRun: vi.fn(),
        onCancel: handleCancel,
        onLanguageChange: vi.fn()
      }
    });

    const cancelBtn = screen.getByRole('button', { name: /Остановить/i });
    expect(cancelBtn).toBeInTheDocument();
    await fireEvent.click(cancelBtn);
    expect(handleCancel).toHaveBeenCalled();

    expect(screen.getByText('Вычисление DAG графа...')).toBeInTheDocument();
    expect(screen.getByText('2.50 с')).toBeInTheDocument();
    expect(screen.getByText(/500 строк/)).toBeInTheDocument();
  });
});
