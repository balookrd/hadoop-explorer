import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import QueryToolbar from '../src/components/QueryToolbar.svelte';

describe('SQL QueryToolbar Component', () => {
  it('renders Run button when query is not running and triggers onRun', async () => {
    const handleRun = vi.fn();
    render(QueryToolbar, {
      props: {
        isRunning: false,
        statusText: '',
        executionTimeMs: 0,
        rowsCount: 0,
        onRun: handleRun,
        onCancel: vi.fn(),
        onFormat: vi.fn()
      }
    });

    const runBtn = screen.getByRole('button', { name: /Выполнить/i });
    expect(runBtn).toBeInTheDocument();

    await fireEvent.click(runBtn);
    expect(handleRun).toHaveBeenCalled();
  });

  it('renders Cancel button when query is running and displays progress', async () => {
    const handleCancel = vi.fn();
    render(QueryToolbar, {
      props: {
        isRunning: true,
        statusText: 'Выполнение на кластере...',
        executionTimeMs: 1450,
        rowsCount: 0,
        onRun: vi.fn(),
        onCancel: handleCancel
      }
    });

    const cancelBtn = screen.getByRole('button', { name: /Остановить/i });
    expect(cancelBtn).toBeInTheDocument();

    await fireEvent.click(cancelBtn);
    expect(handleCancel).toHaveBeenCalled();

    expect(screen.getByText('Выполнение на кластере...')).toBeInTheDocument();
    expect(screen.getByText('1.45 с')).toBeInTheDocument();
  });

  it('displays formatted rows count and triggers AI assistant tabs', async () => {
    const handleAi = vi.fn();
    render(QueryToolbar, {
      props: {
        isRunning: false,
        statusText: '',
        executionTimeMs: 820,
        rowsCount: 15420,
        onRun: vi.fn(),
        onCancel: vi.fn(),
        onOpenAi: handleAi
      }
    });

    expect(screen.getByText(/15[,\s]420 строк/)).toBeInTheDocument();

    const aiGenBtn = screen.getByRole('button', { name: /ИИ Генератор/i });
    await fireEvent.click(aiGenBtn);
    expect(handleAi).toHaveBeenCalledWith('generate');

    const aiAnalysisBtn = screen.getByRole('button', { name: /Анализ/i });
    await fireEvent.click(aiAnalysisBtn);
    expect(handleAi).toHaveBeenCalledWith('check');
  });
});
