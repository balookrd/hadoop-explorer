import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import ResultsGrid from '../src/components/ResultsGrid.svelte';
import type { ColumnMeta } from '../src/types';

describe('SQL ResultsGrid Component', () => {
  const mockColumns: ColumnMeta[] = [
    { name: 'user_id', type: 'BIGINT' },
    { name: 'email', type: 'VARCHAR' },
    { name: 'status', type: 'VARCHAR' }
  ];

  const mockRows = [
    [101, 'alice@example.com', 'ACTIVE'],
    [102, 'bob@example.com', 'SUSPENDED'],
    [103, 'charlie@example.com', 'ACTIVE']
  ];

  it('renders columns and row data correctly', () => {
    render(ResultsGrid, {
      props: {
        columns: mockColumns,
        rows: mockRows,
        errorMessage: null,
        totalRows: 3
      }
    });

    expect(screen.getByText('user_id')).toBeInTheDocument();
    expect(screen.getByText('email')).toBeInTheDocument();
    expect(screen.getByText('alice@example.com')).toBeInTheDocument();
    expect(screen.getByText('bob@example.com')).toBeInTheDocument();
    expect(screen.getByText('Показано 3 из 3')).toBeInTheDocument();
  });

  it('filters rows based on filter input', async () => {
    render(ResultsGrid, {
      props: {
        columns: mockColumns,
        rows: mockRows,
        errorMessage: null,
        totalRows: 3
      }
    });

    const filterInput = screen.getByPlaceholderText('Фильтр в результатах...');
    await fireEvent.input(filterInput, { target: { value: 'alice' } });

    expect(screen.getByText('alice@example.com')).toBeInTheDocument();
    expect(screen.queryByText('bob@example.com')).not.toBeInTheDocument();
    expect(screen.getByText('Показано 1 из 3')).toBeInTheDocument();
  });

  it('renders error banner and triggers AI fix button', async () => {
    const handleFix = vi.fn();
    render(ResultsGrid, {
      props: {
        columns: [],
        rows: [],
        errorMessage: 'Table "analytics.events" does not exist in catalog',
        totalRows: 0,
        onFixWithAi: handleFix
      }
    });

    expect(screen.getByText(/Table "analytics\.events" does not exist in catalog/)).toBeInTheDocument();
    const fixBtn = screen.getByRole('button', { name: /Исправить с ИИ|Исправить/i });
    await fireEvent.click(fixBtn);
    expect(handleFix).toHaveBeenCalled();
  });

  it('renders empty data placeholder when rows are empty and no error', () => {
    render(ResultsGrid, {
      props: {
        columns: [],
        rows: [],
        errorMessage: null,
        totalRows: 0
      }
    });

    expect(screen.getByText('Результаты выполнения запроса появятся здесь')).toBeInTheDocument();
  });
});
