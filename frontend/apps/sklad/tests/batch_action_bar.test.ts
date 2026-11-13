import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import BatchActionBar from '../src/lib/components/BatchActionBar.svelte';
import { explorerStore } from '../src/lib/stores/explorer.svelte';

describe('HDFS BatchActionBar Component', () => {
  beforeEach(() => {
    explorerStore.canWrite = true;
    explorerStore.selectedFileNames = [];
  });

  it('does not render when no files are selected', () => {
    render(BatchActionBar, {
      props: { onOpenBatchDelete: vi.fn() }
    });

    expect(screen.queryByLabelText('Панель пакетных действий')).not.toBeInTheDocument();
  });

  it('renders correctly when files are selected with item count', () => {
    explorerStore.selectedFileNames = ['file1.csv', 'file2.parquet', 'folderA'];

    render(BatchActionBar, {
      props: { onOpenBatchDelete: vi.fn() }
    });

    expect(screen.getByLabelText('Панель пакетных действий')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText(/выбрано/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Скачать ZIP/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Удалить/i })).toBeInTheDocument();
  });

  it('triggers onOpenBatchDelete callback on delete click', async () => {
    const handleDelete = vi.fn();
    explorerStore.selectedFileNames = ['file1.csv'];

    render(BatchActionBar, {
      props: { onOpenBatchDelete: handleDelete }
    });

    const deleteBtn = screen.getByRole('button', { name: /Удалить/i });
    await fireEvent.click(deleteBtn);
    expect(handleDelete).toHaveBeenCalled();
  });

  it('disables delete button when cluster is read-only', () => {
    explorerStore.canWrite = false;
    explorerStore.selectedFileNames = ['file1.csv'];

    render(BatchActionBar, {
      props: { onOpenBatchDelete: vi.fn() }
    });

    expect(screen.getByRole('button', { name: /Удалить/i })).toBeDisabled();
  });

  it('clears selection on dismiss click', async () => {
    explorerStore.selectedFileNames = ['file1.csv'];
    const clearSpy = vi.spyOn(explorerStore, 'clearSelection').mockImplementation(() => {
      explorerStore.selectedFileNames = [];
    });

    render(BatchActionBar, {
      props: { onOpenBatchDelete: vi.fn() }
    });

    const dismissBtn = screen.getByTitle('Снять выделение (Esc)');
    await fireEvent.click(dismissBtn);
    expect(clearSpy).toHaveBeenCalled();
    clearSpy.mockRestore();
  });
});
