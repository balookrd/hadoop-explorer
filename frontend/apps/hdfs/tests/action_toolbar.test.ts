import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import ActionToolbar from '../src/lib/components/ActionToolbar.svelte';
import { explorerStore } from '../src/lib/stores/explorer.svelte';

describe('HDFS ActionToolbar Component', () => {
  beforeEach(() => {
    explorerStore.canWrite = true;
    explorerStore.currentPath = '/';
    explorerStore.searchQuery = '';
    explorerStore.totalDirs = 5;
    explorerStore.totalFiles = 12;
    explorerStore.totalSize = 1048576; // 1 MB
    explorerStore.currentCluster = {
      id: 'cluster-prod',
      name: 'Production HDFS',
      default_path: '/user',
      is_admin: true
    };
  });

  it('renders toolbar buttons and triggers callbacks', async () => {
    const handleUpload = vi.fn();
    const handleMkdir = vi.fn();

    render(ActionToolbar, {
      props: {
        onOpenUpload: handleUpload,
        onOpenMkdir: handleMkdir
      }
    });

    const uploadBtn = screen.getByRole('button', { name: /Загрузить/i });
    const mkdirBtn = screen.getByRole('button', { name: /Новая папка/i });
    const refreshBtn = screen.getByTitle('Обновить список');

    expect(uploadBtn).not.toBeDisabled();
    expect(mkdirBtn).not.toBeDisabled();

    await fireEvent.click(uploadBtn);
    expect(handleUpload).toHaveBeenCalled();

    await fireEvent.click(mkdirBtn);
    expect(handleMkdir).toHaveBeenCalled();

    const refreshSpy = vi.spyOn(explorerStore, 'refresh').mockImplementation(async () => {});
    await fireEvent.click(refreshBtn);
    expect(refreshSpy).toHaveBeenCalled();
    refreshSpy.mockRestore();
  });

  it('disables action buttons when cluster is read-only', () => {
    explorerStore.canWrite = false;

    render(ActionToolbar, {
      props: {
        onOpenUpload: vi.fn(),
        onOpenMkdir: vi.fn()
      }
    });

    expect(screen.getByRole('button', { name: /Загрузить/i })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Новая папка/i })).toBeDisabled();
  });

  it('updates search query in store when typing in filter input', async () => {
    render(ActionToolbar, {
      props: {
        onOpenUpload: vi.fn(),
        onOpenMkdir: vi.fn()
      }
    });

    const filterInput = screen.getByPlaceholderText('Фильтр файлов...');
    await fireEvent.input(filterInput, { target: { value: 'logs' } });
    expect(explorerStore.searchQuery).toBe('logs');
  });

  it('displays directory, file count, and formatted byte size', () => {
    render(ActionToolbar, {
      props: {
        onOpenUpload: vi.fn(),
        onOpenMkdir: vi.fn()
      }
    });

    expect(screen.getByText('Папок:')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();
    expect(screen.getByText('Файлов:')).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument();
    expect(screen.getByText(/1 MB/)).toBeInTheDocument();
  });

  it('shows folder download button when not in root directory', () => {
    explorerStore.currentPath = '/user/analytics';

    render(ActionToolbar, {
      props: {
        onOpenUpload: vi.fn(),
        onOpenMkdir: vi.fn()
      }
    });

    expect(screen.getByTitle('Скачать текущую папку (ZIP-архив)')).toBeInTheDocument();
  });
});
