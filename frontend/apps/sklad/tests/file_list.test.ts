import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import FileList from '../src/lib/components/FileList.svelte';
import { explorerStore } from '../src/lib/stores/explorer.svelte';
import type { HdfsFileStatus } from '../src/lib/types';

describe('HDFS FileList Component', () => {
  const mockFiles: HdfsFileStatus[] = [
    {
      pathSuffix: 'data_dir',
      type: 'DIRECTORY',
      length: 0,
      owner: 'hdfs',
      group: 'supergroup',
      permission: '755',
      accessTime: 1700000000000,
      modificationTime: 1700000000000,
      blockSize: 0,
      replication: 0
    },
    {
      pathSuffix: 'events.json',
      type: 'FILE',
      length: 2048,
      owner: 'alice',
      group: 'analytics',
      permission: '644',
      accessTime: 1700001000000,
      modificationTime: 1700001000000,
      blockSize: 134217728,
      replication: 3
    }
  ];

  beforeEach(() => {
    explorerStore.files = [...mockFiles];
    explorerStore.loading = false;
    explorerStore.error = null;
    explorerStore.searchQuery = '';
    explorerStore.selectedFileNames = [];
    explorerStore.currentPath = '/user/analytics';
    explorerStore.parentPath = '/user';
  });

  it('renders table headers and file items correctly', () => {
    render(FileList, {
      props: {
        onPreview: vi.fn(),
        onRename: vi.fn(),
        onDelete: vi.fn(),
        onCopyCrossCluster: vi.fn()
      }
    });

    expect(screen.getByText('Имя')).toBeInTheDocument();
    expect(screen.getByText('Размер')).toBeInTheDocument();
    expect(screen.getByText('Владелец : Группа')).toBeInTheDocument();
    expect(screen.getByText('Права')).toBeInTheDocument();

    expect(screen.getByText('data_dir')).toBeInTheDocument();
    expect(screen.getByText('events.json')).toBeInTheDocument();
    expect(screen.getByText('alice')).toBeInTheDocument();
    expect(screen.getByText('analytics')).toBeInTheDocument();
  });

  it('displays empty message when files list is empty', () => {
    explorerStore.files = [];

    render(FileList, {
      props: {
        onPreview: vi.fn(),
        onRename: vi.fn(),
        onDelete: vi.fn(),
        onCopyCrossCluster: vi.fn()
      }
    });

    expect(screen.getByText('Папка пуста')).toBeInTheDocument();
  });

  it('displays loading state correctly', () => {
    explorerStore.files = [];
    explorerStore.loading = true;

    render(FileList, {
      props: {
        onPreview: vi.fn(),
        onRename: vi.fn(),
        onDelete: vi.fn(),
        onCopyCrossCluster: vi.fn()
      }
    });

    expect(screen.getByText('Загрузка каталога...')).toBeInTheDocument();
  });

  it('displays error message banner when store has an error', () => {
    explorerStore.error = 'Permission denied: user=bob';

    render(FileList, {
      props: {
        onPreview: vi.fn(),
        onRename: vi.fn(),
        onDelete: vi.fn(),
        onCopyCrossCluster: vi.fn()
      }
    });

    expect(screen.getByText(/Permission denied: user=bob/)).toBeInTheDocument();
  });

  it('navigates to directory when clicking directory item', async () => {
    const navSpy = vi.spyOn(explorerStore, 'navigateTo').mockImplementation(() => {});

    render(FileList, {
      props: {
        onPreview: vi.fn(),
        onRename: vi.fn(),
        onDelete: vi.fn(),
        onCopyCrossCluster: vi.fn()
      }
    });

    const dirRow = screen.getByText('data_dir');
    await fireEvent.click(dirRow);

    expect(navSpy).toHaveBeenCalledWith('/user/analytics/data_dir');
    navSpy.mockRestore();
  });

  it('triggers onPreview when clicking a regular file', async () => {
    const handlePreview = vi.fn();

    render(FileList, {
      props: {
        onPreview: handlePreview,
        onRename: vi.fn(),
        onDelete: vi.fn(),
        onCopyCrossCluster: vi.fn()
      }
    });

    const fileRow = screen.getByText('events.json');
    await fireEvent.click(fileRow);

    expect(handlePreview).toHaveBeenCalledWith(mockFiles[1]);
  });

  it('handles sort column clicks', async () => {
    const sortSpy = vi.spyOn(explorerStore, 'toggleSort').mockImplementation(() => {});

    render(FileList, {
      props: {
        onPreview: vi.fn(),
        onRename: vi.fn(),
        onDelete: vi.fn(),
        onCopyCrossCluster: vi.fn()
      }
    });

    const nameCol = screen.getByText('Имя');
    await fireEvent.click(nameCol);
    expect(sortSpy).toHaveBeenCalledWith('name');

    const sizeCol = screen.getByText('Размер');
    await fireEvent.click(sizeCol);
    expect(sortSpy).toHaveBeenCalledWith('size');

    sortSpy.mockRestore();
  });
});
