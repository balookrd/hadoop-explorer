import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import MkdirModal from '../src/lib/components/Modals/MkdirModal.svelte';
import DeleteModal from '../src/lib/components/Modals/DeleteModal.svelte';
import RenameModal from '../src/lib/components/Modals/RenameModal.svelte';
import { explorerStore } from '../src/lib/stores/explorer.svelte';
import { api } from '../src/lib/api/client';
import type { HdfsFileStatus } from '../src/lib/types';

describe('HDFS Modals Suite', () => {
  beforeEach(() => {
    explorerStore.currentCluster = {
      id: 'test-cluster',
      name: 'Test Cluster',
      default_path: '/',
      is_admin: true
    };
    explorerStore.currentPath = '/data';
  });

  describe('MkdirModal Component', () => {
    it('does not render when isOpen is false', () => {
      render(MkdirModal, {
        props: { isOpen: false, onClose: vi.fn() }
      });
      expect(screen.queryByText('Новая директория')).not.toBeInTheDocument();
    });

    it('renders input and submits new directory creation', async () => {
      const handleClose = vi.fn();
      const createApiSpy = vi.spyOn(api, 'createDirectory').mockResolvedValue(true as any);
      const refreshSpy = vi.spyOn(explorerStore, 'refresh').mockResolvedValue();

      render(MkdirModal, {
        props: { isOpen: true, onClose: handleClose }
      });

      expect(screen.getByText('Новая директория')).toBeInTheDocument();
      expect(screen.getByText('/data')).toBeInTheDocument();

      const input = screen.getByPlaceholderText('например, logs_2026');
      const submitBtn = screen.getByRole('button', { name: /Создать/i });
      expect(submitBtn).toBeDisabled();

      await fireEvent.input(input, { target: { value: 'processed' } });
      expect(submitBtn).not.toBeDisabled();

      await fireEvent.click(submitBtn);
      expect(createApiSpy).toHaveBeenCalledWith('test-cluster', '/data/processed');

      createApiSpy.mockRestore();
      refreshSpy.mockRestore();
    });

    it('calls onClose when clicking cancel', async () => {
      const handleClose = vi.fn();
      render(MkdirModal, {
        props: { isOpen: true, onClose: handleClose }
      });

      const cancelBtn = screen.getByRole('button', { name: /Отмена/i });
      await fireEvent.click(cancelBtn);
      expect(handleClose).toHaveBeenCalled();
    });
  });

  describe('DeleteModal Component', () => {
    const mockFile: HdfsFileStatus = {
      pathSuffix: 'old_archive.tar.gz',
      type: 'FILE',
      length: 1024,
      owner: 'hdfs',
      group: 'hdfs',
      permission: '644',
      accessTime: 0,
      modificationTime: 0,
      blockSize: 0,
      replication: 1
    };

    it('renders confirmation text and triggers delete api', async () => {
      const handleClose = vi.fn();
      const deleteApiSpy = vi.spyOn(api, 'deletePath').mockResolvedValue(true as any);
      const refreshSpy = vi.spyOn(explorerStore, 'refresh').mockResolvedValue();

      render(DeleteModal, {
        props: { file: mockFile, onClose: handleClose }
      });

      expect(screen.getByText('Подтверждение удаления')).toBeInTheDocument();
      expect(screen.getByText('old_archive.tar.gz')).toBeInTheDocument();

      const deleteBtn = screen.getByRole('button', { name: /Удалить/i });
      await fireEvent.click(deleteBtn);

      expect(deleteApiSpy).toHaveBeenCalledWith('test-cluster', '/data/old_archive.tar.gz', false);

      deleteApiSpy.mockRestore();
      refreshSpy.mockRestore();
    });
  });

  describe('RenameModal Component', () => {
    const mockFile: HdfsFileStatus = {
      pathSuffix: 'raw_data.csv',
      type: 'FILE',
      length: 500,
      owner: 'hdfs',
      group: 'hdfs',
      permission: '644',
      accessTime: 0,
      modificationTime: 0,
      blockSize: 0,
      replication: 1
    };

    it('pre-populates current filename and triggers rename api', async () => {
      const handleClose = vi.fn();
      const renameApiSpy = vi.spyOn(api, 'renamePath').mockResolvedValue(true as any);
      const refreshSpy = vi.spyOn(explorerStore, 'refresh').mockResolvedValue();

      render(RenameModal, {
        props: { file: mockFile, onClose: handleClose }
      });

      expect(screen.getByText('Переименовать / Переместить')).toBeInTheDocument();
      const input = await screen.findByDisplayValue('raw_data.csv');
      await fireEvent.input(input, { target: { value: 'clean_data.csv' } });

      const saveBtn = screen.getByRole('button', { name: /Сохранить/i });
      await fireEvent.click(saveBtn);

      expect(renameApiSpy).toHaveBeenCalledWith(
        'test-cluster',
        '/data/raw_data.csv',
        '/data/clean_data.csv'
      );

      renameApiSpy.mockRestore();
      refreshSpy.mockRestore();
    });
  });
});
