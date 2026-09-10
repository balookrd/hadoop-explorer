import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import AddQueueModal from '../src/components/AddQueueModal.svelte';
import XmlExportModal from '../src/components/XmlExportModal.svelte';

describe('YARN Modals Suite', () => {
  describe('AddQueueModal Component', () => {
    it('does not render when isOpen is false', () => {
      render(AddQueueModal, {
        props: {
          parentPath: 'root.users',
          isOpen: false,
          resourceMode: 'percentage',
          selectedPartition: '[default]',
          onConfirm: vi.fn()
        }
      });

      expect(screen.queryByText(/Создать очередь|Add Queue/i)).not.toBeInTheDocument();
    });

    it('renders input fields and confirms queue creation', async () => {
      const handleConfirm = vi.fn();
      render(AddQueueModal, {
        props: {
          parentPath: 'root.users',
          isOpen: true,
          resourceMode: 'percentage',
          selectedPartition: '[default]',
          onConfirm: handleConfirm
        }
      });

      expect(screen.getByText(/root\.users/)).toBeInTheDocument();
      const nameInput = screen.getByPlaceholderText('analytics_batch');
      await fireEvent.input(nameInput, { target: { value: 'marketing' } });

      const confirmBtn = screen.getByRole('button', { name: /Создать очередь/i });
      await fireEvent.click(confirmBtn);

      expect(handleConfirm).toHaveBeenCalled();
      const calledDraft = handleConfirm.mock.calls[0][0];
      expect(calledDraft.name).toBe('marketing');
      expect(calledDraft.parent_path).toBe('root.users');
    });
  });

  describe('XmlExportModal Component', () => {
    it('renders xml content and triggers clipboard copy', async () => {
      const mockXml = '<configuration><property><name>yarn.scheduler.capacity.root.queues</name><value>default</value></property></configuration>';
      const clipboardSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue();

      render(XmlExportModal, {
        props: {
          isOpen: true,
          xmlContent: mockXml,
          filename: 'capacity-scheduler.xml',
          instructions: 'Save to /etc/hadoop/conf',
          canAdmin: true
        }
      });

      expect(screen.getByText('capacity-scheduler.xml')).toBeInTheDocument();
      expect(screen.getByText(/Save to \/etc\/hadoop\/conf/)).toBeInTheDocument();

      const copyBtn = screen.getByRole('button', { name: /Копировать|Copy/i });
      await fireEvent.click(copyBtn);

      expect(clipboardSpy).toHaveBeenCalledWith(mockXml);
      clipboardSpy.mockRestore();
    });
  });
});
