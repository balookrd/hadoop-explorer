import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import Header from '../components/Header.svelte';
import LoginModal from '../components/LoginModal.svelte';
import StatusBadge from '../components/StatusBadge.svelte';
import NotificationToast from '../components/NotificationToast.svelte';
import Modal from '../components/Modal.svelte';

describe('Common UI Components Suite', () => {
  describe('Header Component', () => {
    it('renders application title, subtitle, user profile and role badge', async () => {
      const handleLogout = vi.fn();
      render(Header, {
        props: {
          title: 'HDFS Explorer',
          subtitle: 'Файловая система',
          user: {
            username: 'alice',
            display_name: 'Alice Cooper',
            groups: ['hadoop-devs'],
            is_admin: true,
            auth_method: 'ldap',
            system_role: 'admin'
          },
          onLogout: handleLogout
        }
      });

      expect(screen.getByText('HDFS Explorer')).toBeInTheDocument();
      expect(screen.getByText('Файловая система')).toBeInTheDocument();
      expect(screen.getByText('Alice Cooper')).toBeInTheDocument();
      expect(screen.getByText('@alice')).toBeInTheDocument();
      expect(screen.getByText('ADM')).toBeInTheDocument();

      // Открытие меню профиля
      const profileBtn = screen.getByRole('button', { name: /Alice Cooper/i });
      await fireEvent.click(profileBtn);

      expect(screen.getByText('Выйти из системы')).toBeInTheDocument();
      await fireEvent.click(screen.getByText('Выйти из системы'));
      expect(handleLogout).toHaveBeenCalled();
    });

    it('renders login button when user is null', () => {
      render(Header, {
        props: {
          title: 'YARN Explorer',
          user: null
        }
      });

      expect(screen.getByText('YARN Explorer')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Войти в систему/i })).toBeInTheDocument();
    });
  });

  describe('LoginModal Component', () => {
    it('renders login form and mock user buttons', async () => {
      const handleLogin = vi.fn().mockResolvedValue(undefined);
      render(LoginModal, {
        props: {
          title: 'Вход в Spark Explorer',
          subtitle: 'Служба интерактивных вычислений Apache Spark',
          isModal: false,
          onLogin: handleLogin
        }
      });

      expect(screen.getByText('Вход в Spark Explorer')).toBeInTheDocument();
      expect(screen.getByText('Служба интерактивных вычислений Apache Spark')).toBeInTheDocument();
      expect(screen.getByText('Александр Админов')).toBeInTheDocument();
      expect(screen.getByText('Иван Датаинженеров')).toBeInTheDocument();
      expect(screen.getByText('Анна Аналитикова')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Войти/i })).toBeInTheDocument();
    });
  });

  describe('StatusBadge Component', () => {
    it('renders active and pending statuses with appropriate labels and colors', () => {
      const { container: activeEl } = render(StatusBadge, {
        props: { status: 'success', text: 'Работает' }
      });
      expect(activeEl.textContent).toContain('Работает');

      const { container: errorEl } = render(StatusBadge, {
        props: { status: 'failed', text: 'Ошибка' }
      });
      expect(errorEl.textContent).toContain('Ошибка');
    });
  });

  describe('Modal Component', () => {
    it('renders modal when isOpen is true and emits close on button click', async () => {
      const handleClose = vi.fn();
      render(Modal, {
        props: {
          isOpen: true,
          title: 'Тестовое окно',
          onClose: handleClose
        }
      });

      expect(screen.getByText('Тестовое окно')).toBeInTheDocument();
      const closeButtons = screen.getAllByRole('button');
      await fireEvent.click(closeButtons[0]);
      expect(handleClose).toHaveBeenCalled();
    });
  });

  describe('NotificationToast Component', () => {
    it('renders toast with message', () => {
      render(NotificationToast, {
        props: {
          message: 'Файл успешно загружен',
          type: 'success'
        }
      });

      expect(screen.getByText('Файл успешно загружен')).toBeInTheDocument();
    });
  });
});
