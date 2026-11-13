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
          title: 'Sklad',
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

      expect(screen.getByText('Sklad')).toBeInTheDocument();
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
          title: 'Artel',
          user: null
        }
      });

      expect(screen.getByText('Artel')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Войти в систему/i })).toBeInTheDocument();
    });
  });

  describe('LoginModal Component', () => {
    it('renders login form and mock user buttons', async () => {
      const handleLogin = vi.fn().mockResolvedValue(undefined);
      render(LoginModal, {
        props: {
          title: 'Вход в Iskra',
          app: 'iskra',
          subtitle: 'Служба интерактивных вычислений Apache Spark',
          isModal: false,
          onLogin: handleLogin
        }
      });

      expect(screen.getByText('Вход в Iskra')).toBeInTheDocument();
      expect(screen.getByText('Служба интерактивных вычислений Apache Spark')).toBeInTheDocument();
      expect(screen.getByText('Александр Админов')).toBeInTheDocument();
      expect(screen.getByText('Иван Датаинженеров')).toBeInTheDocument();
      expect(screen.getByText('Анна Аналитикова')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Войти/i })).toBeInTheDocument();
    });

    it('uses the explicit app prop and ignores the title text', () => {
      const { container } = render(LoginModal, {
        props: { title: 'YARN Queue Explorer', app: 'tolmach', isModal: false, onLogin: vi.fn() }
      });
      expect(container.querySelector('[data-testid="login-modal"]')?.getAttribute('data-app')).toBe('tolmach');
      // the list follows the app id, not the YARN word in the title
      expect(screen.getByText('Иван Датаинженеров')).toBeInTheDocument();
      expect(screen.queryByText('Владимир Инженеров')).not.toBeInTheDocument();
    });

    it('accepts every service id', () => {
      for (const app of ['artel', 'sklad', 'tolmach', 'iskra', 'parom'] as const) {
        const { container, unmount } = render(LoginModal, {
          props: { app, isModal: false, onLogin: vi.fn() }
        });
        expect(container.querySelector('[data-testid="login-modal"]')?.getAttribute('data-app')).toBe(app);
        unmount();
      }
    });

    it('switches the demo user set through the tabs without changing the app', async () => {
      const { container } = render(LoginModal, {
        props: { app: 'tolmach', isModal: false, onLogin: vi.fn() }
      });
      // tolmach-only user is visible, universal-only user is not
      expect(screen.getByText('Иван Датаинженеров')).toBeInTheDocument();
      expect(screen.queryByText('Владимир Инженеров')).not.toBeInTheDocument();

      await fireEvent.click(screen.getByRole('button', { name: 'Универсальные' }));

      expect(screen.queryByText('Иван Датаинженеров')).not.toBeInTheDocument();
      expect(screen.getByText('Владимир Инженеров')).toBeInTheDocument();
      expect(container.querySelector('[data-testid="login-modal"]')?.getAttribute('data-app')).toBe('tolmach');
    });

    // Display names are shared between sets, so the lists are told apart by the
    // per-app description, which is unique to each of the six sets.
    const APP_CASES = [
      { app: 'artel', tab: 'YARN', description: 'Администратор YARN, управление очередями и Capacity Scheduler (ADM)' },
      { app: 'sklad', tab: 'HDFS', description: 'Администратор HDFS, полный доступ к неймспейсу (ADM)' },
      { app: 'parom', tab: 'Репликация', description: 'Администратор репликации, полный доступ (ADM)' },
      { app: 'tolmach', tab: 'SQL', description: 'Администратор хранилища данных, управление каталогами (ADM)' },
      { app: 'iskra', tab: 'Spark', description: 'Администратор Spark платформы, управление ресурсами (ADM)' },
    ] as const;

    for (const c of APP_CASES) {
      it(`shows the ${c.app} demo users and marks the ${c.tab} tab active`, () => {
        render(LoginModal, { props: { app: c.app, isModal: false, onLogin: vi.fn() } });
        expect(screen.getByText(c.description)).toBeInTheDocument();
        for (const other of APP_CASES) {
          if (other.app !== c.app) {
            expect(screen.queryByText(other.description)).not.toBeInTheDocument();
          }
        }
        expect(screen.getByRole('button', { name: c.tab }).className).toContain('bg-sky-600');
        expect(screen.getByRole('button', { name: 'Универсальные' }).className).not.toContain('bg-sky-600');
      });
    }

    it('shows data-engineer and analyst users for tolmach and iskra only', () => {
      for (const app of ['tolmach', 'iskra'] as const) {
        const { unmount } = render(LoginModal, { props: { app, isModal: false, onLogin: vi.fn() } });
        expect(screen.getByText('Иван Датаинженеров')).toBeInTheDocument();
        expect(screen.getByText('Анна Аналитикова')).toBeInTheDocument();
        unmount();
      }
      for (const app of ['artel', 'sklad', 'parom'] as const) {
        const { unmount } = render(LoginModal, { props: { app, isModal: false, onLogin: vi.fn() } });
        expect(screen.getByText('Владимир Инженеров')).toBeInTheDocument();
        expect(screen.getByText('Екатерина Наблюдателева')).toBeInTheDocument();
        expect(screen.queryByText('Иван Датаинженеров')).not.toBeInTheDocument();
        expect(screen.queryByText('Анна Аналитикова')).not.toBeInTheDocument();
        unmount();
      }
    });

    it('falls back to the universal users for an unknown app id', () => {
      expect(() =>
        render(LoginModal, { props: { app: 'bogus' as never, isModal: false, onLogin: vi.fn() } })
      ).not.toThrow();
      expect(screen.getByText('Администратор платформы, полный доступ (ADM)')).toBeInTheDocument();
      expect(screen.getByText('Владимир Инженеров')).toBeInTheDocument();
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

    it('renders running status with animated pulse style', () => {
      const { container } = render(StatusBadge, {
        props: { status: 'running', text: 'Выполняется' }
      });
      const badge = container.querySelector('span');
      expect(badge?.className).toContain('animate-pulse');
      expect(container.textContent).toContain('Выполняется');
    });

    it('renders queued, warning, and custom statuses gracefully', () => {
      const { container: queuedEl } = render(StatusBadge, {
        props: { status: 'queued' }
      });
      expect(queuedEl.textContent).toContain('queued');

      const { container: warningEl } = render(StatusBadge, {
        props: { status: 'warning', text: 'Внимание' }
      });
      expect(warningEl.textContent).toContain('Внимание');

      const { container: unknownEl } = render(StatusBadge, {
        props: { status: 'UNKNOWN_STATUS' }
      });
      expect(unknownEl.textContent).toContain('UNKNOWN_STATUS');
    });
  });

  describe('Modal Component', () => {
    it('renders modal when isOpen is true and emits close on button click', async () => {
      const handleClose = vi.fn();
      render(Modal, {
        props: {
          isOpen: true,
          title: 'Тестовое окно',
          subtitle: 'Подзаголовок модального окна',
          onClose: handleClose
        }
      });

      expect(screen.getByText('Тестовое окно')).toBeInTheDocument();
      expect(screen.getByText('Подзаголовок модального окна')).toBeInTheDocument();
      const closeButtons = screen.getAllByRole('button');
      await fireEvent.click(closeButtons[0]);
      expect(handleClose).toHaveBeenCalled();
    });

    it('does not render when isOpen is false', () => {
      render(Modal, {
        props: {
          isOpen: false,
          title: 'Скрытое окно',
          onClose: vi.fn()
        }
      });

      expect(screen.queryByText('Скрытое окно')).not.toBeInTheDocument();
    });

    it('closes on Escape key press', async () => {
      const handleClose = vi.fn();
      render(Modal, {
        props: {
          isOpen: true,
          title: 'Окно с Escape',
          onClose: handleClose
        }
      });

      await fireEvent.keyDown(window, { key: 'Escape' });
      expect(handleClose).toHaveBeenCalled();
    });
  });

  describe('NotificationToast Component', () => {
    it('renders toast with message and triggers onClose on dismiss click', async () => {
      const handleClose = vi.fn();
      render(NotificationToast, {
        props: {
          message: 'Файл успешно загружен',
          type: 'success',
          onClose: handleClose
        }
      });

      expect(screen.getByText('Файл успешно загружен')).toBeInTheDocument();
      const closeBtn = screen.getByRole('button');
      await fireEvent.click(closeBtn);
      expect(handleClose).toHaveBeenCalled();
    });

    it('renders error notification with error-specific classes', () => {
      const { container } = render(NotificationToast, {
        props: {
          message: 'Ошибка при выполнении операции',
          type: 'error'
        }
      });

      expect(container.textContent).toContain('Ошибка при выполнении операции');
      const toast = container.firstElementChild;
      expect(toast?.className).toContain('text-rose-200');
    });
  });
});
