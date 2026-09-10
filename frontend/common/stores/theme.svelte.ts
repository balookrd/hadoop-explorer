/**
 * Глобальный реактивный стор управления светлой/тёмной темой (Svelte 5 Runes).
 * Синхронизирует класс `dark` на document.documentElement и document.body,
 * сохраняя выбор пользователя в localStorage.
 */

export type ThemeMode = 'light' | 'dark' | 'system';

function getInitialTheme(): ThemeMode {
  if (typeof window !== 'undefined') {
    const saved = localStorage.getItem('hadoop_theme') as ThemeMode;
    if (saved === 'dark' || saved === 'light' || saved === 'system') {
      return saved;
    }
  }
  return 'light';
}

class ThemeStore {
  current = $state<ThemeMode>('light');
  isDark = $state<boolean>(false);

  constructor() {
    if (typeof window !== 'undefined') {
      const mode = getInitialTheme();
      this.current = mode;
      this.applyTheme(mode);

      // Отслеживание системных настроек ОС
      try {
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
          if (this.current === 'system') {
            this.applyTheme('system');
          }
        });
      } catch (e) {
        // Fallback для старых окружений
      }
    }
  }

  setTheme(mode: ThemeMode) {
    this.current = mode;
    if (typeof window !== 'undefined') {
      localStorage.setItem('hadoop_theme', mode);
      this.applyTheme(mode);
    }
  }

  toggleTheme() {
    const next = this.isDark ? 'light' : 'dark';
    this.setTheme(next);
  }

  private applyTheme(mode: ThemeMode) {
    if (typeof document === 'undefined') return;

    let dark = false;
    if (mode === 'dark') {
      dark = true;
    } else if (mode === 'system') {
      dark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    }

    this.isDark = dark;
    const root = document.documentElement;
    const body = document.body;

    if (dark) {
      root.classList.add('dark');
      if (body) body.classList.add('dark');
      root.style.colorScheme = 'dark';
    } else {
      root.classList.remove('dark');
      if (body) body.classList.remove('dark');
      root.style.colorScheme = 'light';
    }
  }
}

export const themeStore = new ThemeStore();
