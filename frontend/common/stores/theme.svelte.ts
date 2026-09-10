/**
 * Глобальный реактивный стор управления светлой/тёмной темой (Svelte 5 Runes).
 * Синхронизирует класс `dark` на document.documentElement и сохраняет выбор в localStorage.
 */

export type ThemeMode = 'light' | 'dark' | 'system';

class ThemeStore {
  current = $state<ThemeMode>('light');
  isDark = $state<boolean>(false);

  constructor() {
    if (typeof window !== 'undefined') {
      const saved = (localStorage.getItem('hadoop_theme') as ThemeMode) || 'light';
      this.setTheme(saved);

      // Отслеживание системных настроек
      window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
        if (this.current === 'system') {
          this.applyTheme();
        }
      });
    }
  }

  setTheme(mode: ThemeMode) {
    this.current = mode;
    if (typeof window !== 'undefined') {
      localStorage.setItem('hadoop_theme', mode);
      this.applyTheme();
    }
  }

  toggleTheme() {
    const next = this.isDark ? 'light' : 'dark';
    this.setTheme(next);
  }

  private applyTheme() {
    if (typeof document === 'undefined') return;

    let dark = false;
    if (this.current === 'dark') {
      dark = true;
    } else if (this.current === 'system') {
      dark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    }

    this.isDark = dark;
    if (dark) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }
}

export const themeStore = new ThemeStore();
