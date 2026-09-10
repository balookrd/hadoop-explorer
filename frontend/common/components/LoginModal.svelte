<script lang="ts">
  import type { UserSession } from '../types';
  import { Lock, User, KeyRound, ShieldAlert, CheckCircle2, ArrowRight, Server } from 'lucide-svelte';

  export interface MockUserOption {
    username: string;
    password?: string;
    displayName: string;
    description: string;
    badgeColor?: string;
  }

  export const DEFAULT_MOCK_USERS: MockUserOption[] = [
    {
      username: 'admin_user',
      password: 'password123',
      displayName: 'Александр Админов',
      description: 'Администратор платформы, полный доступ',
      badgeColor: 'text-purple-600',
    },
    {
      username: 'de_user',
      password: 'password123',
      displayName: 'Иван Датаинженеров',
      description: 'Data Engineer, запуск вычислений / R/W',
      badgeColor: 'text-sky-600',
    },
    {
      username: 'analyst_user',
      password: 'password123',
      displayName: 'Анна Аналитикова',
      description: 'Data Analyst, интерактивные запросы (Read-Only)',
      badgeColor: 'text-emerald-600',
    },
  ];

  interface Props {
    title?: string;
    subtitle?: string;
    icon?: any;
    isModal?: boolean;
    mockUsers?: MockUserOption[];
    initialError?: string | null;
    onClose?: () => void;
    onLogin: (username: string, password: string) => Promise<boolean | UserSession | void>;
    onKerberosSso?: () => Promise<boolean | UserSession | void>;
  }

  let {
    title = 'Hadoop Explorer',
    subtitle = 'Аутентификация LDAP & Kerberos SSO',
    icon: IconComponent = Server,
    isModal = true,
    mockUsers = DEFAULT_MOCK_USERS,
    initialError = null,
    onClose,
    onLogin,
    onKerberosSso,
  }: Props = $props();

  let username = $state('');
  let password = $state('password123');
  let isLoading = $state(false);
  let ssoLoading = $state(false);
  let errorMessage = $state<string | null>(null);

  $effect(() => {
    errorMessage = initialError ?? null;
  });

  // Инициализируем первым тестовым пользователем, если список не пуст
  $effect(() => {
    if (mockUsers && mockUsers.length > 0 && !username) {
      username = mockUsers[0].username;
      if (mockUsers[0].password) {
        password = mockUsers[0].password;
      }
    }
  });

  async function handlePasswordLogin(e?: Event) {
    if (e) e.preventDefault();
    if (!username.trim() || !password.trim()) return;

    isLoading = true;
    errorMessage = null;
    try {
      await onLogin(username, password);
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка аутентификации. Проверьте логин и пароль.';
    } finally {
      isLoading = false;
    }
  }

  async function handleSsoLogin() {
    if (!onKerberosSso) return;
    ssoLoading = true;
    errorMessage = null;
    try {
      await onKerberosSso();
    } catch (err: any) {
      errorMessage = err.message || 'Kerberos SPNEGO SSO билет не предоставлен браузером.';
    } finally {
      ssoLoading = false;
    }
  }

  function pickUser(u: MockUserOption) {
    username = u.username;
    if (u.password) {
      password = u.password;
    }
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isModal && onClose) onClose(); }} />

<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
<div
  class={isModal ? "fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none" : "flex items-center justify-center min-h-[calc(100vh-4rem)] p-4 select-none"}
  onclick={(e) => { if (isModal && onClose && e.target === e.currentTarget) onClose(); }}
>
  <div
    class="w-full max-w-md bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-800 dark:text-slate-100"
    onclick={(e) => e.stopPropagation()}
  >
    <!-- Header -->
    <div class="flex items-center gap-3.5 border-b border-slate-100 dark:border-slate-800 pb-4">
      <div class="w-11 h-11 rounded-2xl bg-gradient-to-tr from-sky-600 to-indigo-600 flex items-center justify-center text-white shadow-md shadow-sky-500/25 shrink-0">
        <IconComponent class="w-5 h-5" />
      </div>
      <div>
        <h2 class="text-lg font-bold tracking-tight text-slate-900 dark:text-slate-100">{title}</h2>
        <p class="text-xs text-slate-500 dark:text-slate-400">{subtitle}</p>
      </div>
    </div>

    <!-- Error Banner -->
    {#if errorMessage}
      <div class="p-3 bg-rose-50 dark:bg-rose-950/50 border border-rose-200 dark:border-rose-900 rounded-xl flex items-center gap-2.5 text-xs text-rose-700 dark:text-rose-300 font-medium">
        <ShieldAlert class="w-4 h-4 shrink-0 text-rose-600 dark:text-rose-400" />
        <span>{errorMessage}</span>
      </div>
    {/if}

    <!-- SSO Button -->
    {#if onKerberosSso}
      <button
        type="button"
        onclick={handleSsoLogin}
        disabled={ssoLoading || isLoading}
        class="w-full flex items-center justify-center gap-2 py-2.5 px-4 rounded-xl bg-slate-900 dark:bg-slate-800 hover:bg-slate-800 dark:hover:bg-slate-700 active:bg-slate-950 text-white text-xs font-semibold shadow-md shadow-slate-900/20 transition cursor-pointer disabled:opacity-50"
      >
        <KeyRound class="w-4 h-4" />
        <span>{ssoLoading ? 'Проверка SPNEGO билета...' : 'Войти через Kerberos SSO (SPNEGO)'}</span>
      </button>

      <div class="relative flex py-0.5 items-center">
        <div class="grow border-t border-slate-200 dark:border-slate-800"></div>
        <span class="shrink mx-3 text-[10px] text-slate-400 dark:text-slate-500 font-semibold uppercase tracking-wider">или по паролю</span>
        <div class="grow border-t border-slate-200 dark:border-slate-800"></div>
      </div>
    {/if}

    <!-- Standard Login Form -->
    <form onsubmit={handlePasswordLogin} class="flex flex-col gap-3.5">
      <div>
        <label for="username" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Учетная запись (LDAP UID / sAMAccountName)</label>
        <div class="relative">
          <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-400 dark:text-slate-500">
            <User class="w-4 h-4" />
          </div>
          <input
            id="username"
            type="text"
            bind:value={username}
            placeholder="например, admin_user"
            required
            class="w-full pl-9 pr-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition"
          />
        </div>
      </div>

      <div>
        <label for="password" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Пароль</label>
        <div class="relative">
          <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-400 dark:text-slate-500">
            <Lock class="w-4 h-4" />
          </div>
          <input
            id="password"
            type="password"
            bind:value={password}
            placeholder="••••••••"
            required
            class="w-full pl-9 pr-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition"
          />
        </div>
      </div>

      <button
        type="submit"
        disabled={isLoading || ssoLoading}
        class="w-full mt-1 flex items-center justify-center gap-2 py-2.5 px-4 rounded-xl bg-sky-600 hover:bg-sky-500 active:bg-sky-700 text-white text-xs font-semibold shadow-md shadow-sky-600/25 transition cursor-pointer disabled:opacity-50"
      >
        <span>{isLoading ? 'Аутентификация...' : 'Войти в систему'}</span>
        <ArrowRight class="w-4 h-4" />
      </button>
    </form>

    <!-- Mock Quick-Select Accounts -->
    {#if mockUsers && mockUsers.length > 0}
      <div class="pt-3.5 border-t border-slate-100 dark:border-slate-800 flex flex-col gap-2">
        <div class="text-[11px] font-semibold text-slate-500 dark:text-slate-400 mb-0.5">Быстрый вход для демо/тестирования:</div>
        <div class="flex flex-col gap-1.5">
          {#each mockUsers as mockUser}
            <button
              type="button"
              onclick={() => pickUser(mockUser)}
              class="flex items-center justify-between p-2.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 hover:bg-sky-50/70 dark:hover:bg-sky-950/40 border border-slate-200/80 dark:border-slate-800 text-left transition cursor-pointer shadow-2xs group"
            >
              <div>
                <div class="text-xs font-bold text-slate-800 dark:text-slate-200 group-hover:text-sky-700 dark:group-hover:text-sky-400 transition-colors">
                  {mockUser.displayName} <span class="font-mono font-normal text-slate-500 dark:text-slate-400 text-[11px]">(@{mockUser.username})</span>
                </div>
                <div class="text-[10px] text-slate-500 dark:text-slate-400 mt-0.5">{mockUser.description}</div>
              </div>
              <CheckCircle2 class="w-4 h-4 {mockUser.badgeColor || 'text-sky-600 dark:text-sky-400'} shrink-0" />
            </button>
          {/each}
        </div>
      </div>
    {/if}
  </div>
</div>
