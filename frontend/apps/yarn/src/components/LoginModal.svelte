<script lang="ts">
  import { LayoutGrid, Lock, User, KeyRound, ShieldAlert, CheckCircle2, ArrowRight } from 'lucide-svelte';

  let {
    onLogin
  }: {
    onLogin: (username: string, password: string) => Promise<void>;
  } = $props();

  let username = $state('admin_user');
  let password = $state('password123');
  let isLoading = $state(false);
  let errorMessage = $state<string | null>(null);
  let ssoLoading = $state(false);

  async function handleSubmit(e?: Event) {
    if (e) e.preventDefault();
    if (!username.trim() || !password.trim()) return;
    isLoading = true;
    errorMessage = null;
    try {
      await onLogin(username, password);
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка авторизации';
    } finally {
      isLoading = false;
    }
  }

  async function handleKerberosLogin() {
    ssoLoading = true;
    errorMessage = null;
    try {
      const resp = await fetch('/api/v1/auth/sso', {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include'
      });
      if (resp.ok) {
        const data = await resp.json();
        if (data.user) {
          // SSO вернул пользователя — передаём через onLogin не получится,
          // перезагружаем страницу для подхвата сессии
          window.location.reload();
          return;
        }
      }
      errorMessage = 'Kerberos SPNEGO SSO билет не предоставлен браузером. Войдите по логину и паролю.';
    } catch {
      errorMessage = 'Сетевая ошибка при проверке Kerberos SSO.';
    } finally {
      ssoLoading = false;
    }
  }

  function pickMockUser(u: string, p: string) {
    username = u;
    password = p;
  }
</script>

<div class="fixed inset-0 bg-slate-900/60 backdrop-blur-md flex items-center justify-center p-4 z-50 select-none">
  <div class="w-full max-w-md bg-white/95 backdrop-blur-md border border-slate-200 rounded-2xl shadow-2xl p-6 sm:p-8 flex flex-col gap-5 text-slate-800">
    <!-- Шапка -->
    <div class="flex items-center gap-3.5">
      <div class="w-11 h-11 rounded-2xl bg-gradient-to-tr from-sky-600 to-indigo-600 flex items-center justify-center text-white shadow-md shadow-sky-500/20">
        <LayoutGrid class="w-5 h-5" />
      </div>
      <div>
        <h2 class="text-lg font-bold tracking-tight text-slate-900">Вход в YARN Queue Explorer</h2>
        <p class="text-xs text-slate-500">Управление очередями YARN Capacity Scheduler</p>
      </div>
    </div>

    {#if errorMessage}
      <div class="p-3 rounded-xl bg-red-50 border border-red-200 text-red-700 text-xs flex items-start gap-2.5">
        <ShieldAlert class="w-4 h-4 text-red-500 shrink-0 mt-0.5" />
        <span>{errorMessage}</span>
      </div>
    {/if}

    <!-- Кнопка Kerberos SSO -->
    <button
      type="button"
      onclick={handleKerberosLogin}
      disabled={ssoLoading || isLoading}
      class="w-full py-2.5 px-3 rounded-xl bg-slate-50 hover:bg-slate-100 border border-slate-200 text-slate-700 text-xs font-semibold flex items-center justify-center gap-2 transition cursor-pointer disabled:opacity-50 shadow-2xs"
    >
      <KeyRound class="w-4 h-4 text-sky-600" />
      <span>{ssoLoading ? 'Проверка билета...' : 'Войти через Kerberos SPNEGO (SSO)'}</span>
    </button>

    <div class="flex items-center gap-2 text-[11px] text-slate-400">
      <div class="h-px bg-slate-200 flex-1"></div>
      <span class="font-medium">или учетная запись LDAPS</span>
      <div class="h-px bg-slate-200 flex-1"></div>
    </div>

    <!-- Форма LDAPS -->
    <form onsubmit={handleSubmit} class="flex flex-col gap-3.5">
      <div>
        <label class="block text-xs font-semibold text-slate-700 mb-1">Имя пользователя (LDAP sAMAccountName)</label>
        <div class="relative">
          <User class="w-4 h-4 text-slate-400 absolute left-3 top-2.5" />
          <input
            type="text"
            bind:value={username}
            placeholder="например, admin_user"
            class="w-full bg-slate-50 border border-slate-300 rounded-lg pl-9 pr-3 py-2 text-xs text-slate-900 placeholder-slate-400 outline-none focus:bg-white focus:border-sky-500 focus:ring-1 focus:ring-sky-500 transition"
            required
          />
        </div>
      </div>

      <div>
        <label class="block text-xs font-semibold text-slate-700 mb-1">Пароль домена</label>
        <div class="relative">
          <Lock class="w-4 h-4 text-slate-400 absolute left-3 top-2.5" />
          <input
            type="password"
            bind:value={password}
            placeholder="••••••••"
            class="w-full bg-slate-50 border border-slate-300 rounded-lg pl-9 pr-3 py-2 text-xs text-slate-900 placeholder-slate-400 outline-none focus:bg-white focus:border-sky-500 focus:ring-1 focus:ring-sky-500 transition"
            required
          />
        </div>
      </div>

      <button
        type="submit"
        disabled={isLoading}
        class="w-full mt-1.5 py-2.5 px-3 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-xs font-bold shadow-md shadow-sky-500/20 flex items-center justify-center gap-1.5 transition cursor-pointer disabled:opacity-50"
      >
        <span>{isLoading ? 'Проверка прав...' : 'Войти в систему'}</span>
        <ArrowRight class="w-3.5 h-3.5" />
      </button>
    </form>

    <!-- Быстрый выбор тестовых пользователей (для демонстрации) -->
    <div class="pt-3 border-t border-slate-200">
      <div class="text-[11px] text-slate-500 mb-2 font-semibold">Быстрый вход для демо/тестирования ACL:</div>
      <div class="flex flex-col gap-1.5 text-xs">
        <button
          type="button"
          onclick={() => pickMockUser('admin_user', 'password123')}
          class="flex items-center justify-between p-2 rounded-lg bg-slate-50 hover:bg-sky-50/70 border border-slate-200 text-slate-700 text-left transition cursor-pointer shadow-2xs"
        >
          <div>
            <div class="font-bold text-slate-800">admin_user (Администратор)</div>
            <div class="text-[10px] text-slate-500">Группы: admins • Полный доступ ко всем кластерам</div>
          </div>
          <CheckCircle2 class="w-4 h-4 text-purple-600" />
        </button>

        <button
          type="button"
          onclick={() => pickMockUser('writer_user', 'password123')}
          class="flex items-center justify-between p-2 rounded-lg bg-slate-50 hover:bg-sky-50/70 border border-slate-200 text-slate-700 text-left transition cursor-pointer shadow-2xs"
        >
          <div>
            <div class="font-bold text-slate-800">writer_user (Инженер)</div>
            <div class="text-[10px] text-slate-500">Группы: engineers • R/W редактирование очередей</div>
          </div>
          <CheckCircle2 class="w-4 h-4 text-sky-600" />
        </button>

        <button
          type="button"
          onclick={() => pickMockUser('reader_user', 'password123')}
          class="flex items-center justify-between p-2 rounded-lg bg-slate-50 hover:bg-sky-50/70 border border-slate-200 text-slate-700 text-left transition cursor-pointer shadow-2xs"
        >
          <div>
            <div class="font-bold text-slate-800">reader_user (Наблюдатель)</div>
            <div class="text-[10px] text-slate-500">Группы: viewers • Read-Only просмотр очередей</div>
          </div>
          <CheckCircle2 class="w-4 h-4 text-emerald-600" />
        </button>
      </div>
    </div>
  </div>
</div>
