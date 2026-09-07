<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { UserSession } from '../types';
  import { Shield, User, LogOut, ChevronDown, Server } from 'lucide-svelte';

  interface ClusterOption {
    id: string;
    name: string;
    type?: string;
    host?: string;
    status?: string;
    [key: string]: any;
  }

  interface Props {
    title: string;
    subtitle?: string;
    icon?: any;
    user?: UserSession | null;
    clusters?: ClusterOption[];
    selectedClusterId?: string;
    onClusterSelect?: (clusterId: string) => void;
    onLogout?: () => void;
    extraActions?: Snippet;
  }

  let {
    title,
    subtitle,
    icon: IconComponent = Server,
    user = null,
    clusters = [],
    selectedClusterId = $bindable(''),
    onClusterSelect,
    onLogout,
    extraActions,
  }: Props = $props();

  let showUserMenu = $state(false);

  const activeCluster = $derived(
    clusters.find((c) => c.id === selectedClusterId) || clusters[0]
  );

  function handleClusterChange(e: Event) {
    const target = e.target as HTMLSelectElement;
    selectedClusterId = target.value;
    if (onClusterSelect) {
      onClusterSelect(target.value);
    }
  }

  function getRoleBadgeClass(role?: string, isAdmin?: boolean): string {
    if (isAdmin || role === 'admin') return 'text-purple-700 bg-purple-50 border-purple-200';
    if (role === 'writer') return 'text-amber-700 bg-amber-50 border-amber-200';
    return 'text-slate-600 bg-slate-50 border-slate-200';
  }
</script>

<header class="h-16 bg-white border-b border-slate-200 shadow-xs flex items-center justify-between px-4 sm:px-6 select-none shrink-0 sticky top-0 z-30">
  <!-- Логотип и Бренд -->
  <div class="flex items-center gap-3">
    <div class="w-9 h-9 rounded-xl bg-gradient-to-tr from-sky-600 to-indigo-600 flex items-center justify-center text-white shadow-md shadow-sky-500/20 shrink-0">
      <IconComponent class="w-4 h-4" />
    </div>
    <div class="flex flex-col">
      <span class="text-base font-bold tracking-tight bg-gradient-to-r from-slate-900 to-slate-700 bg-clip-text text-transparent flex items-center gap-1.5">
        {title}
        {#if subtitle}
          <span class="text-[10px] px-2 py-0.5 rounded-full font-mono bg-sky-50 text-sky-700 border border-sky-200">
            {subtitle}
          </span>
        {/if}
      </span>
    </div>
  </div>

  <!-- Селектор кластеров -->
  {#if clusters && clusters.length > 0}
    <div class="flex items-center gap-2 bg-slate-50 border border-slate-200 rounded-xl px-3 py-1.5 shadow-2xs">
      <div class="flex items-center gap-1.5 text-xs text-slate-500">
        <Server class="w-3.5 h-3.5 text-sky-600" />
        <span class="font-medium hidden sm:inline">Кластер:</span>
      </div>
      <select
        value={selectedClusterId || activeCluster?.id}
        onchange={handleClusterChange}
        class="bg-transparent text-xs font-bold text-slate-800 focus:outline-none cursor-pointer pr-1"
      >
        {#each clusters as cluster}
          <option value={cluster.id}>
            {cluster.name || cluster.id} {cluster.type ? `(${cluster.type.toUpperCase()})` : ''}
          </option>
        {/each}
      </select>
    </div>
  {/if}

  <!-- Правая часть: Действия и Профиль пользователя -->
  <div class="flex items-center gap-2 sm:gap-3">
    {#if extraActions}
      {@render extraActions()}
    {/if}

    {#if user}
      <div class="relative">
        <button
          onclick={() => (showUserMenu = !showUserMenu)}
          class="flex items-center gap-2 p-1 sm:px-2.5 sm:py-1.5 rounded-xl border border-slate-200 hover:bg-slate-50 transition cursor-pointer shadow-2xs"
        >
          <div class="w-7 h-7 rounded-lg bg-sky-100 text-sky-700 font-bold flex items-center justify-center text-xs">
            {user.username.charAt(0).toUpperCase()}
          </div>
          <div class="hidden sm:flex flex-col text-left">
            <span class="text-xs font-semibold text-slate-800 leading-tight">
              {user.display_name || user.username}
            </span>
            <span class="text-[10px] text-slate-500 font-mono leading-tight">
              @{user.username}
            </span>
          </div>
          <ChevronDown class="w-3.5 h-3.5 text-slate-400" />
        </button>

        {#if showUserMenu}
          <!-- Backdrop для закрытия по клику вне -->
          <button
            class="fixed inset-0 z-40 bg-transparent cursor-default border-none"
            onclick={() => (showUserMenu = false)}
            aria-label="Закрыть меню"
          ></button>

          <!-- Выпадающая карточка профиля -->
          <div class="absolute right-0 mt-2 w-64 bg-white border border-slate-200 rounded-xl shadow-xl p-3.5 z-50">
            <div class="border-b border-slate-100 pb-2.5 mb-2.5">
              <div class="text-xs font-bold text-slate-800">{user.display_name || user.username}</div>
              <div class="text-[11px] text-slate-500 font-mono">@{user.username}</div>
              {#if user.email}
                <div class="text-[11px] text-slate-500 mt-0.5">{user.email}</div>
              {/if}
            </div>

            <!-- Группы LDAP / Роли -->
            <div class="mb-3">
              <div class="text-[11px] font-semibold text-slate-500 mb-1.5 flex items-center justify-between">
                <span>Группы / Роли:</span>
                <span class="flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] font-bold border {getRoleBadgeClass(user.system_role, user.is_admin)}">
                  {#if user.is_admin}
                    <Shield class="w-3 h-3" />
                  {/if}
                  {(user.system_role || (user.is_admin ? 'ADMIN' : 'USER')).toUpperCase()}
                </span>
              </div>
              <div class="flex flex-wrap gap-1 max-h-24 overflow-y-auto">
                {#if user.groups && user.groups.length > 0}
                  {#each user.groups as group}
                    <span class="text-[10px] px-1.5 py-0.5 rounded bg-slate-100 text-slate-700 border border-slate-200 font-medium">
                      {group}
                    </span>
                  {/each}
                {:else}
                  <span class="text-[10px] text-slate-400 italic">Нет назначенных групп</span>
                {/if}
              </div>
            </div>

            {#if onLogout}
              <button
                onclick={() => {
                  showUserMenu = false;
                  onLogout();
                }}
                class="w-full flex items-center justify-center gap-2 py-1.5 px-3 rounded-lg bg-red-50 hover:bg-red-100 border border-red-200 text-red-700 text-xs font-medium transition cursor-pointer"
              >
                <LogOut class="w-3.5 h-3.5" />
                Выйти из системы
              </button>
            {/if}
          </div>
        {/if}
      </div>
    {/if}
  </div>
</header>
