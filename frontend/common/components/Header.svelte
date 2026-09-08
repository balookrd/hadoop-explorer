<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { UserSession } from '../types';
  import { Shield, User, LogOut, ChevronDown, Server, Cpu } from 'lucide-svelte';

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
    onLoginClick?: () => void;
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
    onLoginClick,
    extraActions,
  }: Props = $props();

  let showUserMenu = $state(false);
  let menuContainerRef: HTMLDivElement | null = $state(null);

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

<svelte:window
  onclick={(e) => {
    if (showUserMenu && menuContainerRef && !menuContainerRef.contains(e.target as Node)) {
      showUserMenu = false;
    }
  }}
  onkeydown={(e) => {
    if (e.key === 'Escape' && showUserMenu) {
      showUserMenu = false;
    }
  }}
/>

<header
  class="h-16 bg-white border-b border-slate-200 shadow-xs flex items-center justify-between px-4 sm:px-6 select-none shrink-0 sticky top-0 z-50"
  style="position: sticky; top: 0; z-index: 50;"
>
  <!-- Логотип и Бренд -->
  <div class="flex items-center gap-3 shrink-0">
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

  <!-- Селектор кластеров и doAs -->
  {#if clusters && clusters.length > 0}
    <div class="flex items-center gap-2 bg-slate-50 border border-slate-200 rounded-lg px-3 py-1.5 shadow-2xs shrink-0">
      <Server class="w-4 h-4 text-slate-400 shrink-0" />
      <span class="text-xs text-slate-500 font-medium hidden sm:inline">Кластер:</span>
      <select
        value={selectedClusterId || activeCluster?.id}
        onchange={handleClusterChange}
        class="bg-transparent text-xs font-semibold text-slate-800 outline-none cursor-pointer pr-1"
      >
        {#each clusters as cluster}
          <option value={cluster.id} class="bg-white text-slate-900">
            {cluster.name || cluster.id} {cluster.type ? `(${cluster.type.toUpperCase()})` : ''}
          </option>
        {/each}
      </select>

      {#if user && activeCluster}
        <div class="h-3.5 w-px bg-slate-300 mx-1 hidden sm:block"></div>
        <div class="hidden sm:flex items-center gap-1 text-[11px] text-emerald-700 bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded font-medium">
          <Cpu class="w-3 h-3" />
          <span>doAs: <strong class="font-mono">{user.username}</strong></span>
        </div>
      {/if}
    </div>
  {/if}

  <!-- Правая часть: Действия и Профиль пользователя -->
  <div class="flex items-center gap-2 sm:gap-3 shrink-0">
    {#if extraActions}
      {@render extraActions()}
    {/if}

    {#if user}
      <div class="relative shrink-0" bind:this={menuContainerRef}>
        <button
          onclick={() => (showUserMenu = !showUserMenu)}
          aria-expanded={showUserMenu}
          class="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-50 hover:bg-slate-100 border border-slate-200 transition cursor-pointer text-left shadow-2xs shrink-0"
        >
          <div class="w-6 h-6 rounded-full bg-slate-200 flex items-center justify-center text-slate-600 font-semibold text-xs shrink-0">
            <User class="w-3.5 h-3.5" />
          </div>
          <div class="flex flex-col text-left max-w-[140px] sm:max-w-[200px]">
            <span class="text-xs font-semibold text-slate-800 leading-tight truncate">
              {user.display_name || user.username || 'Гость'}
            </span>
            <span class="text-[10px] text-slate-500 leading-tight font-medium font-mono truncate">
              @{user.username}
            </span>
          </div>
          <ChevronDown class="w-3.5 h-3.5 text-slate-400 ml-1 shrink-0 transition-transform duration-200 {showUserMenu ? 'rotate-180' : ''}" />
        </button>

        {#if showUserMenu}
          <!-- Выпадающая карточка профиля -->
          <div
            class="absolute right-0 mt-2 w-64 bg-white border border-slate-200 rounded-xl shadow-xl p-3.5 z-50 animate-in fade-in zoom-in-95 duration-100"
            style="z-index: 1000;"
          >
            <div class="border-b border-slate-100 pb-2.5 mb-2.5">
              <div class="flex items-center justify-between gap-2">
                <div class="text-xs font-bold text-slate-800 truncate">{user.display_name || user.username}</div>
                <span class="text-[9px] px-1.5 py-0.5 rounded font-mono uppercase bg-slate-100 text-slate-600 border border-slate-200 shrink-0">
                  {user.auth_method === 'kerberos' ? 'Kerberos SSO' : user.auth_method === 'mock' ? 'Demo' : 'LDAP'}
                </span>
              </div>
              <div class="text-[11px] text-slate-500 font-mono">@{user.username}</div>
              {#if user.email}
                <div class="text-[11px] text-slate-500 mt-0.5">{user.email}</div>
              {/if}
            </div>

            <!-- Группы LDAP / Роли -->
            <div class="mb-3">
              <div class="text-[11px] font-semibold text-slate-500 mb-1.5 flex items-center justify-between">
                <span>Группы LDAP / Роли:</span>
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
    {:else}
      <button
        type="button"
        onclick={onLoginClick}
        class="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-semibold text-xs transition cursor-pointer shadow-xs shrink-0"
      >
        <User class="w-3.5 h-3.5" />
        <span>Войти в систему</span>
      </button>
    {/if}
  </div>
</header>

