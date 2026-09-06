<script lang="ts">
  import type { UserSession, ClusterSummary } from '../types';
  import { Database, Shield, User, LogOut, ChevronDown, Cpu, Server } from 'lucide-svelte';

  let {
    user,
    clusters,
    selectedClusterId = $bindable(),
    onLogout
  }: {
    user: UserSession | null;
    clusters: ClusterSummary[];
    selectedClusterId: string;
    onLogout: () => void;
  } = $props();

  let showUserMenu = $state(false);

  const activeCluster = $derived(
    clusters.find((c) => c.id === selectedClusterId) || clusters[0]
  );
</script>

<header class="h-16 bg-white border-b border-slate-200 shadow-xs flex items-center justify-between px-4 sm:px-6 select-none shrink-0 z-20">
  <!-- Логотип и Бренд -->
  <div class="flex items-center gap-3">
    <div class="w-9 h-9 rounded-xl bg-gradient-to-tr from-sky-600 to-indigo-600 flex items-center justify-center text-white shadow-md shadow-sky-500/20">
      <Database class="w-4 h-4" />
    </div>
    <div class="flex flex-col">
      <span class="text-base font-bold tracking-tight bg-gradient-to-r from-slate-900 to-slate-700 bg-clip-text text-transparent flex items-center gap-1.5">
        SQL Web Explorer
        <span class="text-[10px] px-2 py-0.5 rounded-full font-mono bg-sky-50 text-sky-700 border border-sky-200">
          Trino & Hive
        </span>
      </span>
    </div>
  </div>

  <!-- Селектор кластеров и имперсонация -->
  <div class="flex items-center gap-3">
    <div class="flex items-center gap-2 bg-slate-50 border border-slate-200 rounded-lg px-3 py-1.5 shadow-2xs">
      <Server class="w-4 h-4 text-slate-400" />
      <span class="text-xs text-slate-500 font-medium">Кластер:</span>
      <select
        bind:value={selectedClusterId}
        class="bg-transparent text-xs font-semibold text-slate-800 outline-none cursor-pointer pr-2"
      >
        {#each clusters as c}
          <option value={c.id} class="bg-white text-slate-900">
            {c.name} ({c.type.toUpperCase()})
          </option>
        {/each}
      </select>

      {#if activeCluster}
        <div class="h-3.5 w-px bg-slate-300 mx-1"></div>
        <div class="flex items-center gap-1 text-[11px] text-emerald-700 bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded font-medium">
          <Cpu class="w-3 h-3" />
          <span>doAs: <strong class="font-mono">{user?.username}</strong></span>
        </div>
      {/if}
    </div>
  </div>

  <!-- Профиль пользователя и LDAP / Kerberos инфо -->
  <div class="relative">
    <button
      onclick={() => (showUserMenu = !showUserMenu)}
      class="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-50 hover:bg-slate-100 border border-slate-200 transition cursor-pointer text-left shadow-2xs"
    >
      <div class="w-6 h-6 rounded-full bg-slate-200 flex items-center justify-center text-slate-600 font-semibold text-xs">
        <User class="w-3.5 h-3.5" />
      </div>
      <div class="flex flex-col">
        <span class="text-xs font-semibold text-slate-800 leading-tight">
          {user?.display_name || user?.username || 'Гость'}
        </span>
        <span class="text-[10px] text-slate-500 leading-tight font-medium">
          {user?.auth_method?.toUpperCase()} SSO
        </span>
      </div>
      <ChevronDown class="w-3.5 h-3.5 text-slate-400 ml-1" />
    </button>

    {#if showUserMenu && user}
      <div class="absolute right-0 mt-2 w-64 bg-white border border-slate-200 rounded-xl shadow-xl p-3.5 z-50">
        <div class="border-b border-slate-100 pb-2.5 mb-2.5">
          <div class="text-xs font-bold text-slate-800">{user.display_name}</div>
          <div class="text-[11px] text-slate-500 font-mono">@{user.username}</div>
          {#if user.email}
            <div class="text-[11px] text-slate-500 mt-0.5">{user.email}</div>
          {/if}
        </div>

        <!-- Группы LDAP / ACL -->
        <div class="mb-3">
          <div class="text-[11px] font-semibold text-slate-500 mb-1.5 flex items-center justify-between">
            <span>Группы LDAP / Роли:</span>
            {#if user.is_admin}
              <span class="flex items-center gap-0.5 text-purple-700 bg-purple-50 border border-purple-200 px-1.5 py-0.5 rounded text-[10px] font-bold">
                <Shield class="w-3 h-3" /> ADMIN
              </span>
            {/if}
          </div>
          <div class="flex flex-wrap gap-1 max-h-24 overflow-y-auto">
            {#each user.groups as group}
              <span class="text-[10px] px-1.5 py-0.5 rounded bg-slate-100 text-slate-700 border border-slate-200 font-medium">
                {group}
              </span>
            {/each}
          </div>
        </div>

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
      </div>
    {/if}
  </div>
</header>
