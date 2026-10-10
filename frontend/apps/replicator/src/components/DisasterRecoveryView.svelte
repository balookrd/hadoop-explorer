<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import {
    RefreshCw,
    ShieldAlert,
    AlertTriangle,
    CheckCircle2,
    RotateCcw,
    PowerOff,
    ArrowLeftRight,
    ArrowRight,
    Server,
    Database,
    Lock,
    Unlock,
    Activity,
    Wifi,
    WifiOff,
    Clock,
    Play,
    Pause,
    X,
    Filter,
    HardDrive,
    Info,
    Flame,
    Undo2,
    Trash2
  } from 'lucide-svelte';
  import { api } from '../api/client';
  import type {
    DrStatusResponse,
    DrDcStatus,
    DrClusterStatus,
    DrRouteItem,
    DrEmergencyStopPayload,
    DrReversePayload
  } from '../types';
  import { StatusBadge, type UserSession } from '@hadoop-explorer/common';

  let { user = null }: { user?: UserSession | null } = $props();

  const isReader = $derived(user?.system_role === 'reader');
  const isAdmin = $derived(user?.system_role === 'admin' || user?.is_admin === true);

  // Состояние
  let drStatus = $state<DrStatusResponse | null>(null);
  let loading = $state<boolean>(true);
  let actionLoading = $state<boolean>(false);
  let toastMessage = $state<{ type: 'success' | 'error' | 'info'; text: string } | null>(null);
  let toastTimeout: any = null;

  // Модальные окна
  let showEmergencyModal = $state<boolean>(false);
  let emergencyClusterId = $state<string>('dc1');
  let emergencyReason = $state<string>('');
  let emergencyFenceNetwork = $state<boolean>(true);

  let showReverseModal = $state<boolean>(false);
  let reverseFromCluster = $state<string>('dc2');
  let reverseToCluster = $state<string>('dc1');
  let reverseIncludeHdfs = $state<boolean>(true);
  let reverseIncludeHms = $state<boolean>(true);
  let reverseAutoStart = $state<boolean>(true);
  let reverseConfirmText = $state<string>('');

  // Фильтр маршрутов
  let routeTypeFilter = $state<'ALL' | 'HDFS' | 'HMS'>('ALL');
  let searchQuery = $state<string>('');

  let pollTimer: any = null;

  function showToast(text: string, type: 'success' | 'error' | 'info' = 'info') {
    if (toastTimeout) clearTimeout(toastTimeout);
    toastMessage = { type, text };
    toastTimeout = setTimeout(() => {
      toastMessage = null;
    }, 6000);
  }

  async function loadDrStatus(silent = false) {
    if (!silent) loading = true;
    try {
      drStatus = await api.getDrStatus();
    } catch (e: any) {
      if (!silent) {
        showToast('Не удалось загрузить статус Disaster Recovery: ' + (e?.message || e), 'error');
      }
    } finally {
      if (!silent) loading = false;
    }
  }

  async function executeEmergencyStop() {
    if (!isAdmin) {
      showToast('Управление разделом Disaster Recovery доступно только Администратору платформы (ADMIN)', 'error');
      return;
    }
    actionLoading = true;
    try {
      const resp = await api.emergencyStop({
        cluster_id: emergencyClusterId,
        reason: emergencyReason || 'Аварийный останов оператором через DR Console',
        fence_network: emergencyFenceNetwork
      });
      if (resp.success) {
        showToast(resp.message, 'success');
        showEmergencyModal = false;
        emergencyReason = '';
        await loadDrStatus();
      } else {
        showToast('Ошибка остановки: ' + resp.message, 'error');
      }
    } catch (e: any) {
      showToast('Ошибка выполнения аварийного останова: ' + (e?.message || e), 'error');
    } finally {
      actionLoading = false;
    }
  }

  async function executeReverseReplication() {
    if (!isAdmin) {
      showToast('Управление разделом Disaster Recovery доступно только Администратору платформы (ADMIN)', 'error');
      return;
    }
    if (reverseConfirmText.trim().toUpperCase() !== 'REVERSE') {
      showToast('Введите слово REVERSE для подтверждения операции', 'error');
      return;
    }

    actionLoading = true;
    try {
      const resp = await api.reverseReplication({
        from_cluster_id: reverseFromCluster,
        to_cluster_id: reverseToCluster,
        include_hdfs: reverseIncludeHdfs,
        include_hms: reverseIncludeHms,
        auto_start: reverseAutoStart
      });
      if (resp.success) {
        showToast(resp.message, 'success');
        showReverseModal = false;
        reverseConfirmText = '';
        await loadDrStatus();
      } else {
        showToast('Ошибка инверсии: ' + resp.message, 'error');
      }
    } catch (e: any) {
      showToast('Ошибка создания обратных задач: ' + (e?.message || e), 'error');
    } finally {
      actionLoading = false;
    }
  }

  async function reverseSingleJob(jobId: string) {
    if (!isAdmin) {
      showToast('Управление разделом Disaster Recovery доступно только Администратору платформы (ADMIN)', 'error');
      return;
    }
    actionLoading = true;
    try {
      const resp = await api.reverseSingleJob(jobId);
      if (resp.success) {
        showToast(resp.message, 'success');
        await loadDrStatus();
      } else {
        showToast('Ошибка: ' + resp.message, 'error');
      }
    } catch (e: any) {
      showToast('Ошибка точечного разворота: ' + (e?.message || e), 'error');
    } finally {
      actionLoading = false;
    }
  }

  async function undoReverseRoute(jobId: string, isReverseJob = false) {
    if (!isAdmin) {
      showToast('Управление разделом Disaster Recovery доступно только Администратору платформы (ADMIN)', 'error');
      return;
    }
    actionLoading = true;
    try {
      const resp = await api.undoReverse(jobId);
      if (resp.success) {
        showToast(resp.message, 'success');
        await loadDrStatus();
      } else {
        showToast('Ошибка: ' + resp.message, 'error');
      }
    } catch (e: any) {
      showToast('Ошибка отзыва зеркальной задачи: ' + (e?.message || e), 'error');
    } finally {
      actionLoading = false;
    }
  }

  function formatBytes(bytes: number): string {
    if (bytes <= 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  const allRoutes = $derived(() => {
    if (!drStatus) return [];
    let list: DrRouteItem[] = [];
    if (routeTypeFilter === 'ALL' || routeTypeFilter === 'HDFS') {
      list = [...list, ...drStatus.hdfs_routes];
    }
    if (routeTypeFilter === 'ALL' || routeTypeFilter === 'HMS') {
      list = [...list, ...drStatus.hms_routes];
    }
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      list = list.filter(r =>
        r.name.toLowerCase().includes(q) ||
        r.source_path.toLowerCase().includes(q) ||
        r.target_path.toLowerCase().includes(q) ||
        r.source_cluster_id.toLowerCase().includes(q) ||
        r.target_cluster_id.toLowerCase().includes(q)
      );
    }
    return list;
  });

  onMount(() => {
    loadDrStatus();
    pollTimer = setInterval(() => {
      loadDrStatus(true);
    }, 4000);
  });

  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
    if (toastTimeout) clearTimeout(toastTimeout);
  });
</script>

<div class="space-y-6">
  <!-- Toast уведомления -->
  {#if toastMessage}
    <div
      class="fixed top-16 right-6 z-50 flex items-center gap-3 px-4 py-3 rounded-lg shadow-xl border text-sm max-w-md transition animate-in fade-in slide-in-from-top-2 {
        toastMessage.type === 'success'
          ? 'bg-emerald-50 dark:bg-emerald-950/90 text-emerald-800 dark:text-emerald-200 border-emerald-300 dark:border-emerald-800'
          : toastMessage.type === 'error'
          ? 'bg-rose-50 dark:bg-rose-950/90 text-rose-800 dark:text-rose-200 border-rose-300 dark:border-rose-800'
          : 'bg-sky-50 dark:bg-sky-950/90 text-sky-800 dark:text-sky-200 border-sky-300 dark:border-sky-800'
      }"
    >
      {#if toastMessage.type === 'success'}
        <CheckCircle2 class="w-5 h-5 text-emerald-600 dark:text-emerald-400 shrink-0" />
      {:else if toastMessage.type === 'error'}
        <AlertTriangle class="w-5 h-5 text-rose-600 dark:text-rose-400 shrink-0" />
      {:else}
        <Info class="w-5 h-5 text-sky-600 dark:text-sky-400 shrink-0" />
      {/if}
      <span class="flex-1 font-medium">{toastMessage.text}</span>
      <button onclick={() => (toastMessage = null)} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">
        <X class="w-4 h-4" />
      </button>
    </div>
  {/if}

  <!-- 1. Заголовок и карточки быстрого статуса (Hero KPI) -->
  <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4 bg-white dark:bg-slate-900 p-5 rounded-xl border border-slate-200 dark:border-slate-800 shadow-xs">
    <div class="flex items-center gap-3.5">
      <div class="w-11 h-11 rounded-xl bg-rose-100 dark:bg-rose-950/70 border border-rose-200 dark:border-rose-900 flex items-center justify-center text-rose-600 dark:text-rose-400 shadow-2xs">
        <ShieldAlert class="w-6 h-6" />
      </div>
      <div>
        <h1 class="text-xl font-bold text-slate-900 dark:text-slate-50 flex items-center gap-2">
          Disaster Recovery & Failover Hub
          <span class="text-xs px-2 py-0.5 rounded-full font-semibold bg-rose-100 dark:bg-rose-950 text-rose-700 dark:text-rose-300 border border-rose-200 dark:border-rose-800">
            DR Console
          </span>
          {#if !isAdmin}
            <span class="text-[11px] px-2 py-0.5 rounded-full font-semibold bg-amber-100 dark:bg-amber-950 text-amber-800 dark:text-amber-300 border border-amber-200 dark:border-amber-800 flex items-center gap-1">
              <Lock class="w-3 h-3" /> READ ONLY
            </span>
          {/if}
        </h1>
        <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
          Управление межкластерной непрерывностью бизнеса, аварийная изоляция и переключение репликации
        </p>
      </div>
    </div>

    <div class="flex items-center gap-2.5">
      <button
        onclick={() => loadDrStatus()}
        disabled={loading}
        class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-200 hover:bg-slate-200 dark:hover:bg-slate-700 border border-slate-300 dark:border-slate-700 transition cursor-pointer disabled:opacity-50"
      >
        <RefreshCw class="w-3.5 h-3.5 {loading ? 'animate-spin' : ''}" />
        Обновить статус
      </button>

      {#if isAdmin}
        <button
          onclick={() => {
            emergencyClusterId = 'dc1';
            showEmergencyModal = true;
          }}
          disabled={actionLoading}
          class="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold bg-rose-600 hover:bg-rose-700 text-white shadow-xs transition cursor-pointer disabled:opacity-50"
        >
          <PowerOff class="w-4 h-4" />
          🛑 Kill-Switch (Стоп DC1)
        </button>

        <button
          onclick={() => {
            reverseFromCluster = 'dc2';
            reverseToCluster = 'dc1';
            showReverseModal = true;
          }}
          disabled={actionLoading}
          class="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold bg-indigo-600 hover:bg-indigo-700 text-white shadow-xs transition cursor-pointer disabled:opacity-50"
        >
          <RotateCcw class="w-4 h-4" />
          🔄 Reverse Replication (DC2 ➔ DC1)
        </button>
      {/if}
    </div>
  </div>

  {#if !isAdmin}
    <!-- Информационная плашка для не-админов (Read-Only) -->
    <div class="p-3.5 bg-amber-50 dark:bg-amber-950/40 border border-amber-200 dark:border-amber-800 rounded-xl flex items-center gap-3 text-xs text-amber-800 dark:text-amber-200 font-medium">
      <ShieldAlert class="w-5 h-5 text-amber-600 shrink-0" />
      <div>
        <span class="font-bold">Режим аудита и мониторинга:</span> Управление аварийным остановом (Kill-Switch), разворотом репликации и отзывом зеркал доступно только <strong>Администратору платформы (ADMIN)</strong>.
      </div>
    </div>
  {/if}

  <!-- 2. Интерактивная архитектура топологии и потока данных (Data Flow Canvas) -->
  {#if drStatus}
    <div class="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 p-6 shadow-xs">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-sm font-bold text-slate-800 dark:text-slate-200 uppercase tracking-wider flex items-center gap-2">
          <Activity class="w-4 h-4 text-sky-500" />
          Топология дата-центров и актуальное направление данных
        </h2>
        <div class="flex items-center gap-4 text-xs font-medium text-slate-500 dark:text-slate-400">
          <span class="flex items-center gap-1.5">
            <span class="w-2.5 h-2.5 rounded-full bg-emerald-500"></span> Online
          </span>
          <span class="flex items-center gap-1.5">
            <span class="w-2.5 h-2.5 rounded-full bg-amber-500"></span> Degraded
          </span>
          <span class="flex items-center gap-1.5">
            <span class="w-2.5 h-2.5 rounded-full bg-rose-500"></span> Offline
          </span>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-7 gap-4 items-center">
        <!-- DC1 Карточка -->
        {#each drStatus.datacenters.filter(d => d.id === 'dc1') as dc1}
          <div class="lg:col-span-3 p-5 rounded-xl border-2 transition relative overflow-hidden {
            dc1.status === 'ONLINE'
              ? 'bg-slate-50/80 dark:bg-slate-950/60 border-emerald-500/60 dark:border-emerald-600/60'
              : dc1.status === 'DEGRADED'
              ? 'bg-amber-50/50 dark:bg-amber-950/30 border-amber-500/60'
              : 'bg-rose-50/40 dark:bg-rose-950/30 border-rose-500/60'
          }">
            <div class="flex items-start justify-between">
              <div>
                <div class="flex items-center gap-2">
                  <span class="text-base font-extrabold text-slate-900 dark:text-slate-100">
                    {dc1.name}
                  </span>
                  <span class="text-[10px] font-bold px-2 py-0.5 rounded-full uppercase {
                    dc1.role === 'PRIMARY'
                      ? 'bg-sky-100 dark:bg-sky-950 text-sky-700 dark:text-sky-300 border border-sky-300 dark:border-sky-800'
                      : 'bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300'
                  }">
                    {dc1.role}
                  </span>
                </div>
                <span class="text-xs text-slate-500 dark:text-slate-400 font-mono">Cluster ID: {dc1.id}</span>
              </div>

              <!-- Статус бэйдж -->
              <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-bold {
                dc1.status === 'ONLINE'
                  ? 'bg-emerald-100 dark:bg-emerald-950/80 text-emerald-800 dark:text-emerald-300 border border-emerald-300 dark:border-emerald-800'
                  : dc1.status === 'DEGRADED'
                  ? 'bg-amber-100 dark:bg-amber-950/80 text-amber-800 dark:text-amber-300 border border-amber-300 dark:border-amber-800'
                  : 'bg-rose-100 dark:bg-rose-950/80 text-rose-800 dark:text-rose-300 border border-rose-300 dark:border-rose-800'
              }">
                {#if dc1.status === 'ONLINE'}
                  <Wifi class="w-3.5 h-3.5" /> ONLINE
                {:else if dc1.status === 'DEGRADED'}
                  <AlertTriangle class="w-3.5 h-3.5" /> DEGRADED
                {:else}
                  <WifiOff class="w-3.5 h-3.5" /> OFFLINE
                {/if}
              </span>
            </div>

            <div class="mt-4 pt-3 border-t border-slate-200 dark:border-slate-800/80 grid grid-cols-2 gap-3 text-xs">
              <div>
                <span class="text-slate-400 block text-[11px]">Агенты Full-Duplex</span>
                <span class="font-bold text-slate-800 dark:text-slate-200 font-mono">
                  {dc1.online_agents} / {dc1.total_agents} Online
                </span>
              </div>
              <div>
                <span class="text-slate-400 block text-[11px]">Шейпер пропускной способности</span>
                <span class="font-bold font-mono {dc1.is_fenced ? 'text-rose-600 dark:text-rose-400' : 'text-slate-800 dark:text-slate-200'}">
                  {dc1.is_fenced ? '0 МБ/с (FENCED 🔒)' : `${dc1.bandwidth_limit_mb_s} МБ/с`}
                </span>
              </div>
            </div>

            {#if isAdmin && dc1.status !== 'OFFLINE'}
              <div class="mt-3 pt-2">
                <button
                  onclick={() => {
                    emergencyClusterId = dc1.id;
                    showEmergencyModal = true;
                  }}
                  class="text-xs font-semibold text-rose-600 dark:text-rose-400 hover:underline flex items-center gap-1 cursor-pointer"
                >
                  <PowerOff class="w-3 h-3" /> Экстренно изолировать {dc1.id}
                </button>
              </div>
            {/if}
          </div>
        {/each}

        <!-- Центральный мост WAN и стрелка направления -->
        <div class="lg:col-span-1 flex flex-col items-center justify-center p-2 text-center gap-2">
          <div class="text-[10px] font-bold uppercase tracking-wider text-slate-400">
            WAN Канал
          </div>

          <div class="w-full flex items-center justify-center">
            {#if drStatus.summary.active_source_dc === 'dc1'}
              <!-- Направление DC1 ➔ DC2 -->
              <div class="flex flex-col items-center gap-1 text-sky-600 dark:text-sky-400 animate-pulse">
                <div class="flex items-center gap-1">
                  <span class="text-[11px] font-bold">DC1</span>
                  <ArrowRight class="w-5 h-5 stroke-[2.5]" />
                  <span class="text-[11px] font-bold">DC2</span>
                </div>
                <span class="text-[9px] font-mono text-slate-500 dark:text-slate-400">Прямой поток</span>
              </div>
            {:else}
              <!-- Направление DC2 ➔ DC1 (Reverse) -->
              <div class="flex flex-col items-center gap-1 text-indigo-600 dark:text-indigo-400 animate-pulse">
                <div class="flex items-center gap-1">
                  <span class="text-[11px] font-bold">DC2</span>
                  <ArrowRight class="w-5 h-5 stroke-[2.5]" />
                  <span class="text-[11px] font-bold">DC1</span>
                </div>
                <span class="text-[9px] font-bold uppercase tracking-widest text-indigo-500">Reverse (DR)</span>
              </div>
            {/if}
          </div>

          <div class="mt-1">
            <span class="text-[10px] px-2 py-0.5 rounded-full font-mono bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 border border-slate-200 dark:border-slate-700">
              Lag: {formatBytes(drStatus.summary.unreplicated_bytes)}
            </span>
          </div>
        </div>

        <!-- DC2 Карточка -->
        {#each drStatus.datacenters.filter(d => d.id === 'dc2') as dc2}
          <div class="lg:col-span-3 p-5 rounded-xl border-2 transition relative overflow-hidden {
            dc2.status === 'ONLINE'
              ? 'bg-slate-50/80 dark:bg-slate-950/60 border-emerald-500/60 dark:border-emerald-600/60'
              : dc2.status === 'DEGRADED'
              ? 'bg-amber-50/50 dark:bg-amber-950/30 border-amber-500/60'
              : 'bg-rose-50/40 dark:bg-rose-950/30 border-rose-500/60'
          }">
            <div class="flex items-start justify-between">
              <div>
                <div class="flex items-center gap-2">
                  <span class="text-base font-extrabold text-slate-900 dark:text-slate-100">
                    {dc2.name}
                  </span>
                  <span class="text-[10px] font-bold px-2 py-0.5 rounded-full uppercase {
                    dc2.role === 'PROMOTED_PRIMARY'
                      ? 'bg-indigo-100 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-300 border border-indigo-300 dark:border-indigo-800'
                      : 'bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300'
                  }">
                    {dc2.role}
                  </span>
                </div>
                <span class="text-xs text-slate-500 dark:text-slate-400 font-mono">Cluster ID: {dc2.id}</span>
              </div>

              <!-- Статус бэйдж -->
              <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-bold {
                dc2.status === 'ONLINE'
                  ? 'bg-emerald-100 dark:bg-emerald-950/80 text-emerald-800 dark:text-emerald-300 border border-emerald-300 dark:border-emerald-800'
                  : dc2.status === 'DEGRADED'
                  ? 'bg-amber-100 dark:bg-amber-950/80 text-amber-800 dark:text-amber-300 border border-amber-300 dark:border-amber-800'
                  : 'bg-rose-100 dark:bg-rose-950/80 text-rose-800 dark:text-rose-300 border border-rose-300 dark:border-rose-800'
              }">
                {#if dc2.status === 'ONLINE'}
                  <Wifi class="w-3.5 h-3.5" /> ONLINE
                {:else if dc2.status === 'DEGRADED'}
                  <AlertTriangle class="w-3.5 h-3.5" /> DEGRADED
                {:else}
                  <WifiOff class="w-3.5 h-3.5" /> OFFLINE
                {/if}
              </span>
            </div>

            <div class="mt-4 pt-3 border-t border-slate-200 dark:border-slate-800/80 grid grid-cols-2 gap-3 text-xs">
              <div>
                <span class="text-slate-400 block text-[11px]">Агенты Full-Duplex</span>
                <span class="font-bold text-slate-800 dark:text-slate-200 font-mono">
                  {dc2.online_agents} / {dc2.total_agents} Online
                </span>
              </div>
              <div>
                <span class="text-slate-400 block text-[11px]">Шейпер пропускной способности</span>
                <span class="font-bold font-mono {dc2.is_fenced ? 'text-rose-600 dark:text-rose-400' : 'text-slate-800 dark:text-slate-200'}">
                  {dc2.is_fenced ? '0 МБ/с (FENCED 🔒)' : `${dc2.bandwidth_limit_mb_s} МБ/с`}
                </span>
              </div>
            </div>

            {#if isAdmin}
              <div class="mt-3 pt-2">
                <button
                  onclick={() => {
                    reverseFromCluster = dc2.id;
                    reverseToCluster = 'dc1';
                    showReverseModal = true;
                  }}
                  class="text-xs font-semibold text-indigo-600 dark:text-indigo-400 hover:underline flex items-center gap-1 cursor-pointer"
                >
                  <RotateCcw class="w-3 h-3" /> Настроить репликацию из {dc2.id}
                </button>
              </div>
            {/if}
          </div>
        {/each}
      </div>

      <!-- Суммарные счетчики -->
      <div class="mt-6 pt-5 border-t border-slate-200 dark:border-slate-800 grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div class="p-3 rounded-lg bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800">
          <span class="text-[11px] font-semibold text-slate-500 dark:text-slate-400 block">Активные репликации</span>
          <span class="text-lg font-bold text-emerald-600 dark:text-emerald-400">{drStatus.summary.total_active_jobs}</span>
        </div>
        <div class="p-3 rounded-lg bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800">
          <span class="text-[11px] font-semibold text-slate-500 dark:text-slate-400 block">Остановлено (Kill-Switch)</span>
          <span class="text-lg font-bold text-slate-700 dark:text-slate-300">{drStatus.summary.total_frozen_jobs}</span>
        </div>
        <div class="p-3 rounded-lg bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800">
          <span class="text-[11px] font-semibold text-slate-500 dark:text-slate-400 block">Непереданный объем HDFS</span>
          <span class="text-lg font-bold text-sky-600 dark:text-sky-400 font-mono">{formatBytes(drStatus.summary.unreplicated_bytes)}</span>
        </div>
        <div class="p-3 rounded-lg bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800">
          <span class="text-[11px] font-semibold text-slate-500 dark:text-slate-400 block">Лаг событий HMS CDC</span>
          <span class="text-lg font-bold text-indigo-600 dark:text-indigo-400 font-mono">{drStatus.summary.unreplicated_events} events</span>
        </div>
      </div>
    </div>
  {/if}

  <!-- 3. Таблица маршрутов репликации и точечный разворот (Route Matrix) -->
  <div class="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 p-5 shadow-xs">
    <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
      <div>
        <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
          <ArrowLeftRight class="w-4 h-4 text-sky-500" />
          Репликационные маршруты и статус синхронизации дельты
        </h3>
        <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
          Каталоги HDFS и базы данных Hive Metastore с возможностью индивидуальной инверсии направления
        </p>
      </div>

      <!-- Фильтры и поиск -->
      <div class="flex items-center gap-2">
        <div class="inline-flex rounded-lg border border-slate-200 dark:border-slate-800 p-0.5 bg-slate-50 dark:bg-slate-950 text-xs">
          <button
            onclick={() => (routeTypeFilter = 'ALL')}
            class="px-2.5 py-1 rounded font-semibold transition cursor-pointer {routeTypeFilter === 'ALL' ? 'bg-sky-600 text-white shadow-2xs' : 'text-slate-600 dark:text-slate-400'}"
          >
            Все
          </button>
          <button
            onclick={() => (routeTypeFilter = 'HDFS')}
            class="px-2.5 py-1 rounded font-semibold transition cursor-pointer {routeTypeFilter === 'HDFS' ? 'bg-sky-600 text-white shadow-2xs' : 'text-slate-600 dark:text-slate-400'}"
          >
            HDFS
          </button>
          <button
            onclick={() => (routeTypeFilter = 'HMS')}
            class="px-2.5 py-1 rounded font-semibold transition cursor-pointer {routeTypeFilter === 'HMS' ? 'bg-sky-600 text-white shadow-2xs' : 'text-slate-600 dark:text-slate-400'}"
          >
            HMS Metastore
          </button>
        </div>

        <input
          type="text"
          placeholder="Поиск по пути/БД..."
          bind:value={searchQuery}
          class="text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500 w-44"
        />
      </div>
    </div>

    <!-- Таблица -->
    <div class="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
      <table class="w-full text-left text-xs border-collapse">
        <thead class="bg-slate-50 dark:bg-slate-950 text-slate-500 dark:text-slate-400 font-semibold border-b border-slate-200 dark:border-slate-800 uppercase tracking-wider text-[10px]">
          <tr>
            <th class="py-2.5 px-3">Тип</th>
            <th class="py-2.5 px-3">Маршрут (Источник ➔ Цель)</th>
            <th class="py-2.5 px-3">Каталог / База данных</th>
            <th class="py-2.5 px-3">Статус</th>
            <th class="py-2.5 px-3">Отставание (Лаг)</th>
            <th class="py-2.5 px-3">Обратная задача</th>
            <th class="py-2.5 px-3 text-right">Действия</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60 font-sans">
          {#if allRoutes().length === 0}
            <tr>
              <td colspan="7" class="py-8 text-center text-slate-400 dark:text-slate-500">
                Маршруты репликации не найдены
              </td>
            </tr>
          {:else}
            {#each allRoutes() as route}
              <tr class="hover:bg-slate-50/70 dark:hover:bg-slate-950/40 transition">
                <td class="py-2.5 px-3 font-semibold">
                  <div class="flex items-center gap-1.5 flex-wrap">
                    {#if route.type === 'HDFS'}
                      <span class="inline-flex items-center gap-1 text-sky-600 dark:text-sky-400">
                        <HardDrive class="w-3.5 h-3.5" /> HDFS
                      </span>
                    {:else}
                      <span class="inline-flex items-center gap-1 text-indigo-600 dark:text-indigo-400">
                        <Database class="w-3.5 h-3.5" /> HMS
                      </span>
                    {/if}
                    {#if route.is_reverse_replica}
                      <span class="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-violet-100 dark:bg-violet-950/80 text-violet-700 dark:text-violet-300 border border-violet-200 dark:border-violet-800" title="Задача создана в рамках обратной репликации">
                        Зеркало ⇄
                      </span>
                    {/if}
                  </div>
                </td>

                <td class="py-2.5 px-3 font-mono font-medium text-slate-700 dark:text-slate-300">
                  <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-[11px]">
                    {route.source_cluster_id}
                  </span>
                  <span class="text-slate-400 mx-1">➔</span>
                  <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-[11px]">
                    {route.target_cluster_id}
                  </span>
                </td>

                <td class="py-2.5 px-3 font-mono text-slate-600 dark:text-slate-300 max-w-xs truncate" title={route.name}>
                  {route.name}
                </td>

                <td class="py-2.5 px-3">
                  <div class="flex flex-col gap-0.5">
                    <StatusBadge status={route.status} />
                    {#if route.status === 'FAILED' && route.message}
                      <span class="text-[10px] text-rose-600 dark:text-rose-400 max-w-[190px] truncate cursor-help hover:underline" title={route.message}>
                        ⚠ {route.message}
                      </span>
                    {/if}
                  </div>
                </td>

                <td class="py-2.5 px-3 font-mono font-semibold">
                  {#if route.type === 'HDFS'}
                    <span class="{route.lag_bytes_or_events > 0 ? 'text-amber-600 dark:text-amber-400' : 'text-emerald-600 dark:text-emerald-400'}">
                      {formatBytes(route.lag_bytes_or_events)}
                    </span>
                  {:else}
                    <span class="{route.lag_bytes_or_events > 0 ? 'text-amber-600 dark:text-amber-400' : 'text-emerald-600 dark:text-emerald-400'}">
                      {route.lag_bytes_or_events} events
                    </span>
                  {/if}
                </td>

                <td class="py-2.5 px-3">
                  {#if route.has_reverse_job}
                    <div class="inline-flex items-center gap-1.5 flex-wrap">
                      <span class="inline-flex items-center gap-1 text-emerald-600 dark:text-emerald-400 text-[11px] font-semibold">
                        <CheckCircle2 class="w-3.5 h-3.5" />
                        {route.reverse_job_id ? route.reverse_job_id.substring(0, 10) : 'Настроена'}
                      </span>
                      {#if route.reverse_job_status}
                        <span class="px-1.5 py-0.5 rounded text-[10px] font-mono font-semibold {
                          route.reverse_job_status === 'RUNNING' || route.reverse_job_status === 'ACTIVE'
                            ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300'
                            : route.reverse_job_status === 'FAILED' || route.reverse_job_status === 'ERROR'
                            ? 'bg-rose-100 text-rose-800 dark:bg-rose-950 dark:text-rose-300'
                            : 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300'
                        }">
                          {route.reverse_job_status}
                        </span>
                      {/if}
                    </div>
                  {:else}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px]">
                      Не создана
                    </span>
                  {/if}
                </td>

                <td class="py-2.5 px-3 text-right">
                  {#if isAdmin}
                    <div class="inline-flex items-center gap-1.5 justify-end">
                      {#if !route.has_reverse_job && !route.is_reverse_replica && route.type === 'HDFS'}
                        <button
                          onclick={() => reverseSingleJob(route.id)}
                          disabled={actionLoading}
                          class="inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] font-semibold bg-sky-50 dark:bg-sky-950 text-sky-700 dark:text-sky-300 border border-sky-300 dark:border-sky-800 hover:bg-sky-100 dark:hover:bg-sky-900 transition cursor-pointer disabled:opacity-50"
                          title="Создать зеркальную задачу репликации в обратную сторону"
                        >
                          <RotateCcw class="w-3 h-3" /> Развернуть ⇄
                        </button>
                      {:else if route.has_reverse_job && !route.is_reverse_replica}
                        <!-- Исходная прямая задача: возможность отозвать зеркало и вернуть исходную кнопку -->
                        <button
                          onclick={() => undoReverseRoute(route.id)}
                          disabled={actionLoading}
                          class="inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] font-semibold bg-amber-50 dark:bg-amber-950 text-amber-700 dark:text-amber-300 border border-amber-300 dark:border-amber-800 hover:bg-amber-100 dark:hover:bg-amber-900 transition cursor-pointer disabled:opacity-50"
                          title="Отозвать обратное зеркало ({route.reverse_job_id}) и вернуть кнопку разворота"
                        >
                          <Undo2 class="w-3 h-3" /> Отозвать ↩
                        </button>
                      {:else if route.is_reverse_replica}
                        <!-- Сама обратная задача: кнопка быстрого удаления зеркала -->
                        <button
                          onclick={() => undoReverseRoute(route.id, true)}
                          disabled={actionLoading}
                          class="inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] font-semibold bg-rose-50 dark:bg-rose-950 text-rose-700 dark:text-rose-300 border border-rose-300 dark:border-rose-800 hover:bg-rose-100 dark:hover:bg-rose-900 transition cursor-pointer disabled:opacity-50"
                          title="Удалить эту зеркальную задачу и разблокировать прямой маршрут"
                        >
                          <Trash2 class="w-3 h-3" /> Удалить зеркало ✕
                        </button>
                      {:else}
                        <span class="text-[11px] text-slate-400">—</span>
                      {/if}
                    </div>
                  {:else}
                    <span class="text-[11px] text-slate-400">—</span>
                  {/if}
                </td>
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </div>
</div>

<!-- Модальное окно аварийного останова (Emergency Kill-Switch Modal) -->
{#if isAdmin && showEmergencyModal}
  <div class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-xs animate-in fade-in">
    <div class="bg-white dark:bg-slate-900 rounded-xl border border-rose-300 dark:border-rose-800 shadow-2xl max-w-lg w-full p-6 space-y-4">
      <div class="flex items-center gap-3 text-rose-600 dark:text-rose-400">
        <div class="w-10 h-10 rounded-full bg-rose-100 dark:bg-rose-950/80 flex items-center justify-center">
          <PowerOff class="w-5 h-5" />
        </div>
        <div>
          <h3 class="text-base font-bold text-slate-900 dark:text-slate-100">
            Экстренный останов репликации (Kill-Switch)
          </h3>
          <span class="text-xs text-rose-600 dark:text-rose-400 font-semibold">
            Защита от Split-Brain и аварийная изоляция кластера
          </span>
        </div>
      </div>

      <div class="p-3.5 rounded-lg bg-rose-50 dark:bg-rose-950/40 border border-rose-200 dark:border-rose-900/60 text-xs text-rose-800 dark:text-rose-300 space-y-1">
        <p class="font-bold flex items-center gap-1.5">
          <AlertTriangle class="w-4 h-4 shrink-0 text-rose-600" />
          Внимание: подтверждение данной операции выполнит:
        </p>
        <ul class="list-disc list-inside space-y-0.5 pl-1 text-[11px]">
          <li>Немедленный перевод всех активных задач HDFS кластера <strong>{emergencyClusterId}</strong> в статус <code>STOPPED</code>;</li>
          <li>Отключение расписания (Cron-шедулера), предотвращая перезапуск задач;</li>
          <li>Приостановку CDC стриминга Hive Metastore (пауза очередей);</li>
          <li>Установку квоты сетевого канала в 0 МБ/с (Network Fencing) для изоляции трафика.</li>
        </ul>
      </div>

      <div class="space-y-3 text-xs">
        <div>
          <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Кластер-источник для изоляции
          </label>
          <select
            bind:value={emergencyClusterId}
            class="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-950 font-mono text-xs focus:ring-1 focus:ring-rose-500"
          >
            <option value="dc1">dc1 (HDFS DC1 Production)</option>
            <option value="dc2">dc2 (HDFS DC2 Disaster Recovery)</option>
          </select>
        </div>

        <div>
          <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Причина экстренной остановки (аудит-лог)
          </label>
          <input
            type="text"
            placeholder="например: Отключение электропитания ЦОД1 / Разрыв оптического ввода"
            bind:value={emergencyReason}
            class="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-950 text-xs focus:ring-1 focus:ring-rose-500"
          />
        </div>

        <div class="flex items-center gap-2 pt-1">
          <input
            type="checkbox"
            id="fenceNet"
            bind:checked={emergencyFenceNetwork}
            class="rounded border-slate-300 text-rose-600 focus:ring-rose-500"
          />
          <label for="fenceNet" class="font-medium text-slate-700 dark:text-slate-300 cursor-pointer">
            Сетевое ограждение: перекрыть пропускную способность шейпера (0 МБ/с Fencing)
          </label>
        </div>
      </div>

      <div class="flex items-center justify-end gap-2.5 pt-3 border-t border-slate-200 dark:border-slate-800">
        <button
          onclick={() => (showEmergencyModal = false)}
          disabled={actionLoading}
          class="px-4 py-2 rounded-lg text-xs font-semibold bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-700 transition cursor-pointer"
        >
          Отмена
        </button>
        <button
          onclick={executeEmergencyStop}
          disabled={actionLoading}
          class="px-4 py-2 rounded-lg text-xs font-bold bg-rose-600 hover:bg-rose-700 text-white shadow-xs transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5"
        >
          {#if actionLoading}
            <RefreshCw class="w-3.5 h-3.5 animate-spin" />
            Выполнение...
          {:else}
            <PowerOff class="w-3.5 h-3.5" />
            Подтвердить экстренный останов
          {/if}
        </button>
      </div>
    </div>
  </div>
{/if}

<!-- Модальное окно обратной репликации (Reverse Replication Modal) -->
{#if isAdmin && showReverseModal}
  <div class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-xs animate-in fade-in">
    <div class="bg-white dark:bg-slate-900 rounded-xl border border-indigo-300 dark:border-indigo-800 shadow-2xl max-w-lg w-full p-6 space-y-4">
      <div class="flex items-center gap-3 text-indigo-600 dark:text-indigo-400">
        <div class="w-10 h-10 rounded-full bg-indigo-100 dark:bg-indigo-950/80 flex items-center justify-center">
          <RotateCcw class="w-5 h-5" />
        </div>
        <div>
          <h3 class="text-base font-bold text-slate-900 dark:text-slate-100">
            Запуск обратной репликации (Reverse Replication)
          </h3>
          <span class="text-xs text-indigo-600 dark:text-indigo-400 font-semibold">
            Синхронизация накопившейся дельты изменений из DR-кластера
          </span>
        </div>
      </div>

      <div class="p-3.5 rounded-lg bg-indigo-50 dark:bg-indigo-950/40 border border-indigo-200 dark:border-indigo-900/60 text-xs text-indigo-800 dark:text-indigo-300 space-y-1">
        <p class="font-bold flex items-center gap-1.5">
          <Info class="w-4 h-4 shrink-0 text-indigo-600" />
          Порядок работы Failback & Дельта-синхронизации:
        </p>
        <ul class="list-disc list-inside space-y-0.5 pl-1 text-[11px]">
          <li>Агенты в {reverseFromCluster} будут переключены в режим Sender/Streamer;</li>
          <li>Для каждого каталога HDFS и схемы HMS создается обратное правило <code>{reverseFromCluster} ➔ {reverseToCluster}</code>;</li>
          <li>Агенты сверят контрольные суммы и потоком передадут все файлы, созданные за время работы в DR-режиме;</li>
          <li>Лимиты полосы шейпера восстанавливаются в штатные 100 МБ/с.</li>
        </ul>
      </div>

      <div class="space-y-3 text-xs">
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Новый источник (Primary)
            </label>
            <input
              type="text"
              readonly
              bind:value={reverseFromCluster}
              class="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 bg-slate-100 dark:bg-slate-800 font-mono text-xs text-slate-700 dark:text-slate-300"
            />
          </div>
          <div>
            <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Целевой кластер (Восстановленный)
            </label>
            <input
              type="text"
              readonly
              bind:value={reverseToCluster}
              class="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 bg-slate-100 dark:bg-slate-800 font-mono text-xs text-slate-700 dark:text-slate-300"
            />
          </div>
        </div>

        <div class="space-y-1.5 pt-1">
          <label class="flex items-center gap-2 cursor-pointer font-medium text-slate-700 dark:text-slate-300">
            <input type="checkbox" bind:checked={reverseIncludeHdfs} class="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" />
            Инвертировать каталоги HDFS репликации
          </label>
          <label class="flex items-center gap-2 cursor-pointer font-medium text-slate-700 dark:text-slate-300">
            <input type="checkbox" bind:checked={reverseIncludeHms} class="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" />
            Инвертировать базы данных Hive Metastore (CDC синхронизация)
          </label>
          <label class="flex items-center gap-2 cursor-pointer font-medium text-slate-700 dark:text-slate-300">
            <input type="checkbox" bind:checked={reverseAutoStart} class="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" />
            Автоматически перевести созданные задачи в очередь выполнения (QUEUED)
          </label>
        </div>

        <div class="pt-2 border-t border-slate-200 dark:border-slate-800">
          <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Для подтверждения введите слово <span class="font-mono text-indigo-600 font-bold">REVERSE</span>:
          </label>
          <input
            type="text"
            placeholder="REVERSE"
            bind:value={reverseConfirmText}
            class="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-950 font-mono text-xs focus:ring-1 focus:ring-indigo-500 uppercase"
          />
        </div>
      </div>

      <div class="flex items-center justify-end gap-2.5 pt-3 border-t border-slate-200 dark:border-slate-800">
        <button
          onclick={() => (showReverseModal = false)}
          disabled={actionLoading}
          class="px-4 py-2 rounded-lg text-xs font-semibold bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-700 transition cursor-pointer"
        >
          Отмена
        </button>
        <button
          onclick={executeReverseReplication}
          disabled={actionLoading || reverseConfirmText.trim().toUpperCase() !== 'REVERSE'}
          class="px-4 py-2 rounded-lg text-xs font-bold bg-indigo-600 hover:bg-indigo-700 text-white shadow-xs transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5"
        >
          {#if actionLoading}
            <RefreshCw class="w-3.5 h-3.5 animate-spin" />
            Запуск...
          {:else}
            <RotateCcw class="w-3.5 h-3.5" />
            Запустить обратную синхронизацию
          {/if}
        </button>
      </div>
    </div>
  </div>
{/if}
