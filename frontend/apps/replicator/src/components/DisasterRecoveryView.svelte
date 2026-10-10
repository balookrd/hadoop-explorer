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
  // В демо-режиме без авторизации (user == null) или для admin/operator считаем права полными
  const isAdmin = $derived(!user || user?.system_role === 'admin' || user?.is_admin === true || user?.system_role === 'operator');

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

  let showRollbackModal = $state<boolean>(false);
  let rollbackClusterId = $state<string>('dc1');
  let rollbackRestoreNetwork = $state<boolean>(true);
  let rollbackResumeHms = $state<boolean>(true);
  let rollbackResumeHdfs = $state<boolean>(true);

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

  async function executeRollbackEmergencyStop(targetClusterId?: string) {
    if (!isAdmin) {
      showToast('Управление разделом Disaster Recovery доступно только Администратору платформы (ADMIN)', 'error');
      return;
    }
    const cId = (typeof targetClusterId === 'string' && targetClusterId) ? targetClusterId : (rollbackClusterId || drStatus?.summary?.fenced_cluster_id || 'dc1');
    actionLoading = true;
    try {
      const resp = await api.rollbackEmergencyStop({
        cluster_id: cId,
        restore_network: rollbackRestoreNetwork,
        resume_hms: rollbackResumeHms,
        resume_hdfs: rollbackResumeHdfs
      });
      if (resp.success) {
        showToast(resp.message, 'success');
        showRollbackModal = false;
        await loadDrStatus();
      } else {
        showToast('Ошибка отката: ' + resp.message, 'error');
      }
    } catch (e: any) {
      showToast('Ошибка выполнения отката аварийного останова: ' + (e?.message || e), 'error');
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

      {#if drStatus?.summary?.kill_switch_active}
        <!-- Режим активной аварии: кнопка снятия изоляции и отката -->
        <button
          onclick={() => {
            if (!isAdmin) return;
            rollbackClusterId = drStatus?.summary?.fenced_cluster_id || 'dc1';
            showRollbackModal = true;
          }}
          disabled={actionLoading || !isAdmin}
          class="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold {isAdmin ? 'bg-amber-600 hover:bg-amber-500 text-white shadow-md ring-2 ring-amber-400 cursor-pointer animate-pulse' : 'bg-amber-950/40 text-amber-300/60 border border-amber-800/40 cursor-not-allowed opacity-60'} transition disabled:opacity-50"
          title={isAdmin ? "Снять сетевое ограждение и возобновить репликацию" : "Требуются права Администратора платформы"}
        >
          <Unlock class="w-4 h-4" />
          🛡️ Снять изоляцию / Откат ({drStatus.summary.fenced_cluster_id ? drStatus.summary.fenced_cluster_id.toUpperCase() : 'DC1'})
        </button>
      {:else}
        <!-- Штатный режим: кнопка экстренного останова Kill-Switch -->
        <button
          onclick={() => {
            if (!isAdmin) return;
            emergencyClusterId = 'dc1';
            showEmergencyModal = true;
          }}
          disabled={actionLoading || !isAdmin}
          class="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold {isAdmin ? 'bg-rose-600 hover:bg-rose-700 text-white shadow-xs cursor-pointer' : 'bg-rose-950/30 text-rose-300/60 border border-rose-800/40 cursor-not-allowed opacity-60'} transition disabled:opacity-50"
          title={isAdmin ? "Экстренный останов и изоляция сетевого канала (Kill-Switch)" : "Требуются права Администратора платформы"}
        >
          <PowerOff class="w-4 h-4" />
          🛑 Kill-Switch (Стоп DC1)
        </button>
      {/if}

      <button
        onclick={() => {
          if (!isAdmin) return;
          reverseFromCluster = 'dc2';
          reverseToCluster = 'dc1';
          showReverseModal = true;
        }}
        disabled={actionLoading || !isAdmin}
        class="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold {isAdmin ? 'bg-indigo-600 hover:bg-indigo-700 text-white shadow-xs cursor-pointer' : 'bg-slate-800 text-slate-500 cursor-not-allowed opacity-60'} transition disabled:opacity-50"
        title={isAdmin ? "Развернуть направление репликации" : "Требуются права Администратора платформы"}
      >
        <RotateCcw class="w-4 h-4" />
        🔄 Reverse Replication (DC2 ➔ DC1)
      </button>
    </div>
  </div>

  {#if drStatus?.summary?.kill_switch_active}
    <!-- Баннер активного Kill-Switch режима -->
    <div class="p-4 bg-rose-50 dark:bg-rose-950/50 border border-rose-300 dark:border-rose-800 rounded-xl flex items-center justify-between gap-4 text-xs shadow-xs animate-in fade-in duration-200">
      <div class="flex items-center gap-3">
        <div class="p-2 rounded-lg bg-rose-100 dark:bg-rose-900/60 text-rose-600 dark:text-rose-400 shrink-0">
          <Flame class="w-5 h-5 animate-pulse" />
        </div>
        <div>
          <div class="font-bold text-rose-900 dark:text-rose-200 text-sm flex items-center gap-2">
            <span>Аварийная изоляция активна (Kill-Switch)</span>
            <span class="px-2 py-0.5 rounded text-[10px] font-mono bg-rose-200 dark:bg-rose-900 text-rose-800 dark:text-rose-200 font-bold uppercase">
              {drStatus.summary.fenced_cluster_id || 'DC1'}
            </span>
          </div>
          <div class="text-rose-700 dark:text-rose-300 mt-0.5 leading-relaxed">
            Сетевой канал перекрыт (0 МБ/с). Задачи HDFS и схемы HMS переведены в режим ожидания.
            {#if drStatus.summary.last_emergency_reason}
              Причина: <span class="italic font-medium">«{drStatus.summary.last_emergency_reason}»</span>
            {/if}
          </div>
        </div>
      </div>
      <button
        onclick={() => {
          if (!isAdmin) return;
          rollbackClusterId = drStatus?.summary?.fenced_cluster_id || 'dc1';
          showRollbackModal = true;
        }}
        disabled={actionLoading || !isAdmin}
        class="px-4 py-2 rounded-xl text-xs font-bold {isAdmin ? 'bg-rose-600 hover:bg-rose-500 text-white shadow-md cursor-pointer' : 'bg-slate-300 dark:bg-slate-800 text-slate-500 cursor-not-allowed opacity-60'} transition shrink-0 flex items-center gap-1.5"
        title={isAdmin ? "Снять изоляцию и возобновить репликацию" : "Требуются права Администратора платформы"}
      >
        <RotateCcw class="w-4 h-4" />
        Снять изоляцию и возобновить
      </button>
    </div>
  {/if}

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
          {@const isDc1Fenced = dc1.is_fenced || (drStatus.summary.kill_switch_active && (drStatus.summary.fenced_cluster_id === dc1.id || drStatus.summary.fenced_cluster_id === 'dc1'))}
          <div class="lg:col-span-3 p-5 rounded-xl border-2 transition relative overflow-hidden {
            isDc1Fenced
              ? 'bg-rose-50/90 dark:bg-rose-950/50 border-rose-600 dark:border-rose-500 ring-2 ring-rose-500/40 shadow-md'
              : dc1.status === 'ONLINE'
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
                    isDc1Fenced
                      ? 'bg-rose-200 dark:bg-rose-900 text-rose-800 dark:text-rose-200 border border-rose-400'
                      : dc1.role === 'PRIMARY'
                      ? 'bg-sky-100 dark:bg-sky-950 text-sky-700 dark:text-sky-300 border border-sky-300 dark:border-sky-800'
                      : 'bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300'
                  }">
                    {isDc1Fenced ? 'FENCED' : dc1.role}
                  </span>
                </div>
                <span class="text-xs text-slate-500 dark:text-slate-400 font-mono">Cluster ID: {dc1.id}</span>
              </div>

              <!-- Статус бэйдж -->
              {#if isDc1Fenced}
                <span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-bold bg-rose-600 text-white shadow-xs animate-pulse">
                  <Lock class="w-3.5 h-3.5" /> ПОДАВЛЕН (FENCED)
                </span>
              {:else}
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
              {/if}
            </div>

            <!-- Баннер подавления внутри карточки -->
            {#if isDc1Fenced}
              <div class="mt-3.5 p-2.5 rounded-lg bg-rose-100/90 dark:bg-rose-900/60 border border-rose-300 dark:border-rose-700 flex items-center justify-between gap-2 text-xs text-rose-900 dark:text-rose-100">
                <div class="flex items-center gap-2">
                  <Flame class="w-4 h-4 text-rose-600 shrink-0 animate-pulse" />
                  <div>
                    <div class="font-extrabold text-[11px]">Кластер подавлен (Kill-Switch)</div>
                    <div class="text-[10px] text-rose-700 dark:text-rose-300">Сетевой трафик заблокирован (0 МБ/с). Задачи на паузе.</div>
                  </div>
                </div>
                {#if isAdmin}
                  <button
                    onclick={() => {
                      rollbackClusterId = dc1.id;
                      showRollbackModal = true;
                    }}
                    class="px-2 py-1 rounded-md text-[11px] font-bold bg-rose-600 hover:bg-rose-500 text-white shadow-xs transition cursor-pointer shrink-0"
                  >
                    Снять
                  </button>
                {/if}
              </div>
            {/if}

            <div class="mt-4 pt-3 border-t border-slate-200 dark:border-slate-800/80 grid grid-cols-2 gap-3 text-xs">
              <div>
                <span class="text-slate-400 block text-[11px]">Агенты Full-Duplex</span>
                <span class="font-bold text-slate-800 dark:text-slate-200 font-mono">
                  {dc1.online_agents} / {dc1.total_agents} Online
                </span>
              </div>
              <div>
                <span class="text-slate-400 block text-[11px]">Шейпер пропускной способности</span>
                <span class="font-bold font-mono {isDc1Fenced ? 'text-rose-600 dark:text-rose-400 font-extrabold' : 'text-slate-800 dark:text-slate-200'}">
                  {isDc1Fenced ? '0 МБ/с (ПОДАВЛЕН 🔒)' : `${dc1.bandwidth_limit_mb_s} МБ/с`}
                </span>
              </div>
            </div>

            <!-- Список привязанных HDFS кластеров с бейджами статуса -->
            {#if drStatus.clusters && drStatus.clusters.some(c => c.dc_id === dc1.id || c.id.includes(dc1.id))}
              <div class="mt-3 pt-2.5 border-t border-slate-200/80 dark:border-slate-800/80">
                <span class="text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500 block mb-1.5">
                  Кластеры хранения:
                </span>
                <div class="space-y-1.5">
                  {#each drStatus.clusters.filter(c => c.dc_id === dc1.id || c.id.includes(dc1.id)) as cl}
                    {@const clFenced = cl.is_fenced || isDc1Fenced}
                    <div class="flex items-center justify-between p-2 rounded-lg {clFenced ? 'bg-rose-100/70 dark:bg-rose-900/40 border border-rose-300 dark:border-rose-800' : 'bg-slate-100/70 dark:bg-slate-900/60 border border-slate-200/60 dark:border-slate-800'} text-[11px]">
                      <div class="flex items-center gap-1.5">
                        <Server class="w-3.5 h-3.5 {clFenced ? 'text-rose-600' : 'text-slate-500'}" />
                        <span class="font-bold text-slate-800 dark:text-slate-200">{cl.name}</span>
                        <span class="text-[10px] text-slate-400 font-mono">({cl.id})</span>
                      </div>
                      {#if clFenced}
                        <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-bold bg-rose-600 text-white animate-pulse">
                          <Lock class="w-3 h-3" /> ПОДАВЛЕН
                        </span>
                      {:else}
                        <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-100 dark:bg-emerald-950 text-emerald-700 dark:text-emerald-300">
                          <CheckCircle2 class="w-3 h-3" /> АКТИВЕН
                        </span>
                      {/if}
                    </div>
                  {/each}
                </div>
              </div>
            {/if}

            {#if isAdmin}
              <div class="mt-3 pt-2">
                {#if isDc1Fenced}
                  <button
                    onclick={() => {
                      rollbackClusterId = dc1.id;
                      showRollbackModal = true;
                    }}
                    class="text-xs font-semibold text-emerald-600 dark:text-emerald-400 hover:underline flex items-center gap-1 cursor-pointer font-bold"
                  >
                    <Unlock class="w-3 h-3" /> Снять изоляцию и возобновить {dc1.id}
                  </button>
                {:else if dc1.status !== 'OFFLINE'}
                  <button
                    onclick={() => {
                      emergencyClusterId = dc1.id;
                      showEmergencyModal = true;
                    }}
                    class="text-xs font-semibold text-rose-600 dark:text-rose-400 hover:underline flex items-center gap-1 cursor-pointer"
                  >
                    <PowerOff class="w-3 h-3" /> Экстренно изолировать {dc1.id}
                  </button>
                {/if}
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
            {#if drStatus.summary.kill_switch_active}
              <!-- Аварийное перекрытие канала -->
              <div class="flex flex-col items-center gap-1 text-rose-600 dark:text-rose-400">
                <div class="flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-bold bg-rose-100 dark:bg-rose-950/80 border border-rose-300 dark:border-rose-800">
                  <Lock class="w-3 h-3" />
                  <span>ИЗОЛИРОВАН</span>
                </div>
                <span class="text-[9px] font-mono text-rose-500 font-bold">0 МБ/с Fenced</span>
              </div>
            {:else if drStatus.summary.active_source_dc === 'dc1'}
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
          {@const isDc2Fenced = dc2.is_fenced || (drStatus.summary.kill_switch_active && (drStatus.summary.fenced_cluster_id === dc2.id || drStatus.summary.fenced_cluster_id === 'dc2'))}
          <div class="lg:col-span-3 p-5 rounded-xl border-2 transition relative overflow-hidden {
            isDc2Fenced
              ? 'bg-rose-50/90 dark:bg-rose-950/50 border-rose-600 dark:border-rose-500 ring-2 ring-rose-500/40 shadow-md'
              : dc2.status === 'ONLINE'
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
                    isDc2Fenced
                      ? 'bg-rose-200 dark:bg-rose-900 text-rose-800 dark:text-rose-200 border border-rose-400'
                      : dc2.role === 'PROMOTED_PRIMARY'
                      ? 'bg-indigo-100 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-300 border border-indigo-300 dark:border-indigo-800'
                      : 'bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300'
                  }">
                    {isDc2Fenced ? 'FENCED' : dc2.role}
                  </span>
                </div>
                <span class="text-xs text-slate-500 dark:text-slate-400 font-mono">Cluster ID: {dc2.id}</span>
              </div>

              <!-- Статус бэйдж -->
              {#if isDc2Fenced}
                <span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-bold bg-rose-600 text-white shadow-xs animate-pulse">
                  <Lock class="w-3.5 h-3.5" /> ПОДАВЛЕН (FENCED)
                </span>
              {:else}
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
              {/if}
            </div>

            <!-- Баннер подавления внутри карточки -->
            {#if isDc2Fenced}
              <div class="mt-3.5 p-2.5 rounded-lg bg-rose-100/90 dark:bg-rose-900/60 border border-rose-300 dark:border-rose-700 flex items-center justify-between gap-2 text-xs text-rose-900 dark:text-rose-100">
                <div class="flex items-center gap-2">
                  <Flame class="w-4 h-4 text-rose-600 shrink-0 animate-pulse" />
                  <div>
                    <div class="font-extrabold text-[11px]">Кластер подавлен (Kill-Switch)</div>
                    <div class="text-[10px] text-rose-700 dark:text-rose-300">Сетевой трафик заблокирован (0 МБ/с). Задачи на паузе.</div>
                  </div>
                </div>
                {#if isAdmin}
                  <button
                    onclick={() => {
                      rollbackClusterId = dc2.id;
                      showRollbackModal = true;
                    }}
                    class="px-2 py-1 rounded-md text-[11px] font-bold bg-rose-600 hover:bg-rose-500 text-white shadow-xs transition cursor-pointer shrink-0"
                  >
                    Снять
                  </button>
                {/if}
              </div>
            {/if}

            <div class="mt-4 pt-3 border-t border-slate-200 dark:border-slate-800/80 grid grid-cols-2 gap-3 text-xs">
              <div>
                <span class="text-slate-400 block text-[11px]">Агенты Full-Duplex</span>
                <span class="font-bold text-slate-800 dark:text-slate-200 font-mono">
                  {dc2.online_agents} / {dc2.total_agents} Online
                </span>
              </div>
              <div>
                <span class="text-slate-400 block text-[11px]">Шейпер пропускной способности</span>
                <span class="font-bold font-mono {isDc2Fenced ? 'text-rose-600 dark:text-rose-400 font-extrabold' : 'text-slate-800 dark:text-slate-200'}">
                  {isDc2Fenced ? '0 МБ/с (ПОДАВЛЕН 🔒)' : `${dc2.bandwidth_limit_mb_s} МБ/с`}
                </span>
              </div>
            </div>

            <!-- Список привязанных HDFS кластеров с бейджами статуса -->
            {#if drStatus.clusters && drStatus.clusters.some(c => c.dc_id === dc2.id || c.id.includes(dc2.id))}
              <div class="mt-3 pt-2.5 border-t border-slate-200/80 dark:border-slate-800/80">
                <span class="text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500 block mb-1.5">
                  Кластеры хранения:
                </span>
                <div class="space-y-1.5">
                  {#each drStatus.clusters.filter(c => c.dc_id === dc2.id || c.id.includes(dc2.id)) as cl}
                    {@const clFenced = cl.is_fenced || isDc2Fenced}
                    <div class="flex items-center justify-between p-2 rounded-lg {clFenced ? 'bg-rose-100/70 dark:bg-rose-900/40 border border-rose-300 dark:border-rose-800' : 'bg-slate-100/70 dark:bg-slate-900/60 border border-slate-200/60 dark:border-slate-800'} text-[11px]">
                      <div class="flex items-center gap-1.5">
                        <Server class="w-3.5 h-3.5 {clFenced ? 'text-rose-600' : 'text-slate-500'}" />
                        <span class="font-bold text-slate-800 dark:text-slate-200">{cl.name}</span>
                        <span class="text-[10px] text-slate-400 font-mono">({cl.id})</span>
                      </div>
                      {#if clFenced}
                        <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-bold bg-rose-600 text-white animate-pulse">
                          <Lock class="w-3 h-3" /> ПОДАВЛЕН
                        </span>
                      {:else}
                        <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-100 dark:bg-emerald-950 text-emerald-700 dark:text-emerald-300">
                          <CheckCircle2 class="w-3 h-3" /> АКТИВЕН
                        </span>
                      {/if}
                    </div>
                  {/each}
                </div>
              </div>
            {/if}

            {#if isAdmin}
              <div class="mt-3 pt-2">
                {#if isDc2Fenced}
                  <button
                    onclick={() => {
                      rollbackClusterId = dc2.id;
                      showRollbackModal = true;
                    }}
                    class="text-xs font-semibold text-emerald-600 dark:text-emerald-400 hover:underline flex items-center gap-1 cursor-pointer font-bold"
                  >
                    <Unlock class="w-3 h-3" /> Снять изоляцию и возобновить {dc2.id}
                  </button>
                {:else}
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
                {/if}
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

<!-- Модальное окно отката аварийного останова (Rollback / Unfence Modal) -->
{#if showRollbackModal}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    role="presentation"
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) showRollbackModal = false; }}
  >
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl max-w-lg w-full border border-slate-200 dark:border-slate-800 shadow-2xl p-6 space-y-4 select-auto text-slate-900 dark:text-slate-100"
    >
      <div class="flex items-center justify-between pb-3 border-b border-slate-100 dark:border-slate-800">
        <div class="flex items-center gap-2.5">
          <div class="w-8 h-8 rounded-lg bg-emerald-50 dark:bg-emerald-950/60 border border-emerald-200 dark:border-emerald-800 flex items-center justify-center text-emerald-600 dark:text-emerald-400">
            <Unlock class="w-4 h-4" />
          </div>
          <div>
            <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">
              Снятие изоляции и откат Kill-Switch
            </h3>
            <p class="text-[11px] text-slate-500">Восстановление сетевых каналов и возобновление задач</p>
          </div>
        </div>
        <button
          type="button"
          onclick={() => (showRollbackModal = false)}
          class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 transition cursor-pointer p-1"
        >
          <X class="w-5 h-5" />
        </button>
      </div>

      <div class="space-y-3.5 text-xs">
        <div>
          <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Кластер для снятия изоляции:
          </label>
          <select
            bind:value={rollbackClusterId}
            class="w-full px-3 py-2 rounded-xl border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-950 font-mono text-xs focus:ring-1 focus:ring-emerald-500 font-bold text-slate-800 dark:text-slate-100"
          >
            {#if drStatus?.clusters && drStatus.clusters.length > 0}
              {#each drStatus.clusters as cl}
                <option value={cl.id}>
                  {cl.id} ({cl.name}) {cl.is_fenced || (drStatus?.summary?.kill_switch_active && (drStatus?.summary?.fenced_cluster_id === cl.id || drStatus?.summary?.fenced_cluster_id === cl.dc_id)) ? '— [ПОДАВЛЕН 🔒]' : ''}
                </option>
              {/each}
            {:else if drStatus?.datacenters && drStatus.datacenters.length > 0}
              {#each drStatus.datacenters as dc}
                <option value={dc.id}>
                  {dc.id} ({dc.name}) {dc.is_fenced || (drStatus?.summary?.kill_switch_active && drStatus?.summary?.fenced_cluster_id === dc.id) ? '— [ПОДАВЛЕН 🔒]' : ''}
                </option>
              {/each}
            {:else}
              <option value="dc1">dc1 (HDFS DC1 Production) — [ПОДАВЛЕН 🔒]</option>
              <option value="dc2">dc2 (HDFS DC2 Disaster Recovery)</option>
            {/if}
          </select>
        </div>

        <div class="p-3 bg-slate-50 dark:bg-slate-950/50 rounded-xl border border-slate-200/80 dark:border-slate-800 space-y-2.5">
          <label class="flex items-start gap-2.5 cursor-pointer select-none">
            <input
              type="checkbox"
              bind:checked={rollbackRestoreNetwork}
              class="mt-0.5 rounded text-emerald-600 focus:ring-emerald-500 cursor-pointer"
            />
            <div>
              <div class="font-medium text-slate-800 dark:text-slate-200">
                Снять сетевое ограждение (Unfence Network)
              </div>
              <div class="text-[10px] text-slate-500 leading-relaxed">
                Восстанавливает лимиты шейпера между датацентрами с 0 МБ/с до штатных значений (100 МБ/с).
              </div>
            </div>
          </label>

          <label class="flex items-start gap-2.5 cursor-pointer select-none">
            <input
              type="checkbox"
              bind:checked={rollbackResumeHms}
              class="mt-0.5 rounded text-emerald-600 focus:ring-emerald-500 cursor-pointer"
            />
            <div>
              <div class="font-medium text-slate-800 dark:text-slate-200">
                Возобновить репликацию схем Hive Metastore
              </div>
              <div class="text-[10px] text-slate-500 leading-relaxed">
                Переводит приостановленные (PAUSED) схемы обратно в статус ACTIVE и запускает опрос CDC.
              </div>
            </div>
          </label>

          <label class="flex items-start gap-2.5 cursor-pointer select-none">
            <input
              type="checkbox"
              bind:checked={rollbackResumeHdfs}
              class="mt-0.5 rounded text-emerald-600 focus:ring-emerald-500 cursor-pointer"
            />
            <div>
              <div class="font-medium text-slate-800 dark:text-slate-200">
                Возобновить HDFS задачи и их расписания
              </div>
              <div class="text-[10px] text-slate-500 leading-relaxed">
                Возвращает задачи в очередь QUEUED и реактивирует cron-расписания.
              </div>
            </div>
          </label>
        </div>
      </div>

      <div class="flex items-center justify-end gap-2 pt-3 border-t border-slate-100 dark:border-slate-800">
        <button
          type="button"
          onclick={() => (showRollbackModal = false)}
          disabled={actionLoading}
          class="px-4 py-2 rounded-xl text-xs font-semibold border border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800 transition cursor-pointer"
        >
          Отмена
        </button>
        <button
          type="button"
          onclick={executeRollbackEmergencyStop}
          disabled={actionLoading || (!rollbackRestoreNetwork && !rollbackResumeHms && !rollbackResumeHdfs)}
          class="px-4 py-2 rounded-xl text-xs font-bold bg-emerald-600 hover:bg-emerald-500 text-white shadow-md shadow-emerald-600/20 transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5"
        >
          {#if actionLoading}
            <RefreshCw class="w-3.5 h-3.5 animate-spin" />
            Откат...
          {:else}
            <RotateCcw class="w-3.5 h-3.5" />
            Снять ограничения и возобновить
          {/if}
        </button>
      </div>
    </div>
  </div>
{/if}
