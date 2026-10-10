<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import {
    RefreshCw,
    Plus,
    Trash2,
    ArrowRight,
    Check,
    AlertTriangle,
    Shield,
    Search,
    X,
    Filter,
    RotateCcw,
    Play,
    Pause,
    Zap,
    FileText,
    ChevronDown,
    ChevronUp,
    Database,
    Clock,
    Table2,
    Layers,
    Server,
    HardDrive,
    Info
  } from 'lucide-svelte';
  import { api } from '../api/client';
  import type { HmsReplicationJob, HmsEventLog, Job, TopologyResponse, ClusterInfo, CreateHmsJobPayload } from '../types';
  import { StatusBadge, type UserSession } from '@hadoop-explorer/common';

  // Пропсы
  let { topology = null, user = null }: { topology?: TopologyResponse | null; user?: UserSession | null } = $props();

  const isAdmin = $derived(
    user ? user.system_role === 'admin' || user.is_admin : false
  );

  // Состояние списка задач и топологии
  let internalTopology: TopologyResponse | null = $state(null);
  let jobs: HmsReplicationJob[] = $state([]);
  let loading = $state(false);
  let timer: any = null;

  // Фильтры и поиск
  let searchQuery = $state('');
  let statusFilter = $state('ALL');

  // Развернутая строка таблицы (инлайн аккордеон)
  let expandedJobId: string | null = $state(null);

  // Модальное окно детального просмотра схемы
  let isDetailsModalOpen = $state(false);
  let modalJob: HmsReplicationJob | null = $state(null);
  let detailsTab: 'events' | 'subtasks' = $state('events');
  let detailsLoading = $state(false);
  let events: HmsEventLog[] = $state([]);
  let subtasks: Job[] = $state([]);

  // Модальное окно создания новой репликации
  let isCreateModalOpen = $state(false);
  let showRulesInfo = $state(false);
  let newJob = $state({
    source_cluster_id: 'dc1',
    target_cluster_id: 'dc2',
    source_db: '',
    target_db: '',
    table_pattern: '*',
    drop_extraneous_tables: false,
    drop_extraneous_partitions: false,
    execution_principal: '',
  });

  // Модальное окно подтверждения удаления
  let isDeleteModalOpen = $state(false);
  let jobToDelete: HmsReplicationJob | null = $state(null);
  let isDeleting = $state(false);

  // Модальное окно подтверждения Re-bootstrap
  let isRebootstrapModalOpen = $state(false);
  let jobToRebootstrap: HmsReplicationJob | null = $state(null);
  let isRebootstrapping = $state(false);

  // Оповещения и индикаторы действий
  let rebootstrapMessage: string | null = $state(null);
  let syncingJobId: string | null = $state(null);
  let rebootstrappingJobId: string | null = $state(null);

  // Кластеры из топологии
  const activeTopology = $derived(topology || internalTopology);
  const clusters = $derived<ClusterInfo[]>(
    activeTopology?.clusters && activeTopology.clusters.length > 0
      ? activeTopology.clusters
      : [
          { id: 'dc1', name: 'HDFS DC1 Production', dc_id: 'dc1' } as ClusterInfo,
          { id: 'dc2', name: 'HDFS DC2 Disaster Recovery', dc_id: 'dc2' } as ClusterInfo
        ]
  );

  function getClusterLabel(clusterId: string): string {
    const found = clusters.find((c: ClusterInfo) => c.id === clusterId || c.dc_id === clusterId);
    if (!found) return clusterId;
    return `${found.name} [${found.dc_id.toUpperCase()}]`;
  }

  function getClusterMetaTag(clusterId: string, dcId: string): string {
    const isDc1 = (clusterId || '').toLowerCase().includes('dc1') || (dcId || '').toLowerCase().includes('dc1');
    return isDc1 ? ' — HDP 3.1' : ' — Apache Hive 3.1.3';
  }

  // Фильтрация задач
  const filteredJobs = $derived(
    jobs.filter((j) => {
      if (statusFilter !== 'ALL' && j.status !== statusFilter) {
        return false;
      }
      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase().trim();
        const matchSrcDb = (j.source_db_name || '').toLowerCase().includes(q);
        const matchDstDb = (j.target_db_name || '').toLowerCase().includes(q);
        const matchId = (j.id || '').toLowerCase().includes(q);
        const matchClusters = `${j.source_cluster_id} ${j.target_cluster_id}`.toLowerCase().includes(q);
        if (!matchSrcDb && !matchDstDb && !matchId && !matchClusters) {
          return false;
        }
      }
      return true;
    })
  );

  const hasActiveFilters = $derived(
    statusFilter !== 'ALL' || searchQuery.trim().length > 0
  );

  function resetFilters() {
    statusFilter = 'ALL';
    searchQuery = '';
  }

  function toggleStatusFilter(status: string) {
    statusFilter = statusFilter === status ? 'ALL' : status;
  }

  // Сводная статистика
  const stats = $derived({
    total: jobs.length,
    active: jobs.filter(j => j.status === 'ACTIVE').length,
    bootstrapping: jobs.filter(j => j.status === 'BOOTSTRAPPING').length,
    paused: jobs.filter(j => j.status === 'PAUSED').length,
    tables: jobs.reduce((acc, j) => acc + (j.replicated_tables || 0), 0),
    partitions: jobs.reduce((acc, j) => acc + (j.replicated_partitions || 0), 0),
  });

  // Загрузка данных
  async function loadJobs() {
    try {
      loading = true;
      jobs = await api.getHmsJobs();
      if (modalJob) {
        const updated = jobs.find(j => j.id === modalJob!.id);
        if (updated) modalJob = updated;
      }
    } catch (e) {
      console.error('Ошибка загрузки задач HMS:', e);
    } finally {
      loading = false;
    }
  }

  async function loadDetails(jobId: string) {
    try {
      detailsLoading = true;
      const [evts, subs] = await Promise.all([
        api.getHmsEvents(jobId),
        api.getHmsSubtasks(jobId)
      ]);
      events = evts;
      subtasks = subs;
    } catch (e) {
      console.error('Ошибка загрузки деталей HMS задачи:', e);
    } finally {
      detailsLoading = false;
    }
  }

  function openDetailsModal(job: HmsReplicationJob, initialTab: 'events' | 'subtasks' = 'events') {
    modalJob = job;
    detailsTab = initialTab;
    isDetailsModalOpen = true;
    loadDetails(job.id);
  }

  function toggleExpand(jobId: string) {
    if (expandedJobId === jobId) {
      expandedJobId = null;
    } else {
      expandedJobId = jobId;
      loadDetails(jobId);
    }
  }

  async function handleSyncNow(job: HmsReplicationJob) {
    try {
      syncingJobId = job.id;
      await api.triggerHmsSync(job.id);
      await loadJobs();
      if (modalJob?.id === job.id || expandedJobId === job.id) {
        await loadDetails(job.id);
      }
    } catch (e) {
      console.error('Ошибка триггера CDC синхронизации:', e);
    } finally {
      syncingJobId = null;
    }
  }

  function openRebootstrapModal(job: HmsReplicationJob, e?: MouseEvent) {
    if (e) e.stopPropagation();
    jobToRebootstrap = job;
    isRebootstrapModalOpen = true;
  }

  async function confirmRebootstrap() {
    if (!jobToRebootstrap) return;
    const job = jobToRebootstrap;
    try {
      isRebootstrapping = true;
      rebootstrappingJobId = job.id;
      await api.rebootstrapHmsJob(job.id);
      rebootstrapMessage = `Запущен повторный Bootstrap для схемы «${job.source_db_name}»`;
      setTimeout(() => { rebootstrapMessage = null; }, 4000);
      isRebootstrapModalOpen = false;
      jobToRebootstrap = null;
      await loadJobs();
      if (modalJob?.id === job.id || expandedJobId === job.id) {
        await loadDetails(job.id);
      }
    } catch (e) {
      console.error('Ошибка повторного Bootstrap:', e);
    } finally {
      isRebootstrapping = false;
      rebootstrappingJobId = null;
    }
  }

  async function togglePause(job: HmsReplicationJob) {
    try {
      if (job.status === 'PAUSED') {
        await api.resumeHmsJob(job.id);
      } else {
        await api.pauseHmsJob(job.id);
      }
      await loadJobs();
      if (modalJob?.id === job.id || expandedJobId === job.id) {
        await loadDetails(job.id);
      }
    } catch (e) {
      console.error('Ошибка изменения статуса задачи HMS:', e);
    }
  }

  function openCreateModal() {
    const dc1 = clusters.find((c: ClusterInfo) => c.dc_id === 'dc1' || c.id === 'dc1');
    const dc2 = clusters.find((c: ClusterInfo) => c.dc_id === 'dc2' || c.id === 'dc2');
    newJob = {
      source_cluster_id: dc1 ? dc1.id : (clusters[0]?.id || 'dc1'),
      target_cluster_id: dc2 ? dc2.id : (clusters[1]?.id || 'dc2'),
      source_db: '',
      target_db: '',
      table_pattern: '*',
      drop_extraneous_tables: false,
      drop_extraneous_partitions: false,
      execution_principal: user?.username || '',
    };
    isCreateModalOpen = true;
  }

  async function handleCreateJob() {
    if (!newJob.source_db.trim()) return;
    try {
      loading = true;
      const rawUser = (isAdmin && newJob.execution_principal?.trim())
        ? newJob.execution_principal.trim()
        : (user?.username || 'system_operator');
      const principal = rawUser.includes('@') ? rawUser : `${rawUser}@REALM.LOCAL`;
      const payload: CreateHmsJobPayload = {
        source_cluster_id: newJob.source_cluster_id,
        target_cluster_id: newJob.target_cluster_id,
        source_db: newJob.source_db.trim(),
        target_db: newJob.target_db?.trim() || undefined,
        table_pattern: newJob.table_pattern?.trim() || '*',
        drop_extraneous_tables: newJob.drop_extraneous_tables,
        drop_extraneous_partitions: newJob.drop_extraneous_partitions,
        execution_principal: principal,
      };
      const created = await api.createHmsJob(payload);
      isCreateModalOpen = false;
      await loadJobs();
      openDetailsModal(created, 'events');
    } catch (e) {
      console.error('Ошибка создания задачи HMS:', e);
    } finally {
      loading = false;
    }
  }

  function openDeleteModal(job: HmsReplicationJob) {
    jobToDelete = job;
    isDeleteModalOpen = true;
  }

  async function confirmDeleteJob() {
    if (!jobToDelete) return;
    try {
      isDeleting = true;
      await api.deleteHmsJob(jobToDelete.id);
      isDeleteModalOpen = false;
      if (modalJob?.id === jobToDelete.id) {
        isDetailsModalOpen = false;
        modalJob = null;
      }
      if (expandedJobId === jobToDelete.id) {
        expandedJobId = null;
      }
      jobToDelete = null;
      await loadJobs();
    } catch (e) {
      console.error('Ошибка удаления задачи HMS:', e);
    } finally {
      isDeleting = false;
    }
  }

  function formatTime(isoString?: string): string {
    if (!isoString) return '—';
    try {
      const d = new Date(isoString);
      if (isNaN(d.getTime())) return '—';
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch {
      return '—';
    }
  }

  function formatBytes(bytes: number): string {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  onMount(async () => {
    loadJobs();
    if (!topology) {
      try {
        internalTopology = await api.getTopology();
      } catch (e) {
        console.error('Ошибка загрузки топологии для HMS:', e);
      }
    }
    timer = setInterval(() => {
      loadJobs();
      if (isDetailsModalOpen && modalJob) {
        loadDetails(modalJob.id);
      } else if (expandedJobId) {
        loadDetails(expandedJobId);
      }
    }, 4000);
  });

  onDestroy(() => {
    if (timer) clearInterval(timer);
  });
</script>

<div class="space-y-6">
  <!-- ВЕРХНЯЯ ПАНЕЛЬ: КАРТОЧКИ СТАТИСТИКИ И КНОПКА СОЗДАНИЯ (ЕДИНЫЙ СТИЛЬ HDFS) -->
  <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
    <!-- 6 Карточек статистики с интерактивным фильтром статуса -->
    <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 flex-1">
      <button
        type="button"
        onclick={() => (statusFilter = 'ALL')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {statusFilter === 'ALL'
          ? 'bg-slate-100 dark:bg-slate-800 border-slate-400 dark:border-slate-500 ring-2 ring-slate-400/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300 dark:hover:border-slate-700'}"
      >
        <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Всего схем</span>
        <span class="text-xl font-bold text-slate-900 dark:text-slate-100">{stats.total}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('ACTIVE')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {statusFilter === 'ACTIVE'
          ? 'bg-sky-50 dark:bg-sky-950/40 border-sky-400 dark:border-sky-600 ring-2 ring-sky-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-sky-300 dark:hover:border-sky-800'}"
      >
        <span class="text-[11px] font-medium text-sky-600 dark:text-sky-400 block">Active CDC</span>
        <span class="text-xl font-bold text-sky-600 dark:text-sky-400">{stats.active}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('BOOTSTRAPPING')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {statusFilter === 'BOOTSTRAPPING'
          ? 'bg-amber-50 dark:bg-amber-950/40 border-amber-400 dark:border-amber-600 ring-2 ring-amber-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-amber-300 dark:hover:border-amber-800'}"
      >
        <span class="text-[11px] font-medium text-amber-600 dark:text-amber-400 block">Bootstrap Sync</span>
        <span class="text-xl font-bold text-amber-600 dark:text-amber-400">{stats.bootstrapping}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('PAUSED')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {statusFilter === 'PAUSED'
          ? 'bg-slate-100 dark:bg-slate-800 border-slate-400 dark:border-slate-500 ring-2 ring-slate-400/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300 dark:hover:border-slate-700'}"
      >
        <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">На паузе</span>
        <span class="text-xl font-bold text-slate-600 dark:text-slate-400">{stats.paused}</span>
      </button>

      <div class="p-3 rounded-xl border bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 shadow-2xs">
        <span class="text-[11px] font-medium text-emerald-600 dark:text-emerald-400 block">Таблиц</span>
        <span class="text-xl font-bold text-emerald-600 dark:text-emerald-400">{stats.tables}</span>
      </div>

      <div class="p-3 rounded-xl border bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 shadow-2xs">
        <span class="text-[11px] font-medium text-sky-600 dark:text-sky-400 block">Партиций</span>
        <span class="text-xl font-bold text-slate-800 dark:text-slate-200">{stats.partitions}</span>
      </div>
    </div>

    <!-- Кнопка Новая задача и Обновить (справа в одну строчку) -->
    <div class="flex items-center gap-2 self-end sm:self-auto shrink-0">
      <button
        onclick={loadJobs}
        class="p-2.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-300 transition cursor-pointer shadow-2xs"
        title="Обновить список схем"
      >
        <RefreshCw class="w-4 h-4 {loading ? 'animate-spin' : ''}" />
      </button>

      <button
        onclick={openCreateModal}
        class="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-sky-600 hover:bg-sky-500 text-white font-semibold text-xs transition cursor-pointer shadow-md shadow-sky-600/20"
      >
        <Plus class="w-4 h-4" />
        <span>Новая репликация схемы</span>
      </button>
    </div>
  </div>

  <!-- ПАНЕЛЬ ПОИСКА И ФИЛЬТРОВ (ЕДИНЫЙ СТИЛЬ HDFS) -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-3.5 shadow-2xs">
    <div class="flex flex-col md:flex-row items-stretch md:items-center justify-between gap-3">
      <div class="flex flex-wrap items-center gap-3 flex-1">
        <!-- Поиск -->
        <div class="relative min-w-[220px] flex-1 max-w-sm">
          <Search class="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
          <input
            type="text"
            bind:value={searchQuery}
            placeholder="Поиск по базе, кластерам, ID..."
            class="w-full pl-9 pr-8 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 text-xs text-slate-800 dark:text-slate-200 placeholder-slate-400 focus:ring-2 focus:ring-sky-500 focus:outline-none transition"
          />
          {#if searchQuery}
            <button
              type="button"
              onclick={() => (searchQuery = '')}
              class="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 cursor-pointer p-0.5"
              title="Очистить поиск"
            >
              <X class="w-3.5 h-3.5" />
            </button>
          {/if}
        </div>

        <!-- Фильтр по статусу -->
        <div class="flex items-center gap-2">
          <label for="filter-hms-status" class="text-xs text-slate-500 dark:text-slate-400 font-medium flex items-center gap-1.5 whitespace-nowrap">
            <Filter class="w-3.5 h-3.5 text-slate-400" />
            <span>Статус:</span>
          </label>
          <select
            id="filter-hms-status"
            bind:value={statusFilter}
            class="px-3 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 text-xs text-slate-700 dark:text-slate-300 focus:ring-2 focus:ring-sky-500 focus:outline-none cursor-pointer"
          >
            <option value="ALL">Все статусы ({jobs.length})</option>
            <option value="ACTIVE">Active CDC ({stats.active})</option>
            <option value="BOOTSTRAPPING">Bootstrap Sync ({stats.bootstrapping})</option>
            <option value="PAUSED">На паузе ({stats.paused})</option>
          </select>
        </div>
      </div>

      <!-- Правая часть: Счетчик и Сброс -->
      <div class="flex items-center justify-between md:justify-end gap-3 shrink-0 pt-2 md:pt-0 border-t md:border-t-0 border-slate-100 dark:border-slate-800/80">
        <span class="text-xs text-slate-500 dark:text-slate-400 font-medium whitespace-nowrap">
          Показано: <strong class="text-slate-800 dark:text-slate-200 font-semibold">{filteredJobs.length}</strong> из {jobs.length}
        </span>

        {#if hasActiveFilters}
          <button
            type="button"
            onclick={resetFilters}
            class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 transition cursor-pointer"
            title="Сбросить активные фильтры"
          >
            <RotateCcw class="w-3.5 h-3.5 text-slate-400" />
            <span>Сбросить</span>
          </button>
        {/if}
      </div>
    </div>
  </div>

  {#if rebootstrapMessage}
    <div class="p-3 bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800 rounded-xl flex items-center gap-2.5 text-xs text-emerald-800 dark:text-emerald-200 font-medium shadow-2xs">
      <Check class="w-4 h-4 text-emerald-600 shrink-0" />
      <span>{rebootstrapMessage}</span>
    </div>
  {/if}

  <!-- ОСНОВНАЯ ПОЛНОРАЗМЕРНАЯ ТАБЛИЦА СХЕМ HMS (ЕДИНЫЙ СТИЛЬ HDFS REPLICATION) -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-xs overflow-hidden">
    <div class="overflow-x-auto">
      <table class="w-full table-fixed text-left border-collapse text-xs min-w-[960px]">
        <thead>
          <tr class="bg-slate-50/80 dark:bg-slate-950/60 border-b border-slate-200 dark:border-slate-800 text-slate-500 dark:text-slate-400 font-semibold uppercase tracking-wider text-[10px]">
            <th scope="col" class="py-3 px-4 w-44">Схема и ID</th>
            <th scope="col" class="py-3 px-4 w-36">Маршрут</th>
            <th scope="col" class="py-3 px-4 w-40">Объекты</th>
            <th scope="col" class="py-3 px-4 w-40">CDC Чекпоинт</th>
            <th scope="col" class="py-3 px-4 w-48">Статус</th>
            <th scope="col" class="py-3 px-4 w-44 text-right">Действия</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60">
          {#if jobs.length === 0}
            <tr>
              <td colspan="6" class="py-12 text-center text-slate-400 dark:text-slate-500">
                <div class="flex flex-col items-center gap-2">
                  <Database class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                  <span>Нет активных или сконфигурированных схем репликации Hive Metastore</span>
                  <button
                    onclick={openCreateModal}
                    class="mt-2 text-xs text-sky-600 dark:text-sky-400 hover:underline font-semibold cursor-pointer"
                  >
                    Настроить первую схему
                  </button>
                </div>
              </td>
            </tr>
          {:else if filteredJobs.length === 0}
            <tr>
              <td colspan="6" class="py-12 text-center text-slate-400 dark:text-slate-500">
                <div class="flex flex-col items-center gap-2">
                  <Filter class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                  <span class="font-medium text-slate-700 dark:text-slate-300">Схемы по заданным фильтрам не найдены</span>
                  <span class="text-[11px] text-slate-400">Попробуйте изменить статус или строку поиска</span>
                  <button
                    type="button"
                    onclick={resetFilters}
                    class="mt-2 flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-xs font-medium text-slate-700 dark:text-slate-200 transition cursor-pointer"
                  >
                    <RotateCcw class="w-3.5 h-3.5 text-slate-400" />
                    <span>Сбросить фильтры</span>
                  </button>
                </div>
              </td>
            </tr>
          {:else}
            {#each filteredJobs as job (job.id)}
              <tr class="hover:bg-slate-50/60 dark:hover:bg-slate-850/50 transition-colors">
                <!-- Схема и ID -->
                <td class="py-3.5 px-4 font-mono">
                  <div class="flex items-center gap-1.5 font-sans font-semibold text-slate-900 dark:text-slate-100 text-xs truncate">
                    <span class="truncate" title={job.source_db_name}>{job.source_db_name}</span>
                    <span class="text-slate-400">→</span>
                    <span class="text-sky-600 dark:text-sky-400 truncate" title={job.target_db_name}>{job.target_db_name}</span>
                  </div>
                  <div class="text-[10px] text-slate-400 dark:text-slate-500 font-mono truncate mt-0.5" title={job.id}>
                    {job.id}
                  </div>
                  <div class="text-[10px] font-mono truncate mt-0.5" title={job.execution_principal || job.created_by || ''}>
                    <span class="text-sky-600 dark:text-sky-400 inline-flex items-center gap-1">
                      <span>👤 {job.execution_principal || job.created_by || 'system'}</span>
                      <span class="text-[9px] px-1 py-0.2 rounded bg-sky-100 dark:bg-sky-950/60 font-semibold text-sky-700 dark:text-sky-300">doAs</span>
                    </span>
                  </div>
                  {#if job.table_pattern && job.table_pattern !== '*'}
                    <div class="text-[9px] text-slate-400 font-mono mt-0.5">
                      фильтр: {job.table_pattern}
                    </div>
                  {/if}
                  {#if job.drop_extraneous_tables || job.drop_extraneous_partitions}
                    <div class="flex items-center gap-1 mt-1 flex-wrap">
                      {#if job.drop_extraneous_tables}
                        <span class="px-1.5 py-0.2 rounded text-[9px] font-semibold bg-indigo-50 text-indigo-700 dark:bg-indigo-950/50 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800" title="Включена сверка и очистка лишних таблиц на приемнике">
                          Diff Таблицы
                        </span>
                      {/if}
                      {#if job.drop_extraneous_partitions}
                        <span class="px-1.5 py-0.2 rounded text-[9px] font-semibold bg-purple-50 text-purple-700 dark:bg-purple-950/50 dark:text-purple-300 border border-purple-200 dark:border-purple-800" title="Включена сверка и очистка лишних партиций на приемнике">
                          Diff Партиции
                        </span>
                      {/if}
                    </div>
                  {/if}
                </td>

                <!-- Маршрут -->
                <td class="py-3.5 px-4">
                  <div class="flex items-center gap-1.5 font-medium">
                    <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-mono text-[11px]" title={job.source_cluster_id}>
                      {job.source_cluster_id}
                    </span>
                    <span class="text-slate-400">→</span>
                    <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-mono text-[11px]" title={job.target_cluster_id}>
                      {job.target_cluster_id}
                    </span>
                  </div>
                  <div class="text-[10px] text-slate-400 dark:text-slate-500 mt-1 truncate" title="{getClusterLabel(job.source_cluster_id)} → {getClusterLabel(job.target_cluster_id)}">
                    {getClusterMetaTag(job.source_cluster_id, job.source_cluster_id).replace(' — ', '')} → {getClusterMetaTag(job.target_cluster_id, job.target_cluster_id).replace(' — ', '')}
                  </div>
                </td>

                <!-- Объекты (Таблицы и Партиции) -->
                <td class="py-3.5 px-4">
                  <div class="flex items-center gap-1 text-[11px] text-slate-800 dark:text-slate-200 font-medium">
                    <Table2 class="w-3.5 h-3.5 text-emerald-500 shrink-0" />
                    <span>Таблиц:</span>
                    <strong class="font-bold text-slate-900 dark:text-slate-100">{job.replicated_tables || 0}</strong>
                    <span class="text-slate-400">/ {job.total_tables || 0}</span>
                  </div>
                  <div class="flex items-center gap-1 text-[10px] text-slate-500 dark:text-slate-400 mt-1">
                    <Layers class="w-3 h-3 text-sky-500 shrink-0" />
                    <span>Партиций:</span>
                    <span class="font-mono font-medium text-slate-700 dark:text-slate-300">{job.replicated_partitions || 0}</span>
                  </div>
                </td>

                <!-- CDC Чекпоинт и Lag -->
                <td class="py-3.5 px-4">
                  <div class="flex items-center gap-1.5 font-mono text-[11px]">
                    <span class="text-slate-400 font-semibold">#</span>
                    <span class="font-bold text-slate-800 dark:text-slate-200">{job.last_processed_event_id ?? 0}</span>
                    {#if job.event_lag && job.event_lag > 0}
                      <span class="px-1.5 py-0.2 rounded text-[9px] font-semibold bg-amber-50 text-amber-700 dark:bg-amber-950/50 dark:text-amber-300 border border-amber-200 dark:border-amber-800">
                        lag: {job.event_lag}
                      </span>
                    {:else}
                      <span class="px-1.5 py-0.2 rounded text-[9px] font-semibold bg-emerald-50 text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800">
                        lag: 0
                      </span>
                    {/if}
                  </div>
                  <div class="text-[10px] text-slate-400 mt-1 flex items-center gap-1">
                    <Clock class="w-2.5 h-2.5 text-slate-400" />
                    <span>{job.status === 'BOOTSTRAPPING' ? 'Bootstrap Sync' : 'Streaming CDC'}</span>
                  </div>
                </td>

                <!-- Статус -->
                <td class="py-3.5 px-4">
                  <StatusBadge
                    status={job.status === 'ACTIVE' ? 'running' : job.status === 'BOOTSTRAPPING' ? 'warning' : 'cancelled'}
                    text={job.status}
                  />
                  {#if job.message}
                    <div class="text-[10px] text-slate-500 truncate max-w-[210px] mt-0.5" title={job.message}>
                      {job.message}
                    </div>
                  {/if}
                </td>

                <!-- Действия -->
                <td class="py-3.5 px-4 text-right">
                  <div class="flex items-center justify-end gap-1">
                    <!-- Пауза / Возобновить -->
                    <button
                      onclick={() => togglePause(job)}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer {job.status === 'PAUSED'
                        ? 'hover:bg-emerald-50 dark:hover:bg-emerald-950/40 hover:border-emerald-300 dark:hover:border-emerald-700 text-emerald-600 dark:text-emerald-400'
                        : 'hover:bg-amber-50 dark:hover:bg-amber-950/40 hover:border-amber-300 dark:hover:border-amber-700 text-amber-600 dark:text-amber-400'}"
                      title={job.status === 'PAUSED' ? 'Возобновить репликацию' : 'Приостановить CDC'}
                    >
                      {#if job.status === 'PAUSED'}
                        <Play class="w-3.5 h-3.5 fill-current" />
                      {:else}
                        <Pause class="w-3.5 h-3.5 fill-current" />
                      {/if}
                    </button>

                    <!-- Немедленный опрос CDC -->
                    <button
                      onclick={() => handleSyncNow(job)}
                      disabled={job.status === 'PAUSED' || syncingJobId === job.id}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-sky-50 dark:hover:bg-sky-950/40 hover:border-sky-300 dark:hover:border-sky-700 text-sky-600 dark:text-sky-400"
                      title="Немедленный опрос NOTIFICATION_LOG"
                    >
                      <Zap class="w-3.5 h-3.5 {syncingJobId === job.id ? 'animate-bounce' : ''}" />
                    </button>

                    <!-- Re-bootstrap -->
                    <button
                      onclick={() => openRebootstrapModal(job)}
                      disabled={rebootstrappingJobId === job.id}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-sky-50 dark:hover:bg-sky-950/40 hover:border-sky-300 dark:hover:border-sky-700 text-slate-500 hover:text-sky-600 dark:hover:text-sky-400"
                      title="Повторный Bootstrap схемы с подтверждением (Re-bootstrap)"
                    >
                      <RefreshCw class="w-3.5 h-3.5 {rebootstrappingJobId === job.id ? 'animate-spin' : ''}" />
                    </button>

                    <!-- Детали и журнал событий в модальном окне -->
                    <button
                      onclick={() => openDetailsModal(job, 'events')}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer hover:bg-sky-50 dark:hover:bg-sky-950/40 hover:border-sky-300 dark:hover:border-sky-700 text-sky-600 dark:text-sky-400"
                      title="Журнал DDL событий и вложенные задачи HDFS"
                    >
                      <FileText class="w-3.5 h-3.5" />
                    </button>

                    <!-- Развернуть инлайн в таблице -->
                    <button
                      onclick={() => toggleExpand(job.id)}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
                      title={expandedJobId === job.id ? 'Свернуть' : 'Быстрый просмотр'}
                    >
                      {#if expandedJobId === job.id}
                        <ChevronUp class="w-3.5 h-3.5" />
                      {:else}
                        <ChevronDown class="w-3.5 h-3.5" />
                      {/if}
                    </button>

                    <!-- Удалить -->
                    <button
                      onclick={() => openDeleteModal(job)}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer hover:bg-rose-50 dark:hover:bg-rose-950/40 hover:border-rose-400 dark:hover:border-rose-800 text-slate-400 hover:text-rose-600 dark:hover:text-rose-400"
                      title="Удалить задачу синхронизации"
                    >
                      <Trash2 class="w-3.5 h-3.5" />
                    </button>
                  </div>
                </td>
              </tr>

              <!-- РАСКРЫВАЮЩИЙСЯ ИНЛАЙН-БЛОК ПОД СТРОКОЙ ТАБЛИЦЫ -->
              <!-- РАСКРЫВАЮЩИЙСЯ ИНЛАЙН-БЛОК ПОД СТРОКОЙ ТАБЛИЦЫ (ДВА СТОЛБИКА: HMS И HDFS) -->
              {#if expandedJobId === job.id}
                <tr class="bg-slate-50/70 dark:bg-slate-950/50">
                  <td colspan="6" class="p-4 border-t border-slate-100 dark:border-slate-800/80">
                    <div class="space-y-3">
                      <div class="flex items-center justify-between">
                        <div class="flex items-center gap-2 text-xs font-semibold text-slate-800 dark:text-slate-200">
                          <Clock class="w-3.5 h-3.5 text-sky-600" />
                          <span>Детали репликации схемы и передачи данных</span>
                          {#if detailsLoading}
                            <RefreshCw class="w-3 h-3 text-slate-400 animate-spin" />
                          {/if}
                        </div>
                        <div class="flex items-center gap-3">
                          <button
                            onclick={() => openDetailsModal(job, 'events')}
                            class="text-xs text-sky-600 dark:text-sky-400 hover:underline font-medium cursor-pointer flex items-center gap-1"
                          >
                            <span>Открыть полный журнал в модальном окне</span>
                            <ArrowRight class="w-3 h-3" />
                          </button>
                        </div>
                      </div>

                      <!-- ДВА СТОЛБИКА: СЛЕВА HMS СОБЫТИЯ, СПРАВА HDFS ПОДЗАДАЧИ -->
                      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
                        <!-- ЛЕВЫЙ СТОЛБИК: HMS СОБЫТИЯ МЕТАДАННЫХ -->
                        <div class="space-y-2 bg-slate-100/60 dark:bg-slate-900/60 p-3 rounded-2xl border border-slate-200/70 dark:border-slate-800/70">
                          <div class="flex items-center justify-between text-xs font-semibold text-slate-700 dark:text-slate-300 pb-1.5 border-b border-slate-200/50 dark:border-slate-800/50">
                            <div class="flex items-center gap-1.5">
                              <Database class="w-3.5 h-3.5 text-sky-600 dark:text-sky-400" />
                              <span>HMS События метаданных</span>
                              <span class="px-1.5 py-0.2 rounded-full text-[10px] bg-sky-100 dark:bg-sky-950 text-sky-700 dark:text-sky-300 font-mono font-bold">
                                {events.length}
                              </span>
                            </div>
                            <button
                              onclick={() => openDetailsModal(job, 'events')}
                              class="text-[11px] text-sky-600 dark:text-sky-400 hover:underline flex items-center gap-0.5 cursor-pointer"
                            >
                              <span>Все события ({events.length})</span>
                              <ArrowRight class="w-3 h-3" />
                            </button>
                          </div>

                          {#if events.length === 0}
                            <div class="text-xs text-slate-400 py-4 text-center">
                              Событий CDC пока не зарегистрировано.
                            </div>
                          {:else}
                            <div class="space-y-1.5 max-h-64 overflow-y-auto pr-1">
                              {#each events.slice(0, 6) as ev}
                                <div class="p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200/80 dark:border-slate-800 flex items-center justify-between text-xs hover:border-slate-300 dark:hover:border-slate-700 transition-colors shadow-2xs">
                                  <div class="flex items-center gap-2 min-w-0">
                                    <span class="px-1.5 py-0.5 rounded text-[9px] font-mono font-semibold shrink-0 {ev.event_type.startsWith('BOOTSTRAP') ? 'bg-purple-50 text-purple-700 dark:bg-purple-950/50 dark:text-purple-300 border border-purple-200 dark:border-purple-800' : 'bg-sky-50 text-sky-700 dark:bg-sky-950/50 dark:text-sky-300 border border-sky-200 dark:border-sky-800'}">
                                      {ev.event_type}
                                    </span>
                                    <span class="font-mono text-[11px] text-slate-800 dark:text-slate-200 truncate" title="{ev.table_name}{ev.partition_name ? ` (${ev.partition_name})` : ''}">
                                      {ev.table_name}{#if ev.partition_name}<span class="text-slate-400 font-normal"> ({ev.partition_name})</span>{/if}
                                    </span>
                                  </div>
                                  <div class="flex items-center gap-2 shrink-0">
                                    <span class="text-[10px] text-emerald-600 font-semibold uppercase">● {ev.status}</span>
                                    <span class="text-[10px] text-slate-400 font-mono">{formatTime(ev.created_at)}</span>
                                  </div>
                                </div>
                              {/each}
                            </div>
                          {/if}
                        </div>

                        <!-- ПРАВЫЙ СТОЛБИК: HDFS ПОДЗАДАЧИ ПЕРЕДАЧИ ФАЙЛОВ -->
                        <div class="space-y-2 bg-slate-100/60 dark:bg-slate-900/60 p-3 rounded-2xl border border-slate-200/70 dark:border-slate-800/70">
                          <div class="flex items-center justify-between text-xs font-semibold text-slate-700 dark:text-slate-300 pb-1.5 border-b border-slate-200/50 dark:border-slate-800/50">
                            <div class="flex items-center gap-1.5">
                              <HardDrive class="w-3.5 h-3.5 text-indigo-600 dark:text-indigo-400" />
                              <span>HDFS Подзадачи передачи файлов</span>
                              <span class="px-1.5 py-0.2 rounded-full text-[10px] bg-indigo-100 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-300 font-mono font-bold">
                                {subtasks.length}
                              </span>
                            </div>
                            <button
                              onclick={() => openDetailsModal(job, 'subtasks')}
                              class="text-[11px] text-indigo-600 dark:text-indigo-400 hover:underline flex items-center gap-0.5 cursor-pointer"
                            >
                              <span>Все подзадачи ({subtasks.length})</span>
                              <ArrowRight class="w-3 h-3" />
                            </button>
                          </div>

                          {#if subtasks.length === 0}
                            <div class="text-xs text-slate-400 py-4 text-center">
                              Подзадач передачи данных HDFS пока нет.
                            </div>
                          {:else}
                            <div class="space-y-1.5 max-h-64 overflow-y-auto pr-1">
                              {#each subtasks.slice(0, 6) as sub}
                                <div class="p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-200/80 dark:border-slate-800 flex items-center justify-between text-xs hover:border-slate-300 dark:hover:border-slate-700 transition-colors shadow-2xs">
                                  <div class="flex items-center gap-2 min-w-0">
                                    <span class="px-1.5 py-0.5 rounded text-[9px] font-mono font-semibold bg-indigo-50 dark:bg-indigo-950/50 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800 shrink-0">
                                      {sub.id.substring(0, 8)}
                                    </span>
                                    <div class="truncate text-[11px]">
                                      <span class="font-mono text-slate-700 dark:text-slate-300 truncate" title="{sub.source_path} → {sub.target_path}">
                                        {sub.source_path.split('/').pop() || sub.source_path}
                                      </span>
                                      <span class="text-[10px] text-slate-400 ml-1">({formatBytes(sub.copied_bytes || sub.total_bytes)})</span>
                                    </div>
                                  </div>
                                  <div class="flex items-center gap-2 shrink-0">
                                    <span class="text-[10px] {sub.status === 'COMPLETED' ? 'text-emerald-600' : sub.status === 'RUNNING' ? 'text-sky-600' : 'text-slate-500'} font-semibold uppercase">● {sub.status}</span>
                                    <span class="text-[10px] text-slate-400 font-mono">{formatTime(sub.started_at || sub.created_at)}</span>
                                  </div>
                                </div>
                              {/each}
                            </div>
                          {/if}
                        </div>
                      </div>
                    </div>
                  </td>
                </tr>
              {/if}
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </div>
</div>

<!-- МОДАЛЬНОЕ ОКНО ДЕТАЛЬНОГО ПРОСМОТРА СХЕМЫ (В СТИЛЕ HISTORY MODAL HDFS) -->
{#if isDetailsModalOpen && modalJob}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    role="presentation"
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) isDetailsModalOpen = false; }}
  >
    <div
      class="w-full max-w-5xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[92vh] overflow-hidden"
    >
      <!-- Шапка окна -->
      <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-sky-50 dark:bg-sky-950/60 border border-sky-200 dark:border-sky-800 flex items-center justify-center text-sky-600 dark:text-sky-400 shrink-0">
            <Database class="w-5 h-5" />
          </div>
          <div>
            <div class="flex items-center gap-2">
              <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">
                {modalJob.source_db_name} → {modalJob.target_db_name}
              </h2>
              <StatusBadge
                status={modalJob.status === 'ACTIVE' ? 'running' : modalJob.status === 'BOOTSTRAPPING' ? 'warning' : 'cancelled'}
                text={modalJob.status}
              />
            </div>
            <p class="text-xs text-slate-500 dark:text-slate-400 font-mono flex items-center gap-2 mt-0.5">
              <span>ID: {modalJob.id}</span>
              <span>•</span>
              <span class="text-slate-700 dark:text-slate-300 font-medium">
                {modalJob.source_cluster_id} → {modalJob.target_cluster_id}
              </span>
              <span>•</span>
              <span class="text-slate-400">
                {getClusterLabel(modalJob.source_cluster_id)} → {getClusterLabel(modalJob.target_cluster_id)}
              </span>
              <span>•</span>
              <span class="inline-flex items-center gap-1 text-sky-600 dark:text-sky-400 font-mono">
                <span>👤 {modalJob.execution_principal || modalJob.created_by || 'system'}</span>
                <span class="text-[9px] px-1 py-0.2 rounded bg-sky-100 dark:bg-sky-950/60 font-semibold text-sky-700 dark:text-sky-300">doAs</span>
              </span>
            </p>
          </div>
        </div>

        <div class="flex items-center gap-2">
          <button
            onclick={() => loadDetails(modalJob!.id)}
            class="p-2 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-300 transition cursor-pointer shadow-2xs"
            title="Обновить детали"
          >
            <RefreshCw class="w-4 h-4 {detailsLoading ? 'animate-spin' : ''}" />
          </button>
          <button
            onclick={() => (isDetailsModalOpen = false)}
            class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded cursor-pointer"
          >
            <X class="w-5 h-5" />
          </button>
        </div>
      </div>

      <!-- Плашка статуса чекпоинта и объяснения CDC -->
      <div class="p-3.5 bg-sky-50/70 dark:bg-sky-950/40 border border-sky-200/80 dark:border-sky-800/80 rounded-xl flex flex-col sm:flex-row sm:items-center justify-between gap-3 text-xs">
        <div class="space-y-0.5">
          <div class="font-semibold text-sky-900 dark:text-sky-200 flex items-center gap-2">
            <Shield class="w-4 h-4 text-sky-600" />
            <span>Текущий чекпоинт CDC:</span>
            <strong class="font-mono text-sky-700 dark:text-sky-300">#{modalJob.last_processed_event_id ?? 0}</strong>
            <span class="text-slate-400">•</span>
            <span>Event Lag:</span>
            <span class="font-bold {modalJob.event_lag && modalJob.event_lag > 0 ? 'text-amber-600' : 'text-emerald-600'}">
              {modalJob.event_lag ?? 0}
            </span>
          </div>
          <p class="text-[11px] text-slate-500 dark:text-slate-400">
            При остановке задачи чекпоинт сохраняется. После возобновления все накопившиеся в NOTIFICATION_LOG события последовательно накатываются без потерь.
          </p>
        </div>

        <div class="flex items-center gap-2 shrink-0">
          <button
            onclick={() => handleSyncNow(modalJob!)}
            disabled={modalJob.status === 'PAUSED' || syncingJobId === modalJob.id}
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold transition cursor-pointer shadow-2xs disabled:opacity-50"
          >
            <Zap class="w-3.5 h-3.5 {syncingJobId === modalJob.id ? 'animate-bounce' : ''}" />
            <span>{syncingJobId === modalJob.id ? 'Опрос...' : 'Опросить CDC'}</span>
          </button>

          <button
            onclick={() => openRebootstrapModal(modalJob!)}
            disabled={rebootstrappingJobId === modalJob.id}
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 text-xs font-semibold transition cursor-pointer shadow-2xs disabled:opacity-50"
            title="Запустить повторный Bootstrap схемы с подтверждением"
          >
            <RefreshCw class="w-3.5 h-3.5 {rebootstrappingJobId === modalJob.id ? 'animate-spin' : ''}" />
            <span>Re-bootstrap</span>
          </button>
        </div>
      </div>

      <!-- Вкладки: Журнал DDL и Вложенные подзадачи HDFS -->
      <div class="flex items-center gap-2 border-b border-slate-200 dark:border-slate-800">
        <button
          onclick={() => (detailsTab = 'events')}
          class="flex items-center gap-2 px-4 py-2 border-b-2 text-xs font-semibold transition cursor-pointer {detailsTab === 'events'
            ? 'border-sky-600 text-sky-600 dark:text-sky-400'
            : 'border-transparent text-slate-500 hover:text-slate-800 dark:hover:text-slate-200'}"
        >
          <Clock class="w-4 h-4" />
          <span>Журнал событий DDL</span>
          <span class="px-1.5 py-0.2 rounded-full text-[10px] font-mono bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300">
            {events.length}
          </span>
        </button>

        <button
          onclick={() => (detailsTab = 'subtasks')}
          class="flex items-center gap-2 px-4 py-2 border-b-2 text-xs font-semibold transition cursor-pointer {detailsTab === 'subtasks'
            ? 'border-sky-600 text-sky-600 dark:text-sky-400'
            : 'border-transparent text-slate-500 hover:text-slate-800 dark:hover:text-slate-200'}"
        >
          <HardDrive class="w-4 h-4" />
          <span>Вложенные задачи переноса HDFS</span>
          <span class="px-1.5 py-0.2 rounded-full text-[10px] font-mono bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300">
            {subtasks.length}
          </span>
        </button>
      </div>

      <!-- Содержимое активной вкладки -->
      <div class="flex-1 overflow-y-auto max-h-[50vh] rounded-xl border border-slate-200 dark:border-slate-800">
        {#if detailsTab === 'events'}
          {#if events.length === 0}
            <div class="py-12 text-center text-slate-400 text-xs">
              Событий DDL репликации пока нет. Они появятся при первичном Bootstrap и потоковом изменении таблиц.
            </div>
          {:else}
            <table class="w-full table-fixed text-left border-collapse text-xs">
              <thead class="bg-slate-50 dark:bg-slate-950/80 border-b border-slate-200 dark:border-slate-800 text-slate-500 font-semibold text-[10px] uppercase tracking-wider sticky top-0">
                <tr>
                  <th class="py-2.5 px-3 w-28">Событие</th>
                  <th class="py-2.5 px-3 w-40">Таблица / Партиция</th>
                  <th class="py-2.5 px-3">HDFS Маршрут / URI</th>
                  <th class="py-2.5 px-3 w-24">Статус</th>
                  <th class="py-2.5 px-3 w-24 text-right">Время</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60 font-mono">
                {#each events as ev}
                  <tr class="hover:bg-slate-50/50 dark:hover:bg-slate-850/50">
                    <td class="py-2.5 px-3">
                      <span class="px-2 py-0.5 rounded text-[10px] font-semibold {ev.event_type.startsWith('BOOTSTRAP') ? 'bg-purple-50 text-purple-700 dark:bg-purple-950/50 dark:text-purple-300 border border-purple-200 dark:border-purple-800' : 'bg-sky-50 text-sky-700 dark:bg-sky-950/50 dark:text-sky-300 border border-sky-200 dark:border-sky-800'}">
                        {ev.event_type}
                      </span>
                    </td>
                    <td class="py-2.5 px-3 font-medium text-slate-800 dark:text-slate-200 truncate" title="{ev.table_name}{ev.partition_name ? ` (${ev.partition_name})` : ''}">
                      {ev.table_name}
                      {#if ev.partition_name}
                        <span class="text-slate-400 font-normal text-[10px]">({ev.partition_name})</span>
                      {/if}
                    </td>
                    <td class="py-2.5 px-3 text-slate-500 truncate" title={ev.source_uri || '—'}>
                      {ev.source_uri || '—'}
                    </td>
                    <td class="py-2.5 px-3">
                      <span class="text-[10px] font-semibold text-emerald-600 dark:text-emerald-400">
                        ● {ev.status}
                      </span>
                    </td>
                    <td class="py-2.5 px-3 text-right text-slate-400 text-[10px]">
                      {formatTime(ev.created_at)}
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          {/if}
        {:else}
          {#if subtasks.length === 0}
            <div class="py-12 text-center text-slate-400 text-xs">
              Вложенных подзадач передачи HDFS пока нет.
            </div>
          {:else}
            <table class="w-full table-fixed text-left border-collapse text-xs">
              <thead class="bg-slate-50 dark:bg-slate-950/80 border-b border-slate-200 dark:border-slate-800 text-slate-500 font-semibold text-[10px] uppercase tracking-wider sticky top-0">
                <tr>
                  <th class="py-2.5 px-3 w-36">ID саб-джоры</th>
                  <th class="py-2.5 px-3">HDFS пути (src → dst)</th>
                  <th class="py-2.5 px-3 w-36">Объем</th>
                  <th class="py-2.5 px-3 w-28">Статус</th>
                  <th class="py-2.5 px-3 w-24 text-right">Время</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60 font-mono">
                {#each subtasks as sub}
                  <tr class="hover:bg-slate-50/50 dark:hover:bg-slate-850/50">
                    <td class="py-2.5 px-3 text-slate-700 dark:text-slate-300 truncate" title={sub.id}>
                      <div class="truncate">{sub.id.substring(0, 12)}...</div>
                      <div class="text-[9px] text-sky-600 dark:text-sky-400 font-mono truncate">
                        👤 {sub.execution_principal || sub.created_by || 'system'}
                      </div>
                    </td>
                    <td class="py-2.5 px-3 truncate" title="{sub.source_path} → {sub.target_path}">
                      <div class="truncate text-slate-700 dark:text-slate-300"><span class="text-slate-400">src:</span> {sub.source_path}</div>
                      <div class="truncate text-slate-500"><span class="text-slate-400">dst:</span> {sub.target_path}</div>
                    </td>
                    <td class="py-2.5 px-3 font-mono text-[10px] text-slate-600 dark:text-slate-300">
                      {formatBytes(sub.copied_bytes)} / {formatBytes(sub.total_bytes)}
                    </td>
                    <td class="py-2.5 px-3">
                      <StatusBadge status={sub.status} />
                    </td>
                    <td class="py-2.5 px-3 text-right text-slate-400 text-[10px]">
                      {formatTime(sub.started_at || sub.created_at)}
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          {/if}
        {/if}
      </div>

      <!-- Подвал модального окна -->
      <div class="flex justify-between items-center pt-2 border-t border-slate-100 dark:border-slate-800">
        <div class="text-[11px] text-slate-400">
          Скрытый тип задач: <code class="bg-slate-100 dark:bg-slate-800 px-1 py-0.5 rounded text-sky-600">HMS_SUBJOB</code> (изолированы от регламентных HDFS джоб).
        </div>
        <button
          onclick={() => (isDetailsModalOpen = false)}
          class="px-4 py-2 text-xs font-semibold rounded-xl bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 transition cursor-pointer"
        >
          Закрыть
        </button>
      </div>
    </div>
  </div>
{/if}

<!-- МОДАЛЬНОЕ ОКНО СОЗДАНИЯ НОВОЙ РЕПЛИКАЦИИ СХЕМЫ (ЕДИНЫЙ СТИЛЬ HDFS) -->
{#if isCreateModalOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    role="presentation"
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) isCreateModalOpen = false; }}
  >
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl max-w-xl w-full border border-slate-200 dark:border-slate-800 shadow-2xl flex flex-col max-h-[90vh] overflow-hidden select-auto text-slate-900 dark:text-slate-100"
    >
      <!-- Заголовок (фиксированный) -->
      <div class="flex justify-between items-center px-6 py-4 border-b border-slate-100 dark:border-slate-800 shrink-0">
        <div class="flex items-center gap-3">
          <div class="w-8 h-8 rounded-lg bg-sky-50 dark:bg-sky-950/60 border border-sky-200 dark:border-sky-800 flex items-center justify-center text-sky-600 dark:text-sky-400">
            <Database class="w-4 h-4" />
          </div>
          <div>
            <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">
              Новая репликация схемы Hive Metastore
            </h3>
            <p class="text-[11px] text-slate-500">Автоматический Bootstrap и непрерывный CDC</p>
          </div>
        </div>
        <button
          type="button"
          onclick={() => (isCreateModalOpen = false)}
          class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 transition cursor-pointer p-1 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800"
        >
          <X class="w-5 h-5" />
        </button>
      </div>

      <!-- Тело формы со скроллом при необходимости -->
      <div class="p-6 overflow-y-auto flex-1 space-y-4 text-xs">
        <!-- Кластеры: Источник и Назначение в 2 колонки -->
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
          <div>
            <label for="hms-src-cluster" class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Исходный кластер (Source)
            </label>
            <select
              id="hms-src-cluster"
              bind:value={newJob.source_cluster_id}
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500 cursor-pointer"
            >
              {#if clusters && clusters.length > 0}
                {#each clusters as c}
                  <option value={c.id}>
                    {c.name} [{c.dc_id.toUpperCase()}]{getClusterMetaTag(c.id, c.dc_id)}
                  </option>
                {/each}
              {:else}
                <option value="dc1">HDFS DC1 Production [DC1] — HDP 3.1</option>
                <option value="dc2">HDFS DC2 Disaster Recovery [DC2] — Apache Hive 3.1.3</option>
              {/if}
            </select>
          </div>

          <div>
            <label for="hms-target-cluster" class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Целевой кластер (Target)
            </label>
            <select
              id="hms-target-cluster"
              bind:value={newJob.target_cluster_id}
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500 cursor-pointer"
            >
              {#if clusters && clusters.length > 0}
                {#each clusters as c}
                  <option value={c.id}>
                    {c.name} [{c.dc_id.toUpperCase()}]{getClusterMetaTag(c.id, c.dc_id)}
                  </option>
                {/each}
              {:else}
                <option value="dc2">HDFS DC2 Disaster Recovery [DC2] — Apache Hive 3.1.3</option>
                <option value="dc1">HDFS DC1 Production [DC1] — HDP 3.1</option>
              {/if}
            </select>
          </div>
        </div>

        <!-- Базы данных: Источник и Приемник в 2 колонки -->
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
          <div>
            <label for="hms-source-db" class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
              База в источнике <span class="text-rose-500">*</span>
            </label>
            <input
              id="hms-source-db"
              type="text"
              placeholder="retail_analytics_dw"
              bind:value={newJob.source_db}
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>

          <div>
            <label for="hms-target-db" class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
              База на приемнике
            </label>
            <input
              id="hms-target-db"
              type="text"
              placeholder="совпадает с источником"
              bind:value={newJob.target_db}
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>
        </div>

        <!-- Фильтр таблиц и Пользователь имперсонации в 2 колонки -->
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
          <div>
            <label for="hms-table-pattern" class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Фильтр таблиц (Include Pattern)
            </label>
            <input
              id="hms-table-pattern"
              type="text"
              placeholder="*"
              bind:value={newJob.table_pattern}
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>

          <div>
            <div class="flex items-center justify-between mb-1">
              <label for="hms-impersonation-user" class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1">
                <span>Имперсонация HDFS</span>
                <span
                  class="cursor-help text-slate-400 hover:text-sky-500 transition"
                  title={isAdmin
                    ? 'Администратор может указать имя пользователя для Kerberos doAs имперсонации при репликации файлов данных таблиц и партиций в HDFS.'
                    : `Репликация файлов данных в HDFS выполняется от имени ${user?.username || 'текущего пользователя'} через Kerberos Proxy User.`}
                >
                  <Info class="w-3.5 h-3.5" />
                </span>
              </label>
              <span class="text-[10px] font-mono px-1.5 py-0.2 rounded {isAdmin ? 'bg-sky-100 dark:bg-sky-900/60 text-sky-700 dark:text-sky-300 font-semibold' : 'bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-300'}">
                {isAdmin ? 'admin' : 'doAs'}
              </span>
            </div>
            {#if isAdmin}
              <input
                id="hms-impersonation-user"
                type="text"
                bind:value={newJob.execution_principal}
                placeholder={user?.username || 'hdfs'}
                class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
            {:else}
              <input
                id="hms-impersonation-user"
                type="text"
                value={user?.username || 'текущий пользователь'}
                disabled
                class="w-full px-3 py-2 bg-slate-100 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-mono text-slate-500 dark:text-slate-400 cursor-not-allowed"
              />
            {/if}
          </div>
        </div>

        <!-- Опции двустороннего согласования схемы (Diff & Reconciliation) -->
        <div class="pt-3 border-t border-slate-100 dark:border-slate-800">
          <div class="font-semibold text-slate-700 dark:text-slate-300 text-[11px] uppercase tracking-wider mb-2">
            Сверка схемы и согласование (Diff & Reconciliation)
          </div>
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <label class="flex items-start gap-2.5 p-2.5 rounded-xl border border-slate-200/80 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-950/40 cursor-pointer select-none hover:border-slate-300 dark:hover:border-slate-700 transition">
              <input
                type="checkbox"
                bind:checked={newJob.drop_extraneous_tables}
                class="mt-0.5 rounded text-sky-600 focus:ring-sky-500 cursor-pointer"
              />
              <div>
                <div class="font-medium text-slate-800 dark:text-slate-200">
                  Удалять лишние таблицы
                </div>
                <div class="text-[10px] text-slate-500 leading-tight mt-0.5">
                  Удаляет из приемника таблицы, отсутствующие в источнике (deleteData=false).
                </div>
              </div>
            </label>
            <label class="flex items-start gap-2.5 p-2.5 rounded-xl border border-slate-200/80 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-950/40 cursor-pointer select-none hover:border-slate-300 dark:hover:border-slate-700 transition">
              <input
                type="checkbox"
                bind:checked={newJob.drop_extraneous_partitions}
                class="mt-0.5 rounded text-sky-600 focus:ring-sky-500 cursor-pointer"
              />
              <div>
                <div class="font-medium text-slate-800 dark:text-slate-200">
                  Удалять лишние партиции
                </div>
                <div class="text-[10px] text-slate-500 leading-tight mt-0.5">
                  Удаляет партиции-сироты из целевого HMS (deleteData=false).
                </div>
              </div>
            </label>
          </div>
        </div>

        <!-- Всплывающий баннер правил репликации -->
        {#if showRulesInfo}
          <div class="p-3 bg-amber-50 dark:bg-amber-950/40 rounded-xl border border-amber-200 dark:border-amber-800 text-[11px] text-amber-800 dark:text-amber-300 space-y-1">
            <div class="font-semibold flex items-center justify-between">
              <div class="flex items-center gap-1.5">
                <Shield class="w-3.5 h-3.5" /> <span>Правила репликации схемы:</span>
              </div>
              <button
                type="button"
                onclick={() => (showRulesInfo = false)}
                class="text-amber-600 dark:text-amber-400 hover:text-amber-800 dark:hover:text-amber-200 p-0.5 cursor-pointer"
              >
                <X class="w-3.5 h-3.5" />
              </button>
            </div>
            <div>• Выполняется полный первичный Bootstrap всех таблиц и партиций.</div>
            <div>• Поддерживаются External и Managed Non-Transactional таблицы (ACID пропускаются).</div>
            <div>• Перенос файлов партиций осуществляется изолированными подзадачами HDFS.</div>
          </div>
        {/if}
      </div>

      <!-- Фиксированный футер с кнопками и кнопкой правил -->
      <div class="px-6 py-3.5 bg-slate-50 dark:bg-slate-950/70 border-t border-slate-100 dark:border-slate-800 shrink-0 flex items-center justify-between gap-3">
        <button
          type="button"
          onclick={() => (showRulesInfo = !showRulesInfo)}
          class="flex items-center gap-1.5 text-xs text-slate-500 hover:text-amber-600 dark:hover:text-amber-400 transition cursor-pointer select-none"
        >
          <Info class="w-4 h-4 text-amber-500" />
          <span class="underline decoration-dotted underline-offset-2">
            {showRulesInfo ? 'Скрыть правила' : 'Правила репликации'}
          </span>
        </button>

        <div class="flex items-center gap-2">
          <button
            type="button"
            onclick={() => (isCreateModalOpen = false)}
            class="px-4 py-2 text-xs font-semibold rounded-xl border border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
          >
            Отмена
          </button>
          <button
            type="button"
            onclick={handleCreateJob}
            disabled={!newJob.source_db.trim() || loading}
            class="px-4 py-2 text-xs font-semibold rounded-xl bg-sky-600 hover:bg-sky-500 text-white transition cursor-pointer shadow-md shadow-sky-600/20 disabled:opacity-50"
          >
            {loading ? 'Создание...' : 'Запустить репликацию'}
          </button>
        </div>
      </div>
    </div>
  </div>
{/if}

<!-- МОДАЛЬНОЕ ОКНО ПОДТВЕРЖДЕНИЯ УДАЛЕНИЯ ЗАДАЧИ HMS -->
{#if isDeleteModalOpen && jobToDelete}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    role="presentation"
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) isDeleteModalOpen = false; }}
  >
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl max-w-md w-full border border-slate-200 dark:border-slate-800 shadow-2xl p-6 space-y-4 select-auto text-slate-900 dark:text-slate-100"
    >
      <div class="flex items-start gap-3">
        <div class="p-2.5 rounded-xl bg-rose-50 dark:bg-rose-950/50 text-rose-600 dark:text-rose-400 shrink-0">
          <AlertTriangle class="w-5 h-5" />
        </div>
        <div>
          <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">
            Удалить задачу репликации схемы?
          </h3>
          <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">
            Вы уверены, что хотите удалить синхронизацию схемы <strong class="text-slate-800 dark:text-slate-200 font-mono">{jobToDelete.source_db_name}</strong>?
          </p>
        </div>
      </div>

      <div class="p-3 bg-slate-50 dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 text-[11px] text-slate-600 dark:text-slate-400 space-y-1">
        <div>• Задача перестанет отслеживать события CDC в Hive Metastore.</div>
        <div>• Дочерние HDFS подзадачи и журнал событий будут удалены.</div>
        <div>• <strong class="text-slate-700 dark:text-slate-300">Данные на целевом HDFS не будут удалены</strong> (deleteData = false).</div>
      </div>

      <div class="flex justify-end gap-2 pt-2 border-t border-slate-100 dark:border-slate-800">
        <button
          onclick={() => (isDeleteModalOpen = false)}
          disabled={isDeleting}
          class="px-4 py-2 text-xs font-semibold rounded-xl border border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800 transition cursor-pointer"
        >
          Отмена
        </button>
        <button
          onclick={confirmDeleteJob}
          disabled={isDeleting}
          class="px-4 py-2 text-xs font-semibold rounded-xl bg-rose-600 hover:bg-rose-500 text-white transition cursor-pointer shadow-md shadow-rose-600/20 disabled:opacity-50"
        >
          {isDeleting ? 'Удаление...' : 'Да, удалить схему'}
        </button>
      </div>
    </div>
  </div>
{/if}

<!-- МОДАЛЬНОЕ ОКНО ПОДТВЕРЖДЕНИЯ RE-BOOTSTRAP ЗАДАЧИ HMS -->
{#if isRebootstrapModalOpen && jobToRebootstrap}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    role="presentation"
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) isRebootstrapModalOpen = false; }}
  >
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl max-w-md w-full border border-slate-200 dark:border-slate-800 shadow-2xl p-6 space-y-4 select-auto text-slate-900 dark:text-slate-100"
    >
      <div class="flex items-start gap-3">
        <div class="p-2.5 rounded-xl bg-sky-50 dark:bg-sky-950/50 text-sky-600 dark:text-sky-400 shrink-0">
          <RefreshCw class="w-5 h-5 {isRebootstrapping ? 'animate-spin' : ''}" />
        </div>
        <div>
          <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">
            Запустить повторный Bootstrap схемы?
          </h3>
          <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">
            Вы уверены, что хотите выполнить полный повторный экспорт метаданных для схемы <strong class="text-slate-800 dark:text-slate-200 font-mono">{jobToRebootstrap.source_db_name}</strong>?
          </p>
        </div>
      </div>

      <div class="p-3 bg-slate-50 dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 text-[11px] text-slate-600 dark:text-slate-400 space-y-1.5">
        <div>• Будет выполнен полный экспорт текущего среза таблиц и партиций из источника ({jobToRebootstrap.source_cluster_id} → {jobToRebootstrap.target_cluster_id}).</div>
        <div>• Чекпоинт CDC будет синхронизирован до актуального High Water Mark в Hive Metastore.</div>
        <div>• <strong class="text-slate-700 dark:text-slate-300">Существующие файлы данных в HDFS не удаляются</strong> (deleteData = false, безопасная синхронизация).</div>
      </div>

      <div class="flex justify-end gap-2 pt-2 border-t border-slate-100 dark:border-slate-800">
        <button
          onclick={() => (isRebootstrapModalOpen = false)}
          disabled={isRebootstrapping}
          class="px-4 py-2 text-xs font-semibold rounded-xl border border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800 transition cursor-pointer"
        >
          Отмена
        </button>
        <button
          onclick={confirmRebootstrap}
          disabled={isRebootstrapping}
          class="px-4 py-2 text-xs font-semibold rounded-xl bg-sky-600 hover:bg-sky-500 text-white transition cursor-pointer shadow-md shadow-sky-600/20 disabled:opacity-50 flex items-center gap-1.5"
        >
          {#if isRebootstrapping}
            <RefreshCw class="w-3.5 h-3.5 animate-spin" />
            <span>Запуск...</span>
          {:else}
            <RefreshCw class="w-3.5 h-3.5" />
            <span>Запустить Re-bootstrap</span>
          {/if}
        </button>
      </div>
    </div>
  </div>
{/if}
