<script lang="ts">
  import type { Job } from '../types';
  import { StatusBadge } from '@hadoop-explorer/common';
  import {
    ArrowLeftRight,
    RefreshCw,
    Plus,
    X,
    Calendar,
    Play,
    Square,
    Pencil,
    Trash2,
    Clock,
    Zap,
    Filter,
    Search,
    User,
    RotateCcw,
    History,
    Files
  } from 'lucide-svelte';

  interface Props {
    jobs: Job[];
    canCreate: boolean;
    canManageJob: (job: Job) => boolean;
    onRefresh: () => void;
    onCreateJob: () => void;
    onStartJob: (id: string) => void;
    onStopJob: (id: string) => void;
    onEditJob: (job: Job) => void;
    onDeleteJob: (id: string) => void;
    onOpenHistory: (job: Job) => void;
  }

  let {
    jobs,
    canCreate,
    canManageJob,
    onRefresh,
    onCreateJob,
    onStartJob,
    onStopJob,
    onEditJob,
    onDeleteJob,
    onOpenHistory
  }: Props = $props();

  // Фильтрация и поиск задач
  let selectedStatus = $state<string>('ALL');
  let selectedAuthor = $state<string>('ALL');
  let searchQuery = $state<string>('');

  // Статистика задач
  const stats = $derived({
    total: jobs.length,
    running: jobs.filter((j) => j.status === 'RUNNING').length,
    queued: jobs.filter((j) => j.status === 'QUEUED').length,
    scheduled: jobs.filter((j) => j.status === 'SCHEDULED').length,
    completed: jobs.filter((j) => j.status === 'COMPLETED').length,
    failed: jobs.filter((j) => j.status === 'FAILED').length,
    streaming: jobs.filter((j) => j.status === 'STREAMING' || j.sync_mode === 'STREAMING_INOTIFY').length,
  });

  // Список уникальных авторов задач
  const authorsList = $derived(
    Array.from(new Set(jobs.map((j) => j.created_by).filter((a): a is string => Boolean(a)))).sort()
  );

  // Реактивный список отфильтрованных задач
  const filteredJobs = $derived(
    jobs.filter((job) => {
      // Фильтр по статусу
      if (selectedStatus !== 'ALL') {
        if (selectedStatus === 'STREAMING') {
          if (job.status !== 'STREAMING' && job.sync_mode !== 'STREAMING_INOTIFY') {
            return false;
          }
        } else if (job.status !== selectedStatus) {
          return false;
        }
      }
      // Фильтр по автору
      if (selectedAuthor !== 'ALL' && job.created_by !== selectedAuthor) {
        return false;
      }
      // Быстрый поиск
      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase().trim();
        const matchId = job.id.toLowerCase().includes(q);
        const matchSrc = job.source_path.toLowerCase().includes(q);
        const matchDst = job.target_path.toLowerCase().includes(q);
        const matchClusters = `${job.source_cluster_id} ${job.target_cluster_id}`.toLowerCase().includes(q);
        const matchPrincipal = (job.execution_principal || '').toLowerCase().includes(q);
        const matchAuthor = (job.created_by || '').toLowerCase().includes(q);
        if (!matchId && !matchSrc && !matchDst && !matchClusters && !matchPrincipal && !matchAuthor) {
          return false;
        }
      }
      return true;
    })
  );

  const hasActiveFilters = $derived(
    selectedStatus !== 'ALL' || selectedAuthor !== 'ALL' || searchQuery.trim().length > 0
  );

  function resetFilters() {
    selectedStatus = 'ALL';
    selectedAuthor = 'ALL';
    searchQuery = '';
  }

  function toggleStatusFilter(targetStatus: string) {
    if (selectedStatus === targetStatus) {
      selectedStatus = 'ALL';
    } else {
      selectedStatus = targetStatus;
    }
  }

  function formatBytes(bytes: number): string {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
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

  function getProgressPercent(job: Job): number {
    if (job.progress_percent !== undefined && !isNaN(job.progress_percent) && job.progress_percent > 0) {
      return Math.min(100, Math.max(0, Math.round(job.progress_percent)));
    }
    if (job.total_bytes > 0 && job.copied_bytes > 0) {
      return Math.min(100, Math.max(0, Math.round((job.copied_bytes / job.total_bytes) * 100)));
    }
    return job.status === 'COMPLETED' ? 100 : 0;
  }

  function formatSpeed(job: Job): string {
    const speed = job.average_speed_mb_s ?? job.transfer_speed_mb_s;
    if (speed !== undefined && speed !== null && speed > 0) {
      return `${speed.toFixed(1)} МБ/с`;
    }
    if (job.copied_bytes > 0 && (job.started_at || job.created_at)) {
      const start = new Date(job.started_at || job.created_at!).getTime();
      const end = job.completed_at ? new Date(job.completed_at).getTime() : Date.now();
      const elapsedSec = (end - start) / 1000;
      if (elapsedSec > 0) {
        const mb = (job.copied_bytes / (1024 * 1024)) / elapsedSec;
        if (mb > 0) return `${mb.toFixed(1)} МБ/с`;
      }
    }
    return job.status === 'RUNNING' ? '0.0 МБ/с' : '—';
  }

  function formatEta(job: Job): string {
    if (job.status === 'SCHEDULED') {
      return job.next_run_at ? formatTime(job.next_run_at) : 'По расписанию';
    }
    if (job.status === 'COMPLETED') {
      return formatTime(job.completed_at || job.updated_at);
    }
    if (job.status === 'CANCELLED') {
      return 'Отменена';
    }
    if (job.status === 'FAILED') {
      return 'Ошибка';
    }
    if (job.status === 'QUEUED') {
      return 'В очереди';
    }
    if (job.estimated_completion_at) {
      const etaTime = formatTime(job.estimated_completion_at);
      const diffMs = new Date(job.estimated_completion_at).getTime() - Date.now();
      const diffMin = Math.round(diffMs / 60000);
      const remainingStr = diffMin > 0 ? `~${diffMin} мин` : '< 1 мин';
      return `~${etaTime} (${remainingStr})`;
    }
    if (job.total_bytes > 0 && job.copied_bytes > 0 && (job.started_at || job.created_at)) {
      const start = new Date(job.started_at || job.created_at!).getTime();
      const elapsedSec = (Date.now() - start) / 1000;
      if (elapsedSec > 0) {
        const bytesPerSec = job.copied_bytes / elapsedSec;
        const remainingBytes = Math.max(0, job.total_bytes - job.copied_bytes);
        if (bytesPerSec > 0 && remainingBytes > 0) {
          const remainingSec = Math.round(remainingBytes / bytesPerSec);
          const etaDate = new Date(Date.now() + remainingSec * 1000);
          const etaTime = etaDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
          const remMin = Math.round(remainingSec / 60);
          const remStr = remMin > 0 ? `~${remMin} мин` : `${remainingSec} сек`;
          return `~${etaTime} (${remStr})`;
        }
      }
    }
    return 'Оценка времени...';
  }
</script>

<main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
  <!-- Верхняя панель: Статистика и Кнопка создания -->
  <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
    <!-- Карточки статистики (с интерактивным фильтром статуса) -->
    <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-3 flex-1">
      <button
        type="button"
        onclick={() => (selectedStatus = 'ALL')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'ALL'
          ? 'bg-slate-100 dark:bg-slate-800 border-slate-400 dark:border-slate-500 ring-2 ring-slate-400/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300 dark:hover:border-slate-700'}"
      >
        <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Всего задач</span>
        <span class="text-xl font-bold text-slate-900 dark:text-slate-100">{stats.total}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('RUNNING')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'RUNNING'
          ? 'bg-sky-50 dark:bg-sky-950/40 border-sky-400 dark:border-sky-600 ring-2 ring-sky-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-sky-300 dark:hover:border-sky-800'}"
      >
        <span class="text-[11px] font-medium text-sky-600 dark:text-sky-400 block">В работе</span>
        <span class="text-xl font-bold text-sky-600 dark:text-sky-400">{stats.running}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('QUEUED')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'QUEUED'
          ? 'bg-amber-50 dark:bg-amber-950/40 border-amber-400 dark:border-amber-600 ring-2 ring-amber-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-amber-300 dark:hover:border-amber-800'}"
      >
        <span class="text-[11px] font-medium text-amber-600 dark:text-amber-400 block">В очереди</span>
        <span class="text-xl font-bold text-amber-600 dark:text-amber-400">{stats.queued}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('SCHEDULED')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'SCHEDULED'
          ? 'bg-indigo-50 dark:bg-indigo-950/40 border-indigo-400 dark:border-indigo-600 ring-2 ring-indigo-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-indigo-300 dark:hover:border-indigo-800'}"
      >
        <span class="text-[11px] font-medium text-indigo-600 dark:text-indigo-400 block">По расписанию</span>
        <span class="text-xl font-bold text-indigo-600 dark:text-indigo-400">{stats.scheduled}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('STREAMING')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'STREAMING'
          ? 'bg-purple-50 dark:bg-purple-950/40 border-purple-400 dark:border-purple-600 ring-2 ring-purple-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-purple-300 dark:hover:border-purple-800'}"
      >
        <div class="flex items-center justify-between">
          <span class="text-[11px] font-medium text-purple-600 dark:text-purple-400 block">Live Inotify</span>
          {#if stats.streaming > 0}
            <span class="w-2 h-2 rounded-full bg-purple-500 animate-pulse" title="Активный поток HDFS событий"></span>
          {/if}
        </div>
        <span class="text-xl font-bold text-purple-600 dark:text-purple-400">{stats.streaming}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('COMPLETED')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'COMPLETED'
          ? 'bg-emerald-50 dark:bg-emerald-950/40 border-emerald-400 dark:border-emerald-600 ring-2 ring-emerald-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-emerald-300 dark:hover:border-emerald-800'}"
      >
        <span class="text-[11px] font-medium text-emerald-600 dark:text-emerald-400 block">Завершено</span>
        <span class="text-xl font-bold text-emerald-600 dark:text-emerald-400">{stats.completed}</span>
      </button>

      <button
        type="button"
        onclick={() => toggleStatusFilter('FAILED')}
        class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'FAILED'
          ? 'bg-rose-50 dark:bg-rose-950/40 border-rose-400 dark:border-rose-600 ring-2 ring-rose-500/30'
          : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-rose-300 dark:hover:border-rose-800'}"
      >
        <span class="text-[11px] font-medium text-rose-600 dark:text-rose-400 block">Ошибки</span>
        <span class="text-xl font-bold text-rose-600 dark:text-rose-400">{stats.failed}</span>
      </button>
    </div>

    <!-- Кнопка Новая задача -->
    <div class="flex items-center gap-2 self-end sm:self-auto shrink-0">
      <button
        onclick={onRefresh}
        class="p-2.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-300 transition cursor-pointer shadow-2xs"
        title="Обновить список"
      >
        <RefreshCw class="w-4 h-4" />
      </button>
      {#if canCreate}
        <button
          onclick={onCreateJob}
          class="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-sky-600 hover:bg-sky-500 text-white font-semibold text-xs transition cursor-pointer shadow-md shadow-sky-600/20"
        >
          <Plus class="w-4 h-4" />
          <span>Новая задача репликации</span>
        </button>
      {:else}
        <button
          disabled
          class="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-400 dark:text-slate-500 font-semibold text-xs opacity-60 cursor-not-allowed"
          title="Доступ только для чтения (READER): создание задач запрещено"
        >
          <Plus class="w-4 h-4" />
          <span>Создание недоступно (RO)</span>
        </button>
      {/if}
    </div>
  </div>

  <!-- Панель фильтров и поиска -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-3.5 shadow-2xs">
    <div class="flex flex-col md:flex-row items-stretch md:items-center justify-between gap-3">
      <!-- Левая часть: Поиск + Фильтр Статус + Фильтр Автор -->
      <div class="flex flex-wrap items-center gap-3 flex-1">
        <!-- Поиск -->
        <div class="relative min-w-[220px] flex-1 max-w-sm">
          <Search class="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
          <input
            type="text"
            bind:value={searchQuery}
            placeholder="Поиск по ID, путям, кластерам, principal..."
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
          <label for="filter-status" class="text-xs text-slate-500 dark:text-slate-400 font-medium flex items-center gap-1.5 whitespace-nowrap">
            <Filter class="w-3.5 h-3.5 text-slate-400" />
            <span>Статус:</span>
          </label>
          <select
            id="filter-status"
            bind:value={selectedStatus}
            class="px-3 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 text-xs text-slate-700 dark:text-slate-300 focus:ring-2 focus:ring-sky-500 focus:outline-none cursor-pointer"
          >
            <option value="ALL">Все статусы ({jobs.length})</option>
            <option value="RUNNING">В работе ({stats.running})</option>
            <option value="QUEUED">В очереди ({stats.queued})</option>
            <option value="SCHEDULED">По расписанию ({stats.scheduled})</option>
            <option value="STREAMING">Live Inotify ({stats.streaming})</option>
            <option value="COMPLETED">Завершено ({stats.completed})</option>
            <option value="FAILED">Ошибки ({stats.failed})</option>
            <option value="CANCELLED">Отменено ({jobs.filter((j) => j.status === 'CANCELLED').length})</option>
          </select>
        </div>

        <!-- Фильтр по автору -->
        <div class="flex items-center gap-2">
          <label for="filter-author" class="text-xs text-slate-500 dark:text-slate-400 font-medium flex items-center gap-1.5 whitespace-nowrap">
            <User class="w-3.5 h-3.5 text-slate-400" />
            <span>Автор:</span>
          </label>
          <select
            id="filter-author"
            bind:value={selectedAuthor}
            class="px-3 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-750 text-xs text-slate-700 dark:text-slate-300 focus:ring-2 focus:ring-sky-500 focus:outline-none cursor-pointer"
          >
            <option value="ALL">Все авторы ({authorsList.length})</option>
            {#each authorsList as author}
              <option value={author}>
                {author} ({jobs.filter((j) => j.created_by === author).length})
              </option>
            {/each}
          </select>
        </div>
      </div>

      <!-- Правая часть: Счетчик и Сброс фильтров -->
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

  <!-- Таблица задач -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-xs overflow-hidden">
    <div class="overflow-x-auto">
      <table class="w-full table-fixed text-left border-collapse text-xs min-w-[960px]">
        <thead>
          <tr class="bg-slate-50/80 dark:bg-slate-950/60 border-b border-slate-200 dark:border-slate-800 text-slate-500 dark:text-slate-400 font-semibold uppercase tracking-wider text-[10px]">
            <th scope="col" class="py-3 px-4 w-32">ID задачи</th>
            <th scope="col" class="py-3 px-4 w-44">Маршрут</th>
            <th scope="col" class="py-3 px-4">HDFS Пути</th>
            <th scope="col" class="py-3 px-4 w-40">Автор и Principal</th>
            <th scope="col" class="py-3 px-4 w-64">Прогресс</th>
            <th scope="col" class="py-3 px-4 w-32">Статус</th>
            <th scope="col" class="py-3 px-4 w-44 text-right">Действия</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60">
          {#if jobs.length === 0}
            <tr>
              <td colspan="7" class="py-12 text-center text-slate-400 dark:text-slate-500">
                <div class="flex flex-col items-center gap-2">
                  <ArrowLeftRight class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                  <span>Нет активных или выполненных задач репликации</span>
                  <button
                    onclick={onCreateJob}
                    class="mt-2 text-xs text-sky-600 dark:text-sky-400 hover:underline font-semibold cursor-pointer"
                  >
                    Создать первую задачу
                  </button>
                </div>
              </td>
            </tr>
          {:else if filteredJobs.length === 0}
            <tr>
              <td colspan="7" class="py-12 text-center text-slate-400 dark:text-slate-500">
                <div class="flex flex-col items-center gap-2">
                  <Filter class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                  <span class="font-medium text-slate-700 dark:text-slate-300">Задачи по заданным фильтрам не найдены</span>
                  <span class="text-[11px] text-slate-400">Попробуйте изменить статус, автора или строку поиска</span>
                  <button
                    type="button"
                    onclick={resetFilters}
                    class="mt-2 flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-xs font-medium text-slate-700 dark:text-slate-200 transition cursor-pointer"
                  >
                    <RotateCcw class="w-3.5 h-3.5 text-slate-400" />
                    <span>Сбросить все фильтры</span>
                  </button>
                </div>
              </td>
            </tr>
          {:else}
            {#each filteredJobs as job (job.id)}
              {@const isActive = job.status === 'RUNNING' || job.status === 'QUEUED'}
              <tr class="hover:bg-slate-50/60 dark:hover:bg-slate-850/50 transition-colors">
                <!-- ID -->
                <td class="py-3.5 px-4 font-mono font-medium text-slate-800 dark:text-slate-200">
                  <span title={job.id}>{job.id.substring(0, 8)}...</span>
                  {#if job.is_scheduled}
                    <span class="inline-flex items-center gap-0.5 px-1.5 py-0.2 rounded text-[9px] font-mono bg-indigo-50 dark:bg-indigo-950/50 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800 ml-1" title="Периодическая задача по расписанию">
                      <Calendar class="w-2.5 h-2.5" />
                      {job.cron_expression || 'cron'}
                    </span>
                  {/if}
                  {#if job.sync_mode === 'STREAMING_INOTIFY' || job.status === 'STREAMING'}
                    <span class="inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[9px] font-semibold bg-purple-50 dark:bg-purple-950/60 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-800 ml-1" title="Потоковая репликация HDFS Inotify (Near-Zero RPO)">
                      <span class="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
                      Inotify
                    </span>
                  {/if}
                </td>

                <!-- Маршрут -->
                <td class="py-3.5 px-4">
                  <div class="flex items-center gap-1.5 font-medium">
                    <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-mono text-[11px]">
                      {job.source_cluster_id}
                    </span>
                    <span class="text-slate-400">→</span>
                    <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-mono text-[11px]">
                      {job.target_cluster_id}
                    </span>
                  </div>
                </td>

                <!-- Пути -->
                <td class="py-3.5 px-4 overflow-hidden">
                  <div class="truncate font-mono text-[11px] text-slate-700 dark:text-slate-300" title={job.source_path}>
                    <span class="text-slate-400">src:</span> {job.source_path}
                  </div>
                  <div class="truncate font-mono text-[11px] text-slate-500 dark:text-slate-400" title={job.target_path}>
                    <span class="text-slate-400">dst:</span> {job.target_path}
                  </div>
                </td>

                <!-- Владелец -->
                <td class="py-3.5 px-4 truncate">
                  <div class="font-medium text-slate-800 dark:text-slate-200 truncate">{job.created_by || 'system'}</div>
                  <div class="text-[10px] font-mono truncate" title={job.execution_principal || ''}>
                    {#if job.run_as_service_account}
                      <span class="text-slate-400">🤖 {job.execution_principal || 'tech-service'}</span>
                    {:else}
                      <span class="text-sky-600 dark:text-sky-400 inline-flex items-center gap-1">
                        <span>👤 {job.execution_principal || job.created_by}</span>
                        <span class="text-[9px] px-1 py-0.2 rounded bg-sky-100 dark:bg-sky-950/60 font-semibold text-sky-700 dark:text-sky-300">doAs</span>
                      </span>
                    {/if}
                  </div>
                </td>

                <!-- Прогресс -->
                <td class="py-3.5 px-4">
                  {#if job.sync_mode === 'STREAMING_INOTIFY' || job.status === 'STREAMING'}
                    <div class="p-2 rounded-lg bg-purple-50/60 dark:bg-purple-950/30 border border-purple-200/60 dark:border-purple-800/40 space-y-1">
                      <div class="flex items-center justify-between text-[11px] font-semibold">
                        <span class="text-purple-700 dark:text-purple-300 flex items-center gap-1 font-mono">
                          <span class="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
                          Live Sync
                        </span>
                        <span class="font-mono text-[10px] text-slate-500 dark:text-slate-400">
                          TxID: {job.last_processed_txid ?? 0}
                        </span>
                      </div>
                      <div class="flex items-center justify-between text-[10px]">
                        <span class="text-slate-400">Лаг событий:</span>
                        {#if (job.txid_lag ?? 0) === 0}
                          <span class="font-mono font-semibold text-emerald-600 dark:text-emerald-400">0 tx (In-sync)</span>
                        {:else}
                          <span class="font-mono font-semibold text-amber-600 dark:text-amber-400">+{job.txid_lag} tx</span>
                        {/if}
                      </div>
                    </div>
                  {:else}
                    <div class="flex items-center justify-between text-[11px] font-semibold mb-1">
                      <span class="{job.status === 'FAILED' ? 'text-rose-600 dark:text-rose-400' : job.status === 'COMPLETED' ? 'text-emerald-600 dark:text-emerald-400' : 'text-sky-600 dark:text-sky-400'} font-mono">
                        {getProgressPercent(job)}%
                      </span>
                      <span class="text-slate-400 font-mono text-[10px]">
                        {formatBytes(job.copied_bytes)} / {formatBytes(job.total_bytes)}
                      </span>
                    </div>
                    <div class="w-full bg-slate-100 dark:bg-slate-800 h-2 rounded-full overflow-hidden shadow-inner">
                      <div
                        class="h-full rounded-full transition-all duration-300 {job.status === 'FAILED' ? 'bg-rose-500' : job.status === 'COMPLETED' ? 'bg-emerald-500' : 'bg-gradient-to-r from-sky-500 to-indigo-500'}"
                        style="width: {getProgressPercent(job)}%"
                      ></div>
                    </div>
                  {/if}

                  <!-- Детализация таймингов и скорости передачи -->
                  <div class="mt-2 space-y-1 text-[10px] text-slate-500 dark:text-slate-400 border-t border-slate-100 dark:border-slate-800/80 pt-1.5">
                    <div class="flex items-center justify-between">
                      <span class="flex items-center gap-1 text-slate-400">
                        <Play class="w-2.5 h-2.5 text-sky-500 shrink-0" />
                        <span>{job.status === 'SCHEDULED' ? (!job.started_at ? 'Создана:' : 'Посл. старт:') : 'Старт:'}</span>
                      </span>
                      <span class="font-mono text-slate-700 dark:text-slate-300 font-medium">
                        {formatTime(job.started_at || job.created_at)}
                      </span>
                    </div>

                    <div class="flex items-center justify-between">
                      <span class="flex items-center gap-1 text-slate-400">
                        <Clock class="w-2.5 h-2.5 text-indigo-500 shrink-0" />
                        <span>{job.status === 'SCHEDULED' ? 'След. запуск:' : job.status === 'COMPLETED' ? 'Финиш:' : 'ETA:'}</span>
                      </span>
                      <span class="font-mono text-slate-700 dark:text-slate-300 font-medium truncate max-w-[150px]" title={formatEta(job)}>
                        {formatEta(job)}
                      </span>
                    </div>

                    {#if job.status !== 'SCHEDULED' && job.is_scheduled && job.next_run_at}
                      <div class="flex items-center justify-between text-indigo-600 dark:text-indigo-400">
                        <span class="flex items-center gap-1">
                          <Calendar class="w-2.5 h-2.5 shrink-0" />
                          <span>След. запуск:</span>
                        </span>
                        <span class="font-mono font-medium">
                          {formatTime(job.next_run_at)}
                        </span>
                      </div>
                    {/if}

                    <div class="flex items-center justify-between">
                      <span class="flex items-center gap-1 text-slate-400">
                        <Zap class="w-2.5 h-2.5 text-amber-500 shrink-0" />
                        <span>Ср. скорость:</span>
                      </span>
                      <span class="font-mono font-semibold {job.status === 'RUNNING' ? 'text-sky-600 dark:text-sky-400' : 'text-slate-700 dark:text-slate-300'}">
                        {formatSpeed(job)}
                      </span>
                    </div>

                    {#if (job.total_objects ?? 0) > 0}
                      <div class="flex items-center justify-between">
                        <span class="flex items-center gap-1 text-slate-400">
                          <Files class="w-2.5 h-2.5 text-teal-500 shrink-0" />
                          <span>Объекты:</span>
                        </span>
                        <span class="font-mono text-slate-700 dark:text-slate-300 font-medium">
                          {(job.transferred_objects ?? 0) + (job.skipped_objects ?? 0)} / {job.total_objects}
                          {#if (job.skipped_objects ?? 0) > 0}
                            <span class="text-slate-400 text-[9px]">(проп. {job.skipped_objects})</span>
                          {/if}
                          {#if (job.failed_objects ?? 0) > 0}
                            <span class="text-rose-500 text-[9px] font-bold">(! {job.failed_objects})</span>
                          {/if}
                        </span>
                      </div>
                    {/if}
                  </div>
                </td>

                <!-- Статус -->
                <td class="py-3.5 px-4">
                  {#if job.status === 'STREAMING'}
                    <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-purple-50 dark:bg-purple-950/60 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-800">
                      <span class="w-2 h-2 rounded-full bg-purple-500 animate-pulse"></span>
                      STREAMING
                    </span>
                  {:else}
                    <StatusBadge status={job.status} />
                  {/if}
                  {#if job.error_message}
                    <div class="text-[10px] text-rose-500 truncate max-w-[150px] mt-0.5" title={job.error_message}>
                      {job.error_message}
                    </div>
                  {:else if job.message}
                    <div class="text-[10px] text-slate-500 truncate max-w-[150px] mt-0.5" title={job.message}>
                      {job.message}
                    </div>
                  {/if}
                </td>

                <!-- Действия: Старт / Стоп / Редактировать / Удалить / История -->
                <td class="py-3.5 px-4 text-right">
                  <div class="flex items-center justify-end gap-1">
                    <!-- 1. Старт / Перезапуск -->
                    <button
                      onclick={() => onStartJob(job.id)}
                      disabled={isActive || !canManageJob(job)}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-emerald-50 dark:hover:bg-emerald-950/40 hover:border-emerald-300 dark:hover:border-emerald-700 text-emerald-600 dark:text-emerald-400"
                      title={!canManageJob(job) ? 'Режим только чтения: управление недоступно' : (isActive ? 'Задача уже активна (в очереди или выполняется)' : 'Запустить задачу')}
                    >
                      <Play class="w-3.5 h-3.5 fill-current" />
                    </button>

                    <!-- 2. Стоп -->
                    <button
                      onclick={() => onStopJob(job.id)}
                      disabled={!isActive || !canManageJob(job)}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-rose-50 dark:hover:bg-rose-950/40 hover:border-rose-300 dark:hover:border-rose-700 text-rose-600 dark:text-rose-400"
                      title={!canManageJob(job) ? 'Режим только чтения: управление недоступно' : (isActive ? 'Остановить задачу' : 'Задача не активна (остановлена)')}
                    >
                      <Square class="w-3.5 h-3.5 fill-current" />
                    </button>

                    <!-- 3. Редактировать -->
                    <button
                      onclick={() => onEditJob(job)}
                      disabled={isActive || !canManageJob(job)}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-sky-50 dark:hover:bg-sky-950/40 hover:border-sky-300 dark:hover:border-sky-700 text-sky-600 dark:text-sky-400"
                      title={!canManageJob(job) ? 'Режим только чтения: редактирование недоступно' : (isActive ? 'Нельзя редактировать активную задачу (сначала остановите)' : 'Редактировать параметры')}
                    >
                      <Pencil class="w-3.5 h-3.5" />
                    </button>

                    <!-- 4. История всех запусков со статистикой -->
                    <button
                      onclick={() => onOpenHistory(job)}
                      class="relative p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer hover:bg-indigo-50 dark:hover:bg-indigo-950/40 hover:border-indigo-300 dark:hover:border-indigo-700 text-indigo-600 dark:text-indigo-400"
                      title="История всех запусков со статистикой"
                    >
                      <History class="w-3.5 h-3.5" />
                      {#if job.runs_count && job.runs_count > 0}
                        <span class="absolute -top-1 -right-1 px-1 py-0.2 text-[8px] font-mono font-bold bg-indigo-600 text-white rounded-full leading-none shadow-xs">
                          {job.runs_count}
                        </span>
                      {/if}
                    </button>

                    <!-- 5. Удалить -->
                    <button
                      onclick={() => onDeleteJob(job.id)}
                      disabled={isActive || !canManageJob(job)}
                      class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-rose-50 dark:hover:bg-rose-950/40 hover:border-rose-400 dark:hover:border-rose-800 text-slate-400 hover:text-rose-600 dark:hover:text-rose-400"
                      title={!canManageJob(job) ? 'Режим только чтения: удаление недоступно' : (isActive ? 'Нельзя удалить активную задачу (сначала остановите)' : 'Удалить задачу')}
                    >
                      <Trash2 class="w-3.5 h-3.5" />
                    </button>
                  </div>
                </td>
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </div>
</main>
