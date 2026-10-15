<script lang="ts">
  import type { Job, JobRun } from '../types';
  import { StatusBadge } from '@hadoop-explorer/common';
  import { History, X, RefreshCw, Calendar, Check, Play } from 'lucide-svelte';

  interface Props {
    isOpen: boolean;
    job: Job | null;
    jobRuns: JobRun[];
    runsLoading: boolean;
    onClose: () => void;
    onRefreshRuns: (jobId: string) => void;
    onSaveRetention: (jobId: string, retention: number) => Promise<void>;
  }

  let {
    isOpen,
    job,
    jobRuns,
    runsLoading,
    onClose,
    onRefreshRuns,
    onSaveRetention
  }: Props = $props();

  let runsRetentionInput = $state<number>(20);
  let savingRetention = $state(false);
  let retentionSaveSuccess = $state(false);

  $effect(() => {
    if (isOpen && job) {
      runsRetentionInput = job.history_retention_runs ?? 20;
      retentionSaveSuccess = false;
    }
  });

  async function handleRetentionSave() {
    if (!job) return;
    const val = Number(runsRetentionInput);
    if (!val || val < 1 || val > 500) {
      alert('Глубина истории должна быть от 1 до 500 запусков');
      return;
    }
    savingRetention = true;
    retentionSaveSuccess = false;
    try {
      await onSaveRetention(job.id, val);
      retentionSaveSuccess = true;
      setTimeout(() => {
        retentionSaveSuccess = false;
      }, 3000);
    } catch (e: any) {
      alert(e.message || 'Не удалось сохранить глубину истории');
    } finally {
      savingRetention = false;
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

  function formatDuration(seconds?: number): string {
    if (seconds === undefined || seconds === null) return '—';
    if (seconds < 60) return `${seconds.toFixed(1)} с`;
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return `${m} мин ${s} с`;
  }
</script>

{#if isOpen && job}
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) onClose(); }}
  >
    <div
      class="w-full max-w-4xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[92vh] overflow-hidden"
    >
      <!-- Шапка окна -->
      <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-indigo-50 dark:bg-indigo-950/60 border border-indigo-200 dark:border-indigo-800 flex items-center justify-center text-indigo-600 dark:text-indigo-400">
            <History class="w-5 h-5" />
          </div>
          <div>
            <div class="flex items-center gap-2">
              <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">История запусков задачи</h2>
              <StatusBadge status={job.status} />
            </div>
            <p class="text-xs text-slate-500 dark:text-slate-400 font-mono flex items-center gap-2 mt-0.5">
              <span>ID: {job.id.substring(0, 13)}...</span>
              <span>•</span>
              <span class="text-slate-700 dark:text-slate-300 font-medium">
                {job.source_cluster_id} → {job.target_cluster_id}
              </span>
              {#if job.is_scheduled}
                <span>•</span>
                <span class="inline-flex items-center gap-1 text-indigo-600 dark:text-indigo-400">
                  <Calendar class="w-3 h-3" />
                  {job.cron_expression}
                </span>
              {/if}
            </p>
          </div>
        </div>
        <div class="flex items-center gap-2">
          <button
            onclick={() => onRefreshRuns(job.id)}
            class="p-2 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-300 transition cursor-pointer shadow-2xs"
            title="Обновить историю"
          >
            <RefreshCw class="w-4 h-4 {runsLoading ? 'animate-spin' : ''}" />
          </button>
          <button
            onclick={onClose}
            class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded cursor-pointer"
          >
            <X class="w-5 h-5" />
          </button>
        </div>
      </div>

      <!-- Панель управления глубиной хранения истории (Retention) -->
      <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div class="space-y-0.5">
          <div class="text-xs font-semibold text-slate-800 dark:text-slate-200 flex items-center gap-1.5">
            <span>Настройка глубины истории:</span>
            <span class="font-mono text-indigo-600 dark:text-indigo-400">
              {job.history_retention_runs ?? 20} запусков
            </span>
          </div>
          <p class="text-[11px] text-slate-500">
            Старые запуски автоматически удаляются при превышении лимита (1–500).
          </p>
        </div>

        <div class="flex items-center gap-2 self-start sm:self-auto">
          <input
            type="number"
            min="1"
            max="500"
            bind:value={runsRetentionInput}
            class="w-24 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono focus:outline-none focus:ring-1 focus:ring-indigo-500"
          />
          <button
            onclick={handleRetentionSave}
            disabled={savingRetention}
            class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold transition cursor-pointer disabled:opacity-50 shadow-2xs"
          >
            {#if savingRetention}
              <span>Сохранение...</span>
            {:else if retentionSaveSuccess}
              <Check class="w-3.5 h-3.5" />
              <span>Сохранено!</span>
            {:else}
              <span>Применить лимит</span>
            {/if}
          </button>
        </div>
      </div>

      <!-- Сводная статистика по истории запусков -->
      <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
          <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Запусков в истории</span>
          <span class="text-lg font-bold text-slate-900 dark:text-slate-100">{jobRuns.length}</span>
        </div>
        <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
          <span class="text-[11px] font-medium text-emerald-600 dark:text-emerald-400 block">Успешных</span>
          <span class="text-lg font-bold text-emerald-600 dark:text-emerald-400">
            {jobRuns.filter((r) => r.status === 'COMPLETED').length}
          </span>
        </div>
        <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
          <span class="text-[11px] font-medium text-rose-600 dark:text-rose-400 block">С ошибками</span>
          <span class="text-lg font-bold text-rose-600 dark:text-rose-400">
            {jobRuns.filter((r) => r.status === 'FAILED').length}
          </span>
        </div>
        <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
          <span class="text-[11px] font-medium text-sky-600 dark:text-sky-400 block">Всего передано</span>
          <span class="text-lg font-bold text-sky-600 dark:text-sky-400 font-mono">
            {formatBytes(jobRuns.reduce((acc, r) => acc + (r.copied_bytes || 0), 0))}
          </span>
        </div>
      </div>

      <!-- Таблица истории запусков -->
      <div class="flex-1 overflow-y-auto border border-slate-200 dark:border-slate-800 rounded-xl bg-white dark:bg-slate-950/60 shadow-inner">
        <table class="w-full text-left border-collapse text-xs min-w-[700px]">
          <thead class="sticky top-0 bg-slate-50 dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 text-[10px] uppercase font-semibold text-slate-500 tracking-wider">
            <tr>
              <th scope="col" class="py-2.5 px-3 w-16">№</th>
              <th scope="col" class="py-2.5 px-3 w-32">Триггер</th>
              <th scope="col" class="py-2.5 px-3 w-28">Статус</th>
              <th scope="col" class="py-2.5 px-3 w-40">Старт / Финиш</th>
              <th scope="col" class="py-2.5 px-3 w-28">Длительность</th>
              <th scope="col" class="py-2.5 px-3 w-36">Объем</th>
              <th scope="col" class="py-2.5 px-3 w-28">Ср. скорость</th>
              <th scope="col" class="py-2.5 px-3">Детали / Ошибка</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-100 dark:divide-slate-850">
            {#if runsLoading}
              <tr>
                <td colspan="8" class="py-12 text-center text-slate-400">
                  <div class="flex flex-col items-center gap-2">
                    <div class="w-6 h-6 border-2 border-indigo-600 border-t-transparent rounded-full animate-spin"></div>
                    <span>Загрузка истории запусков...</span>
                  </div>
                </td>
              </tr>
            {:else if jobRuns.length === 0}
              <tr>
                <td colspan="8" class="py-12 text-center text-slate-400">
                  <div class="flex flex-col items-center gap-2">
                    <History class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                    <span class="font-medium text-slate-700 dark:text-slate-300">История запусков пуста</span>
                    <span class="text-[11px] text-slate-400">Запустите задачу вручную или дождитесь срабатывания шедулера</span>
                  </div>
                </td>
              </tr>
            {:else}
              {#each jobRuns as run (run.id)}
                <tr class="hover:bg-slate-50/70 dark:hover:bg-slate-850/50 transition-colors">
                  <!-- Номер запуска -->
                  <td class="py-2.5 px-3 font-mono font-bold text-slate-700 dark:text-slate-300">
                    #{run.run_number}
                  </td>

                  <!-- Триггер -->
                  <td class="py-2.5 px-3">
                    {#if run.trigger_type === 'SCHEDULED'}
                      <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-indigo-50 dark:bg-indigo-950/50 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800">
                        <Calendar class="w-3 h-3" />
                        По расписанию
                      </span>
                    {:else}
                      <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700">
                        <Play class="w-2.5 h-2.5" />
                        Вручную
                      </span>
                    {/if}
                  </td>

                  <!-- Статус -->
                  <td class="py-2.5 px-3">
                    <StatusBadge status={run.status} />
                  </td>

                  <!-- Тайминги -->
                  <td class="py-2.5 px-3 font-mono text-[11px] text-slate-600 dark:text-slate-300">
                    <div>{formatTime(run.started_at || run.created_at)}</div>
                    {#if run.completed_at}
                      <div class="text-[10px] text-slate-400">до {formatTime(run.completed_at)}</div>
                    {/if}
                  </td>

                  <!-- Длительность -->
                  <td class="py-2.5 px-3 font-mono text-slate-700 dark:text-slate-300">
                    {formatDuration(run.duration_seconds)}
                  </td>

                  <!-- Объем переданных данных -->
                  <td class="py-2.5 px-3 font-mono text-[11px] text-slate-700 dark:text-slate-300">
                    <div>{formatBytes(run.copied_bytes)}</div>
                    <div class="text-[10px] text-slate-400">из {formatBytes(run.total_bytes)}</div>
                    {#if (run.total_objects ?? 0) > 0}
                      <div class="text-[9px] text-teal-600 dark:text-teal-400 font-medium">
                        {(run.transferred_objects ?? 0) + (run.skipped_objects ?? 0)}/{run.total_objects} объектов
                      </div>
                    {/if}
                  </td>

                  <!-- Средняя скорость -->
                  <td class="py-2.5 px-3 font-mono text-slate-700 dark:text-slate-300">
                    {#if run.average_speed_mb_s !== undefined && run.average_speed_mb_s > 0}
                      <span class="font-semibold text-sky-600 dark:text-sky-400">
                        {run.average_speed_mb_s.toFixed(1)} МБ/с
                      </span>
                    {:else}
                      <span class="text-slate-400">—</span>
                    {/if}
                  </td>

                  <!-- Детали и ошибки -->
                  <td class="py-2.5 px-3 max-w-xs truncate">
                    {#if run.error_message}
                      <span class="text-rose-600 dark:text-rose-400 font-medium truncate block" title={run.error_message}>
                        {run.error_message}
                      </span>
                    {:else if run.message}
                      <span class="text-slate-500 truncate block" title={run.message}>
                        {run.message}
                      </span>
                    {:else if run.triggered_by}
                      <span class="text-slate-400 text-[10px]">автор: {run.triggered_by}</span>
                    {:else}
                      <span class="text-slate-400 text-[10px]">ОК</span>
                    {/if}
                  </td>
                </tr>
              {/each}
            {/if}
          </tbody>
        </table>
      </div>

      <!-- Кнопка закрытия -->
      <div class="flex items-center justify-between pt-2 border-t border-slate-100 dark:border-slate-800">
        <span class="text-[11px] text-slate-400">
          Отображаются последние {jobRuns.length} запусков (лимит хранения: {job.history_retention_runs ?? 20})
        </span>
        <button
          type="button"
          onclick={onClose}
          class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-800 text-xs font-semibold hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
        >
          Закрыть
        </button>
      </div>
    </div>
  </div>
{/if}
