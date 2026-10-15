<script lang="ts">
  import type { Job, TopologyData } from '../types';
  import { Pencil, X, AlertCircle, Calendar, History } from 'lucide-svelte';

  interface Props {
    isOpen: boolean;
    job: Job | null;
    topology: TopologyData | null;
    isAdmin: boolean;
    onClose: () => void;
    onSave: (jobId: string, payload: any) => Promise<void>;
  }

  let {
    isOpen,
    job,
    topology,
    isAdmin,
    onClose,
    onSave
  }: Props = $props();

  let editJobSourceCluster = $state('demo-cluster');
  let editJobTargetCluster = $state('backup-cluster');
  let editJobSourcePath = $state('');
  let editJobTargetPath = $state('');
  let editJobImpersonationUser = $state('');
  let editJobIsScheduled = $state(false);
  let editJobCronPreset = $state('@every_5m');
  let editJobHistoryRetention = $state<number>(20);
  let editJobError = $state<string | null>(null);
  let isSubmittingEditJob = $state(false);

  $effect(() => {
    if (isOpen && job) {
      editJobSourceCluster = job.source_cluster_id;
      editJobTargetCluster = job.target_cluster_id;
      editJobSourcePath = job.source_path;
      editJobTargetPath = job.target_path;
      const initialUser = job.execution_principal
        ? (job.execution_principal.includes('@') ? job.execution_principal.split('@')[0] : job.execution_principal)
        : (job.created_by || '');
      editJobImpersonationUser = initialUser;
      editJobIsScheduled = job.is_scheduled ?? false;
      editJobCronPreset = job.cron_expression || '@every_5m';
      editJobHistoryRetention = job.history_retention_runs ?? 20;
      editJobError = null;
    }
  });

  async function handleSubmit(e: Event) {
    e.preventDefault();
    if (!job) return;
    editJobError = null;
    isSubmittingEditJob = true;

    try {
      const payload: any = {
        source_cluster_id: editJobSourceCluster,
        target_cluster_id: editJobTargetCluster,
        source_path: editJobSourcePath.trim(),
        target_path: editJobTargetPath.trim(),
        is_scheduled: editJobIsScheduled,
        history_retention_runs: Number(editJobHistoryRetention) || 20,
      };

      if (isAdmin && editJobImpersonationUser.trim()) {
        const u = editJobImpersonationUser.trim();
        payload.execution_principal = u.includes('@') ? u : `${u}@REALM.LOCAL`;
      }

      if (editJobIsScheduled) {
        payload.cron_expression = editJobCronPreset;
      }

      await onSave(job.id, payload);
    } catch (e: any) {
      editJobError = e.message || 'Ошибка связи с сервером';
    } finally {
      isSubmittingEditJob = false;
    }
  }
</script>

{#if isOpen && job}
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) onClose(); }}
  >
    <div
      class="w-full max-w-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[90vh] overflow-y-auto"
    >
      <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-indigo-50 dark:bg-indigo-950/60 border border-indigo-200 dark:border-indigo-800 flex items-center justify-center text-indigo-600 dark:text-indigo-400">
            <Pencil class="w-5 h-5" />
          </div>
          <div>
            <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">Редактирование задачи</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate max-w-xs" title={job.id}>
              ID: {job.id.substring(0, 8)}...
            </p>
          </div>
        </div>
        <button
          onclick={onClose}
          class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded cursor-pointer"
        >
          <X class="w-5 h-5" />
        </button>
      </div>

      {#if editJobError}
        <div class="p-3 bg-rose-50 dark:bg-rose-950/50 border border-rose-200 dark:border-rose-900 rounded-xl flex items-center gap-2.5 text-xs text-rose-700 dark:text-rose-300 font-medium">
          <AlertCircle class="w-4 h-4 shrink-0" />
          <span>{editJobError}</span>
        </div>
      {/if}

      <form onsubmit={handleSubmit} class="flex flex-col gap-4">
        <!-- Кластеры источника и назначения -->
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Исходный кластер (Source)
            </label>
            <select
              bind:value={editJobSourceCluster}
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
            >
              {#if topology && topology.clusters}
                {#each topology.clusters as c}
                  <option value={c.id}>{c.name} [{c.dc_id.toUpperCase()}]</option>
                {/each}
              {:else}
                <option value="demo-cluster">HDFS Primary [DC1]</option>
              {/if}
            </select>
          </div>

          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Целевой кластер (Target)
            </label>
            <select
              bind:value={editJobTargetCluster}
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
            >
              {#if topology && topology.clusters}
                {#each topology.clusters as c}
                  <option value={c.id}>{c.name} [{c.dc_id.toUpperCase()}]</option>
                {/each}
              {:else}
                <option value="backup-cluster">HDFS DR [DC2]</option>
              {/if}
            </select>
          </div>
        </div>

        <!-- Исходный путь в HDFS -->
        <div>
          <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Исходный путь в HDFS (Source Path)
          </label>
          <input
            type="text"
            required
            bind:value={editJobSourcePath}
            placeholder="/data/production/events/2026-10"
            class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
          />
        </div>

        <!-- Целевой путь в HDFS -->
        <div>
          <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Целевой путь назначения (Target Path)
          </label>
          <input
            type="text"
            required
            bind:value={editJobTargetPath}
            placeholder="/backup/mirror/events/2026-10"
            class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
          />
        </div>

        <!-- Пользователь для имперсонации (doAs) -->
        <div>
          <label for="edit-impersonation-user" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Пользователь имперсонации HDFS (doAs username)
          </label>
          <div class="relative">
            {#if isAdmin}
              <input
                id="edit-impersonation-user"
                type="text"
                bind:value={editJobImpersonationUser}
                placeholder="hdfs"
                class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
              <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-sky-100 dark:bg-sky-900/60 text-sky-700 dark:text-sky-300 font-semibold">
                admin
              </span>
            {:else}
              <input
                id="edit-impersonation-user"
                type="text"
                value={editJobImpersonationUser}
                disabled
                class="w-full px-3 py-2 bg-slate-100 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-mono text-slate-500 dark:text-slate-400 cursor-not-allowed"
              />
              <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-300 font-semibold">
                doAs
              </span>
            {/if}
          </div>
          <p class="text-[10px] text-slate-400 mt-1">
            {#if isAdmin}
              Администратор может переопределить учетную запись Kerberos doAs имперсонации.
            {:else}
              Имперсонация зафиксирована за создателем задачи.
            {/if}
          </p>
        </div>

        <!-- Планировщик периодических задач (Cron Scheduler) -->
        <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-2.5">
          <label class="flex items-center gap-2 text-xs font-semibold text-slate-700 dark:text-slate-300 cursor-pointer select-none">
            <input
              type="checkbox"
              bind:checked={editJobIsScheduled}
              class="rounded border-slate-300 text-sky-600 focus:ring-sky-500"
            />
            <span class="flex items-center gap-1.5">
              <Calendar class="w-3.5 h-3.5 text-indigo-500" />
              Запуск по расписанию (Cron)
            </span>
          </label>

          {#if editJobIsScheduled}
            <div class="space-y-1.5 pt-1">
              <label class="block text-[11px] font-medium text-slate-500">Интервал повторения:</label>
              <select
                bind:value={editJobCronPreset}
                class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono"
              >
                <option value="@every_5m">Каждые 5 минут (@every_5m)</option>
                <option value="@every_15m">Каждые 15 минут (@every_15m)</option>
                <option value="@hourly">Каждый час (@hourly / 0 * * * *)</option>
                <option value="@daily">Раз в сутки в полночь (@daily / 0 0 * * *)</option>
              </select>
            </div>
          {/if}
        </div>

        <!-- Глубина истории запусков -->
        <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-1.5">
          <div class="flex items-center justify-between">
            <label for="edit-job-retention" class="text-xs font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
              <History class="w-3.5 h-3.5 text-indigo-500" />
              <span>Глубина истории запусков:</span>
            </label>
            <span class="text-xs font-mono font-bold text-indigo-600 dark:text-indigo-400">
              {editJobHistoryRetention} запусков
            </span>
          </div>
          <div class="flex items-center gap-2">
            <input
              id="edit-job-retention"
              type="number"
              min="1"
              max="500"
              bind:value={editJobHistoryRetention}
              class="w-32 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
            <span class="text-[11px] text-slate-500">лимит хранения (1–500)</span>
          </div>
          <p class="text-[10px] text-slate-400">
            При сохранении старые запуски сверх лимита будут автоматически очищены.
          </p>
        </div>

        <!-- Кнопка отправки -->
        <div class="flex items-center justify-end gap-3 pt-2">
          <button
            type="button"
            onclick={onClose}
            class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-800 text-xs font-semibold hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
          >
            Отмена
          </button>
          <button
            type="submit"
            disabled={isSubmittingEditJob}
            class="px-5 py-2 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-md shadow-sky-600/20 transition cursor-pointer disabled:opacity-50"
          >
            {isSubmittingEditJob ? 'Сохранение...' : 'Сохранить изменения'}
          </button>
        </div>
      </form>
    </div>
  </div>
{/if}
