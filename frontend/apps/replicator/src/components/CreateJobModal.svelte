<script lang="ts">
  import type { TopologyData, SyncMode } from '../types';
  import type { UserSession } from '@hadoop-explorer/common';
  import { Plus, X, AlertCircle, Shield, History } from 'lucide-svelte';

  interface Props {
    isOpen: boolean;
    topology: TopologyData | null;
    user: UserSession | null;
    isAdmin: boolean;
    isStreamingAvailable: boolean;
    onClose: () => void;
    onCreate: (payload: any) => Promise<void>;
  }

  let {
    isOpen,
    topology,
    user,
    isAdmin,
    isStreamingAvailable,
    onClose,
    onCreate
  }: Props = $props();

  let newJobSourceCluster = $state('demo-cluster');
  let newJobTargetCluster = $state('backup-cluster');
  let newJobSourcePath = $state('/data/production/events/2026-10');
  let newJobTargetPath = $state('/backup/mirror/events/2026-10');
  let newJobImpersonationUser = $state('');
  let newJobSyncMode = $state<SyncMode>('MANUAL');
  let newJobCronPreset = $state('@every_5m');
  let newJobHistoryRetention = $state<number>(20);
  let createJobError = $state<string | null>(null);
  let isSubmittingJob = $state(false);

  $effect(() => {
    if (isOpen) {
      newJobImpersonationUser = user?.username || '';
      createJobError = null;
      if (topology?.clusters && topology.clusters.length > 0) {
        const dc1 = topology.clusters.find((c) => c.dc_id === 'dc1' || c.id === 'dc1');
        newJobSourceCluster = dc1 ? dc1.id : topology.clusters[0].id;
        const dc2 = topology.clusters.find((c) => c.dc_id === 'dc2' || c.id === 'dc2');
        newJobTargetCluster = dc2 ? dc2.id : (topology.clusters.length > 1 ? topology.clusters[1].id : topology.clusters[0].id);
      }
    }
  });

  async function handleSubmit(e: Event) {
    e.preventDefault();
    createJobError = null;
    isSubmittingJob = true;

    try {
      const chosenUser = (isAdmin && newJobImpersonationUser.trim())
        ? newJobImpersonationUser.trim()
        : (user?.username || 'writer_user');
      const executionPrincipal = chosenUser.includes('@') ? chosenUser : `${chosenUser}@REALM.LOCAL`;
      const isScheduled = newJobSyncMode === 'SCHEDULED';

      const payload: any = {
        source_cluster_id: newJobSourceCluster,
        target_cluster_id: newJobTargetCluster,
        source_path: newJobSourcePath.trim(),
        target_path: newJobTargetPath.trim(),
        run_as_service_account: false,
        execution_principal: executionPrincipal,
        is_scheduled: isScheduled,
        sync_mode: newJobSyncMode,
        history_retention_runs: Number(newJobHistoryRetention) || 20,
      };

      if (isScheduled) {
        payload.cron_expression = newJobCronPreset;
      }

      await onCreate(payload);
    } catch (e: any) {
      createJobError = e.message || 'Ошибка связи с сервером';
    } finally {
      isSubmittingJob = false;
    }
  }
</script>

{#if isOpen}
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) onClose(); }}
  >
    <div
      class="w-full max-w-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[90vh] overflow-y-auto"
    >
      <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-sky-50 dark:bg-sky-950/60 border border-sky-200 dark:border-sky-800 flex items-center justify-center text-sky-600 dark:text-sky-400">
            <Plus class="w-5 h-5" />
          </div>
          <div>
            <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">Новая задача репликации</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400">Межкластерная передача файлов и каталогов по gRPC</p>
          </div>
        </div>
        <button
          onclick={onClose}
          class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded cursor-pointer"
        >
          <X class="w-5 h-5" />
        </button>
      </div>

      {#if createJobError}
        <div class="p-3 bg-rose-50 dark:bg-rose-950/50 border border-rose-200 dark:border-rose-900 rounded-xl flex items-center gap-2.5 text-xs text-rose-700 dark:text-rose-300 font-medium">
          <AlertCircle class="w-4 h-4 shrink-0" />
          <span>{createJobError}</span>
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
              bind:value={newJobSourceCluster}
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
              bind:value={newJobTargetCluster}
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
            bind:value={newJobSourcePath}
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
            bind:value={newJobTargetPath}
            placeholder="/backup/mirror/events/2026-10"
            class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
          />
        </div>

        <!-- Пользователь для имперсонации (doAs) и аудит Ranger -->
        <div>
          <label for="create-impersonation-user" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Пользователь имперсонации HDFS (doAs username)
          </label>
          <div class="relative">
            {#if isAdmin}
              <input
                id="create-impersonation-user"
                type="text"
                bind:value={newJobImpersonationUser}
                placeholder={user?.username || 'hdfs'}
                class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
              <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-sky-100 dark:bg-sky-900/60 text-sky-700 dark:text-sky-300 font-semibold">
                admin
              </span>
            {:else}
              <input
                id="create-impersonation-user"
                type="text"
                value={user?.username || 'текущий пользователь'}
                disabled
                class="w-full px-3 py-2 bg-slate-100 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-mono text-slate-500 dark:text-slate-400 cursor-not-allowed"
              />
              <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-300 font-semibold">
                doAs
              </span>
            {/if}
          </div>
          <div class="mt-1.5 p-2.5 bg-sky-50/60 dark:bg-sky-950/25 border border-sky-200/70 dark:border-sky-800/50 rounded-xl flex items-start gap-2">
            <Shield class="w-3.5 h-3.5 text-sky-600 dark:text-sky-400 mt-0.5 shrink-0" />
            <div class="text-[11px] text-slate-500 dark:text-slate-400 leading-relaxed">
              {#if isAdmin}
                Администратор может указать username для выполнения HDFS репликации от его имени через Hadoop Proxy User.
              {:else}
                Доступ к HDFS выполняется от вашего имени <strong class="font-mono text-slate-700 dark:text-slate-300">({user?.username || 'текущий пользователь'})</strong> через Kerberos Proxy User.
              {/if}
            </div>
          </div>
        </div>

        <!-- Режим синхронизации (Sync Mode) -->
        <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-3">
          <div>
            <span class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Режим синхронизации</span>
            <p class="text-[11px] text-slate-500 dark:text-slate-400">Выберите способ запуска и частоту репликации данных</p>
          </div>

          <div class="grid grid-cols-1 sm:grid-cols-3 gap-2">
            <!-- Режим 1: Ручной -->
            <label class="flex flex-col p-2.5 rounded-xl border cursor-pointer transition select-none {newJobSyncMode === 'MANUAL'
              ? 'bg-sky-50/70 dark:bg-sky-950/50 border-sky-400 dark:border-sky-600 ring-1 ring-sky-500/30'
              : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300'}">
              <div class="flex items-center gap-2 mb-1">
                <input
                  type="radio"
                  name="sync_mode"
                  value="MANUAL"
                  bind:group={newJobSyncMode}
                  class="text-sky-600 focus:ring-sky-500"
                />
                <span class="text-xs font-bold text-slate-800 dark:text-slate-200">Ручной запуск</span>
              </div>
              <span class="text-[10px] text-slate-500">Однократная репликация по кнопке Play</span>
            </label>

            <!-- Режим 2: По расписанию (Cron) -->
            <label class="flex flex-col p-2.5 rounded-xl border cursor-pointer transition select-none {newJobSyncMode === 'SCHEDULED'
              ? 'bg-indigo-50/70 dark:bg-indigo-950/50 border-indigo-400 dark:border-indigo-600 ring-1 ring-indigo-500/30'
              : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300'}">
              <div class="flex items-center gap-2 mb-1">
                <input
                  type="radio"
                  name="sync_mode"
                  value="SCHEDULED"
                  bind:group={newJobSyncMode}
                  class="text-indigo-600 focus:ring-indigo-500"
                />
                <span class="text-xs font-bold text-slate-800 dark:text-slate-200">По расписанию</span>
              </div>
              <span class="text-[10px] text-slate-500">Cron-шедулер оркестратора</span>
            </label>

            <!-- Режим 3: Потоковый Inotify -->
            <label class="flex flex-col p-2.5 rounded-xl border transition select-none {isStreamingAvailable ? 'cursor-pointer' : 'cursor-not-allowed opacity-60'} {newJobSyncMode === 'STREAMING_INOTIFY'
              ? 'bg-purple-50/70 dark:bg-purple-950/50 border-purple-400 dark:border-purple-600 ring-1 ring-purple-500/30'
              : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300'}">
              <div class="flex items-center gap-2 mb-1">
                <input
                  type="radio"
                  name="sync_mode"
                  value="STREAMING_INOTIFY"
                  disabled={!isStreamingAvailable}
                  bind:group={newJobSyncMode}
                  class="text-purple-600 focus:ring-purple-500 disabled:opacity-50"
                />
                <span class="text-xs font-bold text-slate-800 dark:text-slate-200 flex items-center gap-1">
                  <span>Live Inotify</span>
                  <span class="text-[9px] px-1 py-0.2 rounded bg-purple-100 dark:bg-purple-900/60 text-purple-700 dark:text-purple-300 font-mono">Near-0 RPO</span>
                </span>
              </div>
              <span class="text-[10px] text-slate-500">
                {isStreamingAvailable ? 'Потоковое чтение событий NameNode' : 'Отключено (streaming.enabled=false)'}
              </span>
            </label>
          </div>

          {#if newJobSyncMode === 'SCHEDULED'}
            <div class="space-y-1.5 pt-2 border-t border-slate-200/60 dark:border-slate-800">
              <label class="block text-[11px] font-medium text-slate-500">Интервал повторения:</label>
              <select
                bind:value={newJobCronPreset}
                class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono"
              >
                <option value="@every_5m">Каждые 5 минут (@every_5m)</option>
                <option value="@every_15m">Каждые 15 минут (@every_15m)</option>
                <option value="@hourly">Каждый час (@hourly / 0 * * * *)</option>
                <option value="@daily">Раз в сутки в полночь (@daily / 0 0 * * *)</option>
              </select>
            </div>
          {:else if newJobSyncMode === 'STREAMING_INOTIFY'}
            <div class="p-2.5 rounded-lg bg-purple-50/50 dark:bg-purple-950/20 border border-purple-200/60 dark:border-purple-800/40 text-[11px] text-purple-800 dark:text-purple-300 space-y-1">
              <div class="font-semibold flex items-center gap-1.5">
                <span class="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
                <span>Режим непрерывного потока HDFS EditLog (Near-Zero RPO)</span>
              </div>
              <p class="text-[10px] text-slate-500 dark:text-slate-400">
                Стример перехватывает операции Create/Close/Rename в реальном времени. Временные staging-каталоги игнорируются до финального перемещения.
              </p>
            </div>
          {/if}
        </div>

        <!-- Глубина истории запусков -->
        <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-1.5">
          <div class="flex items-center justify-between">
            <label for="new-job-retention" class="text-xs font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
              <History class="w-3.5 h-3.5 text-indigo-500" />
              <span>Глубина истории запусков:</span>
            </label>
            <span class="text-xs font-mono font-bold text-indigo-600 dark:text-indigo-400">
              {newJobHistoryRetention} запусков
            </span>
          </div>
          <div class="flex items-center gap-2">
            <input
              id="new-job-retention"
              type="number"
              min="1"
              max="500"
              bind:value={newJobHistoryRetention}
              class="w-32 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
            <span class="text-[11px] text-slate-500">лимит хранения (1–500, по умолчанию 20)</span>
          </div>
          <p class="text-[10px] text-slate-400">
            Старые запуски сверх указанного лимита автоматически очищаются для экономии места.
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
            disabled={isSubmittingJob}
            class="px-5 py-2 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-md shadow-sky-600/20 transition cursor-pointer disabled:opacity-50"
          >
            {isSubmittingJob ? 'Создание...' : 'Создать задачу'}
          </button>
        </div>
      </form>
    </div>
  </div>
{/if}
