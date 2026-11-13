<script lang="ts">
  import { api } from '../../api/client';
  import { explorerStore } from '../../stores/explorer.svelte';
  import type { HdfsFileStatus, ClusterPublicInfo } from '../../types';
  import { formatBytes } from '../../utils/format';
  import { X, ArrowRightLeft, Folder, File as FileIcon, CheckCircle2, AlertCircle, Loader2 } from 'lucide-svelte';

  interface Props {
    file: HdfsFileStatus | null;
    onClose: () => void;
  }

  let { file, onClose }: Props = $props();

  let targetClusterId = $state<string>('');
  let targetPath = $state<string>('');
  let overwrite = $state<boolean>(false);
  let copying = $state<boolean>(false);
  let error = $state<string | null>(null);
  let successMessage = $state<string | null>(null);

  // Доступные кластеры для копирования (куда можно писать)
  let writableClusters = $derived(
    explorerStore.clusters.filter((c: ClusterPublicInfo) => !c.is_read_only)
  );

  let sourceFullPath = $derived.by(() => {
    if (!file) return '';
    const cur = explorerStore.currentPath;
    return cur === '/' ? `/${file.pathSuffix}` : `${cur}/${file.pathSuffix}`;
  });

  $effect(() => {
    if (file) {
      error = null;
      successMessage = null;
      copying = false;
      // По умолчанию целевой каталог - текущий путь
      targetPath = explorerStore.currentPath;

      // Выбираем кластер, отличный от текущего
      const otherCluster = writableClusters.find(
        (c: ClusterPublicInfo) => c.id !== explorerStore.currentCluster?.id
      ) || writableClusters[0];

      targetClusterId = otherCluster ? otherCluster.id : '';
    }
  });

  async function handleCopy() {
    if (!file || !explorerStore.currentCluster || !targetClusterId) return;

    copying = true;
    error = null;
    successMessage = null;

    try {
      const res = await api.copyCrossCluster({
        source_cluster_id: explorerStore.currentCluster.id,
        source_path: sourceFullPath,
        target_cluster_id: targetClusterId,
        target_path: targetPath || '/',
        overwrite,
      });

      successMessage = res.message;
      setTimeout(() => {
        if (targetClusterId === explorerStore.currentCluster?.id) {
          explorerStore.refresh();
        }
        onClose();
      }, 1500);
    } catch (err: any) {
      error = err.message || 'Ошибка копирования между кластерами';
    } finally {
      copying = false;
    }
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && file) onClose(); }} />

{#if file}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 overflow-y-auto bg-slate-900/50 dark:bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) onClose(); }}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-xl max-w-lg w-full p-5 sm:p-6 border border-slate-200 dark:border-slate-800"
      onclick={(e) => e.stopPropagation()}
    >
      <!-- Header -->
      <div class="flex items-center justify-between pb-3.5 border-b border-slate-100 dark:border-slate-800">
        <div class="flex items-center gap-2 text-slate-900 dark:text-slate-100 font-bold text-sm">
          <ArrowRightLeft class="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
          <span>Копирование в другой кластер</span>
        </div>
        <button onclick={onClose} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded-lg cursor-pointer">
          <X class="w-4 h-4" />
        </button>
      </div>

      <!-- Messages -->
      {#if error}
        <div class="mt-3.5 p-3 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 rounded-xl text-xs flex items-start gap-2.5 shadow-2xs">
          <AlertCircle class="w-4 h-4 shrink-0 text-red-500 mt-0.5" />
          <span class="break-words">{error}</span>
        </div>
      {/if}

      {#if successMessage}
        <div class="mt-3.5 p-3 bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-900 text-emerald-700 dark:text-emerald-300 rounded-xl text-xs flex items-center gap-2.5 shadow-2xs">
          <CheckCircle2 class="w-4 h-4 shrink-0 text-emerald-500" />
          <span>{successMessage}</span>
        </div>
      {/if}

      <div class="mt-3.5 space-y-3.5">
        <!-- Source info -->
        <div class="p-3 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs space-y-1 shadow-2xs">
          <div class="text-slate-500 dark:text-slate-400 flex items-center justify-between text-[11px]">
            <span>Источник:</span>
            <span class="font-semibold text-slate-800 dark:text-slate-200">{explorerStore.currentCluster?.name}</span>
          </div>
          <div class="flex items-center gap-2 pt-0.5 font-mono text-slate-800 dark:text-slate-200 text-xs">
            {#if file.type === 'DIRECTORY'}
              <Folder class="w-3.5 h-3.5 text-amber-500 shrink-0" />
            {:else}
              <FileIcon class="w-3.5 h-3.5 text-sky-500 shrink-0" />
            {/if}
            <span class="truncate" title={sourceFullPath}>{sourceFullPath}</span>
            {#if file.type === 'FILE'}
              <span class="text-[11px] text-slate-400 dark:text-slate-500 font-normal ml-auto shrink-0 font-sans">
                ({formatBytes(file.length)})
              </span>
            {/if}
          </div>
        </div>

        <!-- Target cluster selector -->
        <div>
          <label for="target-cluster" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Целевой кластер HDFS
          </label>
          <select
            id="target-cluster"
            bind:value={targetClusterId}
            disabled={copying || writableClusters.length === 0}
            class="w-full px-3 py-2 text-xs bg-slate-50 dark:bg-slate-950 border border-slate-300 dark:border-slate-700 text-slate-900 dark:text-slate-100 rounded-lg focus:outline-none focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500 focus:bg-white dark:focus:bg-slate-900 transition cursor-pointer disabled:bg-slate-100 dark:disabled:bg-slate-800 disabled:cursor-not-allowed shadow-2xs"
          >
            {#if writableClusters.length === 0}
              <option value="" disabled>Нет доступных кластеров для записи</option>
            {/if}
            {#each writableClusters as cl}
              <option value={cl.id}>
                {cl.name} {cl.id === explorerStore.currentCluster?.id ? '(текущий)' : ''}
              </option>
            {/each}
          </select>
        </div>

        <!-- Target directory -->
        <div>
          <label for="target-path" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Каталог назначения в целевом кластере
          </label>
          <input
            id="target-path"
            type="text"
            bind:value={targetPath}
            disabled={copying}
            placeholder="/data"
            class="w-full px-3 py-2 text-xs font-mono bg-slate-50 dark:bg-slate-950 border border-slate-300 dark:border-slate-700 text-slate-900 dark:text-slate-100 rounded-lg focus:outline-none focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500 focus:bg-white dark:focus:bg-slate-900 transition disabled:bg-slate-100 dark:disabled:bg-slate-800 shadow-2xs"
          />
          <p class="text-[11px] text-slate-400 dark:text-slate-500 mt-1">
            Укажите директорию, куда поместить {file.type === 'DIRECTORY' ? 'папку' : 'файл'}.
          </p>
        </div>

        <!-- Overwrite checkbox -->
        <label class="flex items-center gap-2 text-xs text-slate-700 dark:text-slate-300 cursor-pointer select-none pt-0.5">
          <input
            type="checkbox"
            bind:checked={overwrite}
            disabled={copying}
            class="rounded text-indigo-600 border-slate-300 dark:border-slate-700 focus:ring-indigo-500 dark:bg-slate-950"
          />
          <span>Перезаписывать существующие файлы при совпадении имен</span>
        </label>
      </div>

      <!-- Actions -->
      <div class="mt-5 flex items-center justify-end gap-2.5 pt-3.5 border-t border-slate-100 dark:border-slate-800">
        <button
          type="button"
          onclick={onClose}
          disabled={copying}
          class="px-3.5 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 transition cursor-pointer shadow-2xs disabled:opacity-50"
        >
          Отмена
        </button>
        <button
          type="button"
          onclick={handleCopy}
          disabled={copying || !targetClusterId || !targetPath.trim()}
          class="inline-flex items-center gap-1.5 px-4 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-bold rounded-lg shadow-md shadow-indigo-500/20 transition cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {#if copying}
            <Loader2 class="w-3.5 h-3.5 animate-spin" />
            <span>Копирование...</span>
          {:else}
            <ArrowRightLeft class="w-3.5 h-3.5" />
            <span>Скопировать</span>
          {/if}
        </button>
      </div>
    </div>
  </div>
{/if}
