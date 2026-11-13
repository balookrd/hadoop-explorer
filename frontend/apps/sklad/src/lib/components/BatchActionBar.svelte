<script lang="ts">
  import { explorerStore } from '../stores/explorer.svelte';
  import { api } from '../api/client';
  import { formatBytes } from '../utils/format';
  import { FolderDown, Trash2, X, Loader2 } from 'lucide-svelte';

  interface Props {
    onOpenBatchDelete: () => void;
  }

  let { onOpenBatchDelete }: Props = $props();

  let isDownloading = $state(false);

  async function handleBatchDownload() {
    if (!explorerStore.currentCluster || explorerStore.selectedFullPaths.length === 0) return;
    isDownloading = true;

    try {
      const blob = await api.batchDownload(
        explorerStore.currentCluster.id,
        explorerStore.selectedFullPaths
      );

      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      a.download = `hdfs-batch-${explorerStore.currentCluster.id}-${timestamp}.zip`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err: any) {
      alert(err.message || 'Ошибка скачивания архива');
    } finally {
      isDownloading = false;
    }
  }
</script>

{#if explorerStore.selectedFileNames.length > 0}
  <aside
    aria-label="Панель пакетных действий"
    class="fixed bottom-6 left-1/2 -translate-x-1/2 z-40 bg-white/95 dark:bg-slate-900/95 text-slate-800 dark:text-white backdrop-blur-md px-4 py-2.5 rounded-2xl shadow-xl border border-slate-200 dark:border-slate-800 flex items-center gap-3 select-none animate-in fade-in slide-in-from-bottom-4 duration-200"
  >
    <!-- Бейдж количества и размера -->
    <div class="flex items-center gap-2 pr-1 text-xs">
      <span class="flex items-center justify-center w-5 h-5 rounded-md bg-sky-50 dark:bg-sky-500/20 text-sky-600 dark:text-sky-400 border border-sky-200 dark:border-sky-500/30 font-mono font-bold text-[11px]">
        {explorerStore.selectedFileNames.length}
      </span>
      <span class="font-medium text-slate-700 dark:text-slate-200">
        выбрано
        <span class="text-slate-500 dark:text-slate-400 text-[11px] font-mono ml-1">
          ({formatBytes(explorerStore.selectedTotalSize)})
        </span>
      </span>
    </div>

    <div class="h-4 w-px bg-slate-200 dark:bg-slate-700 shrink-0"></div>

    <!-- Кнопка Скачать ZIP -->
    <button
      type="button"
      onclick={handleBatchDownload}
      disabled={isDownloading}
      class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-100 hover:bg-slate-200 active:bg-slate-300 dark:bg-slate-800 dark:hover:bg-slate-700 dark:active:bg-slate-600 text-slate-700 dark:text-slate-200 text-xs font-semibold transition cursor-pointer border border-slate-200 dark:border-slate-700 disabled:opacity-50 shadow-2xs"
      title="Скачать выбранные файлы и папки единым ZIP-архивом"
    >
      {#if isDownloading}
        <Loader2 class="w-3.5 h-3.5 animate-spin text-sky-600 dark:text-sky-400" />
        <span>Упаковка ZIP...</span>
      {:else}
        <FolderDown class="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400" />
        <span>Скачать ZIP</span>
      {/if}
    </button>

    <!-- Кнопка Удалить -->
    <button
      type="button"
      onclick={onOpenBatchDelete}
      disabled={!explorerStore.canWrite}
      class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-red-50 hover:bg-red-100 active:bg-red-200 dark:bg-red-950/80 dark:hover:bg-red-900 border border-red-200 dark:border-red-800/80 text-red-700 hover:text-red-800 dark:text-red-200 dark:hover:text-white text-xs font-semibold transition cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed shadow-2xs"
      title={explorerStore.canWrite ? 'Удалить все выбранные объекты' : 'Кластер только для чтения'}
    >
      <Trash2 class="w-3.5 h-3.5 text-red-600 dark:text-red-400" />
      <span>Удалить</span>
    </button>

    <div class="h-4 w-px bg-slate-200 dark:bg-slate-700 shrink-0"></div>

    <!-- Сброс выделения -->
    <button
      type="button"
      onclick={() => explorerStore.clearSelection()}
      class="p-1 rounded-lg text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
      title="Снять выделение (Esc)"
    >
      <X class="w-4 h-4" />
    </button>
  </aside>
{/if}
