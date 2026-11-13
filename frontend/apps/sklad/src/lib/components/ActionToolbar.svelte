<script lang="ts">
  import { explorerStore } from '../stores/explorer.svelte';
  import { api } from '../api/client';
  import { formatBytes } from '../utils/format';
  import { Upload, FolderPlus, RefreshCw, Search, HardDrive, FolderDown } from 'lucide-svelte';

  interface Props {
    onOpenUpload: () => void;
    onOpenMkdir: () => void;
  }

  let { onOpenUpload, onOpenMkdir }: Props = $props();

  function getCurrentDirDownloadUrl() {
    if (!explorerStore.currentCluster) return '#';
    return api.getDownloadUrl(explorerStore.currentCluster.id, explorerStore.currentPath);
  }
</script>

<div class="h-12 bg-slate-50 dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 px-4 sm:px-6 flex items-center justify-between select-none shrink-0">
  <div class="w-full flex items-center justify-between gap-3">
    <!-- Action buttons -->
    <div class="flex items-center space-x-2">
      <button
        onclick={onOpenUpload}
        disabled={!explorerStore.canWrite}
        class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold rounded-lg shadow-md shadow-sky-500/20 transition cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
        title={explorerStore.canWrite ? 'Загрузить файлы или папку' : 'Кластер только для чтения'}
      >
        <Upload class="w-3.5 h-3.5" />
        <span>Загрузить</span>
      </button>

      <button
        onclick={onOpenMkdir}
        disabled={!explorerStore.canWrite}
        class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-white dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 text-xs font-medium rounded-lg transition border border-slate-200 dark:border-slate-700 shadow-2xs cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
        title="Создать новую директорию"
      >
        <FolderPlus class="w-3.5 h-3.5 text-sky-600 dark:text-sky-400" />
        <span>Новая папка</span>
      </button>

      {#if explorerStore.currentCluster && explorerStore.currentPath !== '/'}
        <a
          href={getCurrentDirDownloadUrl()}
          download
          class="p-1.5 text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 hover:text-emerald-600 dark:hover:text-emerald-400 rounded-lg border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-800 shadow-2xs transition cursor-pointer"
          title="Скачать текущую папку (ZIP-архив)"
        >
          <FolderDown class="w-3.5 h-3.5" />
        </a>
      {/if}

      <button
        onclick={() => explorerStore.refresh()}
        class="p-1.5 text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-800 shadow-2xs transition cursor-pointer"
        title="Обновить список"
      >
        <RefreshCw class={`w-3.5 h-3.5 ${explorerStore.loading ? 'animate-spin text-sky-600 dark:text-sky-400' : ''}`} />
      </button>
    </div>

    <!-- Search / Filter & Stats -->
    <div class="flex items-center space-x-3 justify-end">
      <div class="relative w-48 sm:w-60">
        <Search class="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-2" />
        <input
          type="text"
          bind:value={explorerStore.searchQuery}
          placeholder="Фильтр файлов..."
          class="w-full pl-9 pr-2.5 py-1 text-xs bg-white dark:bg-slate-950 border border-slate-300 dark:border-slate-700 rounded-lg focus:outline-none focus:ring-1 focus:ring-sky-500 focus:border-sky-500 text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 shadow-2xs transition"
        />
      </div>

      <div class="hidden md:flex items-center text-xs text-slate-500 dark:text-slate-400 space-x-2 shrink-0 bg-white dark:bg-slate-800 px-2.5 py-1 rounded-lg border border-slate-200 dark:border-slate-700 shadow-2xs">
        <span>Папок: <strong class="text-slate-700 dark:text-slate-200">{explorerStore.totalDirs}</strong></span>
        <span class="text-slate-300 dark:text-slate-600">•</span>
        <span>Файлов: <strong class="text-slate-700 dark:text-slate-200">{explorerStore.totalFiles}</strong></span>
        <span class="text-slate-300 dark:text-slate-600">•</span>
        <span class="flex items-center gap-1 font-mono text-slate-700 dark:text-slate-200">
          <HardDrive class="w-3 h-3 text-slate-400 dark:text-slate-500" />
          {formatBytes(explorerStore.totalSize)}
        </span>
      </div>
    </div>
  </div>
</div>
