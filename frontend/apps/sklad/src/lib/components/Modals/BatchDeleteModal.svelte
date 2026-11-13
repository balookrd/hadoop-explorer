<script lang="ts">
  import { api } from '../../api/client';
  import { explorerStore } from '../../stores/explorer.svelte';
  import type { BatchDeleteResponse } from '../../types';
  import { formatBytes } from '../../utils/format';
  import { X, AlertTriangle, AlertCircle, Trash2, Folder, File as FileIcon, CheckCircle2 } from 'lucide-svelte';

  interface Props {
    isOpen: boolean;
    onClose: () => void;
  }

  let { isOpen, onClose }: Props = $props();

  let recursive = $state<boolean>(true);
  let deleting = $state<boolean>(false);
  let error = $state<string | null>(null);
  let deleteResult = $state<BatchDeleteResponse | null>(null);

  const selectedItems = $derived(explorerStore.selectedItems);
  const selectedPaths = $derived(explorerStore.selectedFullPaths);
  const totalSize = $derived(explorerStore.selectedTotalSize);
  const hasDirectories = $derived(selectedItems.some(i => i.type === 'DIRECTORY'));

  $effect(() => {
    if (isOpen) {
      recursive = hasDirectories;
      error = null;
      deleteResult = null;
    }
  });

  async function handleBatchDelete() {
    if (!explorerStore.currentCluster || selectedPaths.length === 0) return;

    deleting = true;
    error = null;
    deleteResult = null;

    try {
      const res = await api.batchDelete(
        explorerStore.currentCluster.id,
        selectedPaths,
        recursive
      );

      deleteResult = res;
      await explorerStore.refresh();

      if (res.success && res.failed.length === 0) {
        explorerStore.clearSelection();
        setTimeout(() => {
          onClose();
        }, 500);
      }
    } catch (err: any) {
      error = err.message || 'Ошибка пакетного удаления';
    } finally {
      deleting = false;
    }
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isOpen && !deleting) onClose(); }} />

{#if isOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 overflow-y-auto bg-slate-900/50 dark:bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget && !deleting) onClose(); }}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-xl max-w-lg w-full p-5 sm:p-6 border border-slate-200 dark:border-slate-800 animate-in fade-in zoom-in-95 duration-150"
      onclick={(e) => e.stopPropagation()}
    >
      <div class="flex items-center justify-between pb-3.5 border-b border-slate-100 dark:border-slate-800">
        <div class="flex items-center gap-2 text-red-600 dark:text-red-400 font-bold text-sm">
          <AlertTriangle class="w-4 h-4" />
          <span>Пакетное удаление объектов ({selectedItems.length})</span>
        </div>
        <button
          onclick={onClose}
          disabled={deleting}
          class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded-lg cursor-pointer disabled:opacity-40"
        >
          <X class="w-4 h-4" />
        </button>
      </div>

      <div class="mt-4">
        <p class="text-xs text-slate-600 dark:text-slate-300">
          Вы действительно хотите удалить <strong>{selectedItems.length}</strong> объектов суммарным объёмом <strong>{formatBytes(totalSize)}</strong>?
        </p>

        <!-- Список удаляемых объектов -->
        <div class="mt-3 max-h-48 overflow-y-auto bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl p-2 space-y-1 divide-y divide-slate-100 dark:divide-slate-800/40">
          {#each selectedItems as item}
            <div class="flex items-center justify-between pt-1 text-xs font-mono text-slate-800 dark:text-slate-200">
              <div class="flex items-center gap-2 min-w-0 pr-2">
                {#if item.type === 'DIRECTORY'}
                  <Folder class="w-3.5 h-3.5 text-amber-500 shrink-0" />
                {:else}
                  <FileIcon class="w-3.5 h-3.5 text-sky-500 shrink-0" />
                {/if}
                <span class="truncate">{item.pathSuffix}</span>
              </div>
              <span class="text-[11px] text-slate-400 shrink-0">
                {item.type === 'DIRECTORY' ? 'папка' : formatBytes(item.length)}
              </span>
            </div>
          {/each}
        </div>

        {#if hasDirectories}
          <div class="mt-3.5 flex items-center gap-2">
            <input
              type="checkbox"
              id="batch-recursive"
              bind:checked={recursive}
              class="rounded border-slate-300 dark:border-slate-700 text-red-600 focus:ring-red-500 dark:bg-slate-950 cursor-pointer"
            />
            <label for="batch-recursive" class="text-xs text-slate-700 dark:text-slate-300 cursor-pointer select-none">
              Рекурсивное удаление (включая все вложенные файлы и папки)
            </label>
          </div>
        {/if}

        {#if error}
          <div class="mt-3 p-3 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 rounded-xl text-xs flex items-center gap-2 shadow-2xs">
            <AlertCircle class="w-4 h-4 shrink-0 text-red-500" />
            <span>{error}</span>
          </div>
        {/if}

        <!-- Отчет о частичном выполнении -->
        {#if deleteResult && deleteResult.failed.length > 0}
          <div class="mt-3 p-3 bg-amber-50 dark:bg-amber-950/40 border border-amber-200 dark:border-amber-900 text-amber-800 dark:text-amber-300 rounded-xl text-xs space-y-1">
            <div class="font-bold flex items-center gap-1.5">
              <AlertCircle class="w-3.5 h-3.5 text-amber-600 dark:text-amber-400" />
              <span>Часть объектов не удалось удалить ({deleteResult.failed.length}):</span>
            </div>
            {#each deleteResult.failed as f}
              <div class="text-[11px] font-mono break-all text-red-600 dark:text-red-400">
                • {f.path}: {f.error}
              </div>
            {/each}
          </div>
        {/if}
      </div>

      <div class="mt-5 flex items-center justify-end gap-2.5 pt-3.5 border-t border-slate-100 dark:border-slate-800">
        <button
          onclick={onClose}
          disabled={deleting}
          class="px-3.5 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 transition cursor-pointer shadow-2xs disabled:opacity-50"
        >
          {deleteResult ? 'Закрыть' : 'Отмена'}
        </button>
        {#if !deleteResult || deleteResult.failed.length > 0}
          <button
            onclick={handleBatchDelete}
            disabled={deleting}
            class="px-4 py-1.5 text-xs font-bold text-white bg-red-600 hover:bg-red-700 rounded-lg shadow-md shadow-red-500/20 transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5"
          >
            {#if deleting}
              <div class="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
              <span>Удаление...</span>
            {:else}
              <Trash2 class="w-3.5 h-3.5" />
              <span>Удалить всё ({selectedItems.length})</span>
            {/if}
          </button>
        {/if}
      </div>
    </div>
  </div>
{/if}
