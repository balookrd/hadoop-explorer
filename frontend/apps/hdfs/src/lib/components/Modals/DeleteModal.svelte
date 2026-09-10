<script lang="ts">
  import { api } from '../../api/client';
  import { explorerStore } from '../../stores/explorer.svelte';
  import type { HdfsFileStatus } from '../../types';
  import { X, AlertTriangle, AlertCircle } from 'lucide-svelte';

  interface Props {
    file: HdfsFileStatus | null;
    onClose: () => void;
  }

  let { file, onClose }: Props = $props();

  let recursive = $state<boolean>(false);
  let deleting = $state<boolean>(false);
  let error = $state<string | null>(null);

  $effect(() => {
    if (file) {
      recursive = file.type === 'DIRECTORY';
      error = null;
    }
  });

  async function handleDelete() {
    if (!file || !explorerStore.currentCluster) return;

    deleting = true;
    error = null;
    try {
      const p = explorerStore.currentPath === '/'
        ? `/${file.pathSuffix}`
        : `${explorerStore.currentPath}/${file.pathSuffix}`;

      await api.deletePath(explorerStore.currentCluster.id, p, recursive);
      await explorerStore.refresh();
      onClose();
    } catch (err: any) {
      error = err.message || 'Ошибка удаления';
    } finally {
      deleting = false;
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
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-xl max-w-md w-full p-5 sm:p-6 border border-slate-200 dark:border-slate-800"
      onclick={(e) => e.stopPropagation()}
    >
      <div class="flex items-center justify-between pb-3.5 border-b border-slate-100 dark:border-slate-800">
        <div class="flex items-center gap-2 text-red-600 dark:text-red-400 font-bold text-sm">
          <AlertTriangle class="w-4 h-4" />
          <span>Подтверждение удаления</span>
        </div>
        <button onclick={onClose} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded-lg cursor-pointer">
          <X class="w-4 h-4" />
        </button>
      </div>

      <div class="mt-4">
        <p class="text-xs text-slate-600 dark:text-slate-300">
          Вы уверены, что хотите удалить {file.type === 'DIRECTORY' ? 'директорию' : 'файл'}:
        </p>
        <div class="mt-2 p-2.5 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-lg text-xs font-mono text-slate-800 dark:text-slate-200 break-all shadow-2xs">
          {file.pathSuffix}
        </div>

        {#if error}
          <div class="mt-3 p-3 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 rounded-xl text-xs flex items-center gap-2 shadow-2xs">
            <AlertCircle class="w-4 h-4 shrink-0 text-red-500" />
            <span>{error}</span>
          </div>
        {/if}

        {#if file.type === 'DIRECTORY'}
          <div class="mt-3.5 flex items-center gap-2">
            <input
              type="checkbox"
              id="recursive"
              bind:checked={recursive}
              class="rounded border-slate-300 dark:border-slate-700 text-red-600 focus:ring-red-500 dark:bg-slate-950"
            />
            <label for="recursive" class="text-xs text-slate-700 dark:text-slate-300 cursor-pointer select-none">
              Рекурсивное удаление (включая все вложенные файлы и папки)
            </label>
          </div>
        {/if}
      </div>

      <div class="mt-5 flex items-center justify-end gap-2.5 pt-3.5 border-t border-slate-100 dark:border-slate-800">
        <button
          onclick={onClose}
          disabled={deleting}
          class="px-3.5 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 transition cursor-pointer shadow-2xs disabled:opacity-50"
        >
          Отмена
        </button>
        <button
          onclick={handleDelete}
          disabled={deleting}
          class="px-4 py-1.5 text-xs font-bold text-white bg-red-600 hover:bg-red-700 rounded-lg shadow-md shadow-red-500/20 transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5"
        >
          {#if deleting}
            <div class="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
            <span>Удаление...</span>
          {:else}
            <span>Удалить</span>
          {/if}
        </button>
      </div>
    </div>
  </div>
{/if}
