<script lang="ts">
  import { api } from '../../api/client';
  import { explorerStore } from '../../stores/explorer.svelte';
  import { X, FolderPlus, AlertCircle } from 'lucide-svelte';

  interface Props {
    isOpen: boolean;
    onClose: () => void;
  }

  let { isOpen, onClose }: Props = $props();

  let dirName = $state<string>('');
  let creating = $state<boolean>(false);
  let error = $state<string | null>(null);

  async function create() {
    const name = dirName.trim();
    if (!name || !explorerStore.currentCluster) return;

    creating = true;
    error = null;
    try {
      const fullPath = explorerStore.currentPath === '/'
        ? `/${name}`
        : `${explorerStore.currentPath}/${name}`;

      await api.createDirectory(explorerStore.currentCluster.id, fullPath);
      await explorerStore.refresh();
      dirName = '';
      onClose();
    } catch (err: any) {
      error = err.message || 'Ошибка создания директории';
    } finally {
      creating = false;
    }
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isOpen) onClose(); }} />

{#if isOpen}
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
        <div class="flex items-center gap-2 text-slate-900 dark:text-slate-100 font-bold text-sm">
          <FolderPlus class="w-4 h-4 text-sky-600 dark:text-sky-400" />
          <span>Новая директория</span>
        </div>
        <button onclick={onClose} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded-lg cursor-pointer">
          <X class="w-4 h-4" />
        </button>
      </div>

      <form onsubmit={(e) => { e.preventDefault(); create(); }} class="mt-4">
        <div class="text-xs text-slate-500 dark:text-slate-400 mb-3 font-mono">
          Создание внутри: <span class="text-slate-800 dark:text-slate-200 font-semibold">{explorerStore.currentPath}</span>
        </div>

        {#if error}
          <div class="mb-3 p-3 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 rounded-xl text-xs flex items-center gap-2 shadow-2xs">
            <AlertCircle class="w-4 h-4 shrink-0 text-red-500" />
            <span>{error}</span>
          </div>
        {/if}

        <div>
          <label for="dirName" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Имя папки</label>
          <!-- svelte-ignore a11y_autofocus -->
          <input
            type="text"
            id="dirName"
            bind:value={dirName}
            placeholder="например, logs_2026"
            class="w-full px-3 py-2 border border-slate-300 dark:border-slate-700 rounded-lg text-xs bg-slate-50 dark:bg-slate-950 focus:bg-white dark:focus:bg-slate-900 focus:ring-1 focus:ring-sky-500 focus:border-sky-500 outline-none font-mono text-slate-900 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 transition shadow-2xs"
            autofocus
          />
        </div>

        <div class="mt-5 flex items-center justify-end gap-2.5 pt-3.5 border-t border-slate-100 dark:border-slate-800">
          <button
            type="button"
            onclick={onClose}
            disabled={creating}
            class="px-3.5 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 transition cursor-pointer shadow-2xs disabled:opacity-50"
          >
            Отмена
          </button>
          <button
            type="submit"
            disabled={!dirName.trim() || creating}
            class="px-4 py-1.5 text-xs font-bold text-white bg-sky-600 hover:bg-sky-500 rounded-lg shadow-md shadow-sky-500/20 transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5"
          >
            {#if creating}
              <div class="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
              <span>Создание...</span>
            {:else}
              <span>Создать</span>
            {/if}
          </button>
        </div>
      </form>
    </div>
  </div>
{/if}
