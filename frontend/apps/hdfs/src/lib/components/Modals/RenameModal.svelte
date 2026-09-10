<script lang="ts">
  import { api } from '../../api/client';
  import { explorerStore } from '../../stores/explorer.svelte';
  import type { HdfsFileStatus } from '../../types';
  import { X, Edit2, AlertCircle } from 'lucide-svelte';

  interface Props {
    file: HdfsFileStatus | null;
    onClose: () => void;
  }

  let { file, onClose }: Props = $props();

  let newName = $state<string>('');
  let renaming = $state<boolean>(false);
  let error = $state<string | null>(null);

  $effect(() => {
    if (file) {
      newName = file.pathSuffix;
      error = null;
    }
  });

  async function rename() {
    if (!file || !explorerStore.currentCluster || !newName.trim()) return;

    renaming = true;
    error = null;
    try {
      const src = explorerStore.currentPath === '/'
        ? `/${file.pathSuffix}`
        : `${explorerStore.currentPath}/${file.pathSuffix}`;

      // Если в newName передан абсолютный путь, перемещаем туда, иначе в текущей папке
      const dst = newName.startsWith('/')
        ? newName
        : (explorerStore.currentPath === '/' ? `/${newName}` : `${explorerStore.currentPath}/${newName}`);

      await api.renamePath(explorerStore.currentCluster.id, src, dst);
      await explorerStore.refresh();
      onClose();
    } catch (err: any) {
      error = err.message || 'Ошибка переименования';
    } finally {
      renaming = false;
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
        <div class="flex items-center gap-2 text-slate-900 dark:text-slate-100 font-bold text-sm">
          <Edit2 class="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
          <span>Переименовать / Переместить</span>
        </div>
        <button onclick={onClose} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded-lg cursor-pointer">
          <X class="w-4 h-4" />
        </button>
      </div>

      <form onsubmit={(e) => { e.preventDefault(); rename(); }} class="mt-4">
        {#if error}
          <div class="mb-3 p-3 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 rounded-xl text-xs flex items-center gap-2 shadow-2xs">
            <AlertCircle class="w-4 h-4 shrink-0 text-red-500" />
            <span>{error}</span>
          </div>
        {/if}

        <div>
          <label for="newName" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Новое имя или путь</label>
          <!-- svelte-ignore a11y_autofocus -->
          <input
            type="text"
            id="newName"
            bind:value={newName}
            class="w-full px-3 py-2 border border-slate-300 dark:border-slate-700 rounded-lg text-xs font-mono bg-slate-50 dark:bg-slate-950 focus:bg-white dark:focus:bg-slate-900 focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500 outline-none text-slate-900 dark:text-slate-100 transition shadow-2xs"
            autofocus
          />
          <p class="text-[11px] text-slate-400 dark:text-slate-500 mt-1.5">
            Укажите имя или полный путь (начинающийся со слеша <span class="font-mono text-slate-600 dark:text-slate-300">/</span>) для перемещения.
          </p>
        </div>

        <div class="mt-5 flex items-center justify-end gap-2.5 pt-3.5 border-t border-slate-100 dark:border-slate-800">
          <button
            type="button"
            onclick={onClose}
            disabled={renaming}
            class="px-3.5 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 transition cursor-pointer shadow-2xs disabled:opacity-50"
          >
            Отмена
          </button>
          <button
            type="submit"
            disabled={!newName.trim() || newName === file.pathSuffix || renaming}
            class="px-4 py-1.5 text-xs font-bold text-white bg-indigo-600 hover:bg-indigo-700 rounded-lg shadow-md shadow-indigo-500/20 transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5"
          >
            {#if renaming}
              <div class="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
              <span>Сохранение...</span>
            {:else}
              <span>Сохранить</span>
            {/if}
          </button>
        </div>
      </form>
    </div>
  </div>
{/if}
