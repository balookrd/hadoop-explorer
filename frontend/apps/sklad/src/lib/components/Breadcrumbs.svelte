<script lang="ts">
  import { explorerStore } from '../stores/explorer.svelte';
  import { ChevronRight, Home, ArrowUp, Edit2, Check, X } from 'lucide-svelte';

  let isEditing = $state(false);
  let editPathValue = $state('');

  let segments = $derived.by(() => {
    const p = explorerStore.currentPath;
    if (p === '/' || !p) return [];
    const parts = p.split('/').filter(Boolean);
    return parts.map((part, index) => {
      const full = '/' + parts.slice(0, index + 1).join('/');
      return { name: part, path: full };
    });
  });

  function startEdit() {
    editPathValue = explorerStore.currentPath;
    isEditing = true;
  }

  function applyEdit() {
    let clean = editPathValue.trim();
    if (!clean.startsWith('/')) clean = '/' + clean;
    isEditing = false;
    explorerStore.navigateTo(clean);
  }

  function cancelEdit() {
    isEditing = false;
  }
</script>

<div class="bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 px-4 py-2 sm:px-6 shadow-2xs select-none">
  <div class="w-full flex items-center justify-between gap-3">
    {#if isEditing}
      <form onsubmit={(e) => { e.preventDefault(); applyEdit(); }} class="flex items-center gap-1.5 flex-1">
        <!-- svelte-ignore a11y_autofocus -->
        <input
          type="text"
          bind:value={editPathValue}
          class="flex-1 px-2.5 py-1 text-xs font-mono bg-slate-50 dark:bg-slate-950 border border-sky-400 dark:border-sky-500 rounded-lg focus:outline-none focus:ring-1 focus:ring-sky-500 focus:bg-white dark:focus:bg-slate-900 text-slate-900 dark:text-slate-100"
          placeholder="/path/in/hdfs"
          autofocus
        />
        <button
          type="submit"
          class="p-1 text-emerald-700 dark:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-950/40 rounded-lg border border-emerald-200 dark:border-emerald-800 bg-emerald-50/50 dark:bg-emerald-950/20 transition cursor-pointer shadow-2xs"
          title="Перейти"
        >
          <Check class="w-3.5 h-3.5" />
        </button>
        <button
          type="button"
          onclick={cancelEdit}
          class="p-1 text-slate-500 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900 transition cursor-pointer shadow-2xs"
          title="Отмена"
        >
          <X class="w-3.5 h-3.5" />
        </button>
      </form>
    {:else}
      <nav class="flex items-center space-x-1 text-xs font-medium text-slate-600 dark:text-slate-300 overflow-x-auto py-0.5 flex-1">
        <button
          onclick={() => explorerStore.navigateUp()}
          disabled={!explorerStore.parentPath}
          class="p-1 text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-md transition disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer shadow-2xs border border-slate-200 dark:border-slate-800"
          title="На уровень вверх"
        >
          <ArrowUp class="w-3.5 h-3.5" />
        </button>

        <button
          onclick={() => explorerStore.navigateTo('/')}
          class="flex items-center px-2 py-1 text-slate-700 dark:text-slate-200 hover:text-sky-600 dark:hover:text-sky-400 hover:bg-sky-50 dark:hover:bg-sky-950/30 rounded-md transition cursor-pointer font-mono"
        >
          <Home class="w-3.5 h-3.5 mr-1 text-slate-400" />
          <span>/</span>
        </button>

        {#each segments as seg}
          <ChevronRight class="w-3.5 h-3.5 text-slate-300 dark:text-slate-600 shrink-0" />
          <button
            onclick={() => explorerStore.navigateTo(seg.path)}
            class="px-2 py-1 text-slate-700 dark:text-slate-200 hover:text-sky-600 dark:hover:text-sky-400 hover:bg-sky-50 dark:hover:bg-sky-950/30 rounded-md transition whitespace-nowrap cursor-pointer font-mono text-xs"
          >
            {seg.name}
          </button>
        {/each}

        <button
          onclick={startEdit}
          class="p-1 text-slate-400 dark:text-slate-500 hover:text-slate-700 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-md transition ml-1 cursor-pointer"
          title="Редактировать путь"
        >
          <Edit2 class="w-3 h-3" />
        </button>
      </nav>
    {/if}
  </div>
</div>
