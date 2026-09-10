<script lang="ts">
  import { Table, Terminal, Download, Search, Copy, Check, AlertCircle } from 'lucide-svelte';
  import type { ColumnMeta } from '../types';

  let {
    columns = [],
    rows = [],
    totalRows = 0,
    logs = '',
    errorMessage = null,
    executionTimeMs = 0,
    activeTab = $bindable('table'),
    onTabChange
  }: {
    columns: ColumnMeta[];
    rows: any[][];
    totalRows: number;
    logs: string;
    errorMessage: string | null;
    executionTimeMs: number;
    activeTab: 'table' | 'logs';
    onTabChange?: (tab: 'table' | 'logs') => void;
  } = $props();

  let searchQuery = $state('');
  let copied = $state(false);

  const filteredRows = $derived(
    searchQuery
      ? rows.filter((r) => r.some((val) => String(val).toLowerCase().includes(searchQuery.toLowerCase())))
      : rows
  );

  function exportCsv() {
    if (!columns.length || !rows.length) return;
    const header = columns.map((c) => `"${c.name.replace(/"/g, '""')}"`).join(',');
    const body = rows
      .map((r) => r.map((val) => (val === null ? '' : `"${String(val).replace(/"/g, '""')}"`)).join(','))
      .join('\n');
    const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + encodeURIComponent(header + '\n' + body);
    const link = document.createElement('a');
    link.setAttribute('href', csvContent);
    link.setAttribute('download', `spark_results_${Date.now()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }

  function copyLogs() {
    navigator.clipboard.writeText(logs);
    copied = true;
    setTimeout(() => (copied = false), 2000);
  }
</script>

<div class="h-full flex flex-col bg-white dark:bg-slate-950 overflow-hidden select-none">
  <!-- Верхний бар результатов -->
  <div class="h-10 border-b border-slate-200 dark:border-slate-800 px-4 flex items-center justify-between shrink-0 bg-slate-50/50 dark:bg-slate-900/50">
    <div class="flex items-center gap-4 text-xs font-semibold">
      <button
        onclick={() => {
          activeTab = 'table';
          onTabChange?.('table');
        }}
        class="flex items-center gap-1.5 py-2.5 border-b-2 transition cursor-pointer {activeTab === 'table' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200'}"
      >
        <Table class="w-3.5 h-3.5" />
        <span>Таблица DataFrame ({rows.length})</span>
      </button>

      <button
        onclick={() => {
          activeTab = 'logs';
          onTabChange?.('logs');
        }}
        class="flex items-center gap-1.5 py-2.5 border-b-2 transition cursor-pointer {activeTab === 'logs' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200'}"
      >
        <Terminal class="w-3.5 h-3.5" />
        <span>Консоль и логи Spark</span>
      </button>
    </div>

    <!-- Правая часть тулбара -->
    <div class="flex items-center gap-3">
      {#if executionTimeMs > 0}
        <span class="text-[11px] text-slate-500 dark:text-slate-400 font-mono">
          Время: {(executionTimeMs / 1000).toFixed(2)}с
        </span>
      {/if}

      {#if activeTab === 'table' && rows.length > 0}
        <div class="relative">
          <Search class="w-3.5 h-3.5 absolute left-2 top-1.5 text-slate-400" />
          <input
            type="text"
            placeholder="Фильтр строк..."
            class="pl-7 pr-2 py-1 text-xs border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 text-slate-800 dark:text-slate-200 rounded-lg w-40 focus:outline-none focus:border-amber-500 placeholder-slate-400 dark:placeholder-slate-500"
            bind:value={searchQuery}
          />
        </div>

        <button
          onclick={exportCsv}
          class="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-slate-700 dark:text-slate-300 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-800 transition cursor-pointer shadow-2xs"
          title="Экспорт в CSV"
        >
          <Download class="w-3.5 h-3.5 text-slate-500 dark:text-slate-400" />
          <span>CSV</span>
        </button>
      {:else if activeTab === 'logs' && logs}
        <button
          onclick={copyLogs}
          class="flex items-center gap-1 px-2 py-1 text-xs font-medium text-slate-600 dark:text-slate-400 hover:bg-slate-200/50 dark:hover:bg-slate-800/50 rounded-lg transition cursor-pointer"
        >
          {#if copied}
            <Check class="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400" />
            <span class="text-emerald-600 dark:text-emerald-400">Скопировано</span>
          {:else}
            <Copy class="w-3.5 h-3.5" />
            <span>Скопировать</span>
          {/if}
        </button>
      {/if}
    </div>
  </div>

  <!-- Контент результатов -->
  <div class="flex-1 overflow-auto">
    {#if errorMessage}
      <div class="m-4 p-4 rounded-xl bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 flex items-start gap-3">
        <AlertCircle class="w-5 h-5 shrink-0 mt-0.5" />
        <div>
          <h4 class="text-xs font-bold mb-1">Ошибка исполнения задачи Spark</h4>
          <p class="text-xs font-mono whitespace-pre-wrap">{errorMessage}</p>
        </div>
      </div>
    {/if}

    {#if activeTab === 'table'}
      {#if columns.length > 0 && filteredRows.length > 0}
        <table class="w-full text-left text-xs border-collapse font-mono">
          <thead class="bg-slate-50 dark:bg-slate-900 sticky top-0 border-b border-slate-200 dark:border-slate-800 z-10 select-none">
            <tr>
              <th class="py-2 px-3 text-[11px] font-bold text-slate-400 dark:text-slate-500 w-12 border-r border-slate-200 dark:border-slate-800 text-center">#</th>
              {#each columns as col}
                <th class="py-2 px-3 text-[11px] font-bold text-slate-700 dark:text-slate-300 border-r border-slate-200 dark:border-slate-800">
                  <div class="flex items-center justify-between gap-2">
                    <span>{col.name}</span>
                    <span class="text-[10px] text-slate-400 dark:text-slate-500 font-normal">{col.type}</span>
                  </div>
                </th>
              {/each}
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
            {#each filteredRows as row, i}
              <tr class="hover:bg-amber-50/40 dark:hover:bg-amber-950/20 transition">
                <td class="py-1.5 px-3 text-[10px] text-slate-400 dark:text-slate-500 border-r border-slate-100 dark:border-slate-800 text-center select-none bg-slate-50/50 dark:bg-slate-900/50">{i + 1}</td>
                {#each row as cell}
                  <td class="py-1.5 px-3 border-r border-slate-100 dark:border-slate-800 truncate max-w-xs text-slate-800 dark:text-slate-200">
                    {cell === null ? '<null>' : String(cell)}
                  </td>
                {/each}
              </tr>
            {/each}
          </tbody>
        </table>
      {:else if !errorMessage}
        <div class="h-full flex flex-col items-center justify-center text-slate-400 dark:text-slate-500 py-12">
          <Table class="w-8 h-8 stroke-1 mb-2 text-slate-300 dark:text-slate-600" />
          <p class="text-xs">Нет данных для отображения</p>
          <p class="text-[11px] text-slate-400 dark:text-slate-500 mt-1">Используйте <code>display(df)</code> в PySpark или выполните запрос</p>
        </div>
      {/if}
    {:else}
      <!-- Вкладка Консоль / Логи -->
      <div class="p-4 font-mono text-xs text-slate-200 bg-slate-950 min-h-full whitespace-pre-wrap selection:bg-amber-500 selection:text-white leading-relaxed">
        {logs || 'Логи выполнения пока отсутствуют. Запустите расчет для просмотра вывода.'}
      </div>
    {/if}
  </div>
</div>
