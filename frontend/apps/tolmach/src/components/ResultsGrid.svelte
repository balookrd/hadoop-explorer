<script lang="ts">
  import type { ColumnMeta } from '../types';
  import { Download, Search, AlertCircle, ChevronLeft, ChevronRight, FileSpreadsheet, FileJson, Wand2 } from 'lucide-svelte';

  let {
    columns,
    rows,
    errorMessage,
    totalRows,
    onFixWithAi
  }: {
    columns: ColumnMeta[];
    rows: any[][];
    errorMessage: string | null;
    totalRows: number;
    onFixWithAi?: () => void;
  } = $props();


  let filterText = $state('');
  let currentPage = $state(1);
  let pageSize = $state(50);

  // Фильтрация строк по тексту
  const filteredRows = $derived(
    filterText.trim() === ''
      ? rows
      : rows.filter((r) =>
          r.some((cell) => String(cell).toLowerCase().includes(filterText.toLowerCase()))
        )
  );

  // Пагинация
  const totalPages = $derived(Math.max(1, Math.ceil(filteredRows.length / pageSize)));
  const paginatedRows = $derived(
    filteredRows.slice((currentPage - 1) * pageSize, currentPage * pageSize)
  );

  function sanitizeForCsv(val: any): string {
    const raw = String(val ?? '');
    let escaped = raw.replace(/"/g, '""');
    // Защита от CSV / Formula Injection (DDE) в Excel/Calc
    if (/^[=+\-@\t\r]/.test(escaped)) {
      escaped = `'${escaped}`;
    }
    return `"${escaped}"`;
  }

  function exportToCsv() {
    if (columns.length === 0 || rows.length === 0) return;
    const header = columns.map((c) => sanitizeForCsv(c.name)).join(',');
    const body = rows
      .map((r) => r.map((val) => sanitizeForCsv(val)).join(','))
      .join('\n');
    const blob = new Blob([header + '\n' + body], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `query_result_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '_')}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }

  function exportToJson() {
    if (columns.length === 0 || rows.length === 0) return;
    const jsonObjects = rows.map((r) => {
      const obj: Record<string, any> = {};
      columns.forEach((col, idx) => {
        obj[col.name] = r[idx];
      });
      return obj;
    });
    const blob = new Blob([JSON.stringify(jsonObjects, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `query_result_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '_')}.json`;
    link.click();
    URL.revokeObjectURL(url);
  }
</script>

<div class="h-full w-full flex flex-col bg-white dark:bg-slate-950 overflow-hidden select-none">
  <!-- Верхняя строка фильтрации и экспорта результатов -->
  <div class="h-10 bg-slate-50 dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-3 shrink-0">
    <div class="flex items-center gap-2">
      <div class="relative w-52">
        <Search class="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-2.5" />
        <input
          type="text"
          bind:value={filterText}
          placeholder="Фильтр в результатах..."
          class="w-full bg-white dark:bg-slate-950 border border-slate-300 dark:border-slate-700 text-xs rounded-md pl-8 pr-2.5 py-1 text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500 transition"
        />
      </div>

      <span class="text-xs text-slate-500 dark:text-slate-400 font-medium">
        Показано {filteredRows.length} из {totalRows}
      </span>
    </div>

    <!-- Кнопки экспорта и пагинации -->
    <div class="flex items-center gap-3">
      <div class="flex items-center gap-1.5">
        <button
          onclick={exportToCsv}
          disabled={rows.length === 0}
          class="flex items-center gap-1 px-2.5 py-1 rounded-md bg-white dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-40 text-slate-700 dark:text-slate-200 text-xs border border-slate-200 dark:border-slate-700 font-medium transition cursor-pointer shadow-2xs"
          title="Скачать в формате CSV"
        >
          <FileSpreadsheet class="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400" />
          <span>CSV</span>
        </button>

        <button
          onclick={exportToJson}
          disabled={rows.length === 0}
          class="flex items-center gap-1 px-2.5 py-1 rounded-md bg-white dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-40 text-slate-700 dark:text-slate-200 text-xs border border-slate-200 dark:border-slate-700 font-medium transition cursor-pointer shadow-2xs"
          title="Скачать в формате JSON"
        >
          <FileJson class="w-3.5 h-3.5 text-amber-600 dark:text-amber-400" />
          <span>JSON</span>
        </button>
      </div>

      <!-- Пагинация -->
      <div class="flex items-center gap-1 text-xs text-slate-600 dark:text-slate-400 font-medium">
        <button
          onclick={() => (currentPage = Math.max(1, currentPage - 1))}
          disabled={currentPage === 1}
          class="p-1 rounded-md bg-white dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-700 disabled:opacity-40 cursor-pointer shadow-2xs"
        >
          <ChevronLeft class="w-3.5 h-3.5 text-slate-600 dark:text-slate-300" />
        </button>
        <span class="px-1.5">{currentPage} / {totalPages}</span>
        <button
          onclick={() => (currentPage = Math.min(totalPages, currentPage + 1))}
          disabled={currentPage === totalPages}
          class="p-1 rounded-md bg-white dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-700 disabled:opacity-40 cursor-pointer shadow-2xs"
        >
          <ChevronRight class="w-3.5 h-3.5 text-slate-600 dark:text-slate-300" />
        </button>
      </div>
    </div>
  </div>

  <!-- Область таблицы или ошибки -->
  <div class="flex-1 overflow-auto bg-white dark:bg-slate-950">
    {#if errorMessage}
      <div class="p-4 m-3 rounded-xl bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-900 text-red-800 dark:text-red-300 flex items-start justify-between gap-3 text-xs shadow-2xs">
        <div class="flex items-start gap-3">
          <AlertCircle class="w-5 h-5 text-red-600 dark:text-red-400 shrink-0 mt-0.5" />
          <div>
            <div class="font-bold mb-1 text-red-900 dark:text-red-200">Ошибка исполнения запроса</div>
            <div class="font-mono text-[11px] whitespace-pre-wrap text-red-800 dark:text-red-300">{errorMessage}</div>
          </div>
        </div>

        {#if onFixWithAi}
          <button
            onclick={onFixWithAi}
            class="px-3 py-1.5 rounded-lg bg-red-600 hover:bg-red-700 text-white font-medium text-xs flex items-center gap-1.5 shadow-sm transition cursor-pointer shrink-0"
            title="Автоматически исправить ошибку запроса с помощью ИИ"
          >
            <Wand2 class="w-3.5 h-3.5" />
            <span>Исправить с ИИ</span>
          </button>
        {/if}
      </div>

    {:else if columns.length === 0}
      <div class="h-full flex items-center justify-center text-slate-400 dark:text-slate-500 text-xs">
        Результаты выполнения запроса появятся здесь
      </div>
    {:else}
      <table class="w-full text-left border-collapse font-mono text-xs">
        <thead class="sticky top-0 bg-slate-50 dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 z-10 shadow-2xs">
          <tr>
            <th class="py-2 px-3 border-r border-slate-200 dark:border-slate-800 text-slate-400 dark:text-slate-500 w-12 text-right font-normal">#</th>
            {#each columns as col}
              <th class="py-2 px-3 border-r border-slate-200 dark:border-slate-800 text-slate-700 dark:text-slate-300 font-semibold whitespace-nowrap">
                <div>{col.name}</div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-normal">{col.type}</div>
              </th>
            {/each}
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
          {#each paginatedRows as row, rIdx}
            <tr class="hover:bg-sky-50/50 dark:hover:bg-sky-950/20 transition">
              <td class="py-1.5 px-3 border-r border-slate-100 dark:border-slate-800 text-slate-400 dark:text-slate-500 text-right bg-slate-50/30 dark:bg-slate-900/30">
                {(currentPage - 1) * pageSize + rIdx + 1}
              </td>
              {#each row as cell}
                <td class="py-1.5 px-3 border-r border-slate-100 dark:border-slate-800 text-slate-800 dark:text-slate-200 whitespace-nowrap max-w-xs truncate">
                  {#if cell === null || cell === undefined}
                    <span class="text-slate-400 dark:text-slate-500 italic font-sans">null</span>
                  {:else}
                    {String(cell)}
                  {/if}
                </td>
              {/each}
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
</div>
