<script lang="ts">
  import { Play, Square, Clock, Layers } from 'lucide-svelte';

  let {
    language = 'pyspark',
    isRunning,
    statusText = '',
    executionTimeMs = 0,
    rowsCount = 0,
    onLanguageChange,
    onRun,
    onCancel
  }: {
    language: 'pyspark' | 'scalaspark' | 'sql';
    isRunning: boolean;
    statusText?: string;
    executionTimeMs?: number;
    rowsCount?: number;
    onLanguageChange: (lang: 'pyspark' | 'scalaspark' | 'sql') => void;
    onRun: () => void;
    onCancel?: () => void;
  } = $props();
</script>

<div class="h-11 bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 px-3 flex items-center justify-between select-none shrink-0 shadow-2xs">
  <!-- Левая часть: Кнопка Run + Индикатор подготовки/выполнения + Переключатель языка -->
  <div class="flex items-center gap-2 sm:gap-3">
    {#if !isRunning}
      <button
        onclick={onRun}
        class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-amber-500 hover:bg-amber-600 text-white font-semibold text-xs shadow-sm transition cursor-pointer"
        title="Запустить выполнение скрипта (Cmd+Enter / Ctrl+Enter)"
      >
        <Play class="w-3.5 h-3.5 fill-current" />
        <span>Выполнить</span>
        <span class="text-[10px] text-amber-100 font-mono ml-1 hidden sm:inline">⌘+↵</span>
      </button>
    {:else}
      <button
        onclick={onCancel}
        class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-red-600 hover:bg-red-500 text-white font-semibold text-xs shadow-sm transition cursor-pointer"
        title="Прервать выполнение текущей задачи Spark"
      >
        <Square class="w-3.5 h-3.5 fill-current" />
        <span>Остановить</span>
      </button>

      {#if statusText}
        <div class="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-50 dark:bg-amber-950/50 border border-amber-200 dark:border-amber-800 text-amber-800 dark:text-amber-300 text-xs font-medium animate-pulse">
          <div class="w-2 h-2 rounded-full bg-amber-500"></div>
          <span>{statusText}</span>
        </div>
      {/if}
    {/if}

    <div class="h-4 w-px bg-slate-200 dark:border-slate-800"></div>

    <!-- Переключатель языка скрипта -->
    <div class="flex items-center bg-slate-100 dark:bg-slate-950 p-0.5 rounded-lg border border-slate-200 dark:border-slate-800 text-xs font-medium">
      <button
        onclick={() => onLanguageChange('pyspark')}
        class="px-2.5 py-1 rounded-md transition cursor-pointer {language === 'pyspark' ? 'bg-white dark:bg-slate-800 text-amber-600 dark:text-amber-400 shadow-xs font-bold' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200'}"
        title="Скрипт на Python (PySpark API)"
      >
        PySpark
      </button>
      <button
        onclick={() => onLanguageChange('scalaspark')}
        class="px-2.5 py-1 rounded-md transition cursor-pointer {language === 'scalaspark' ? 'bg-white dark:bg-slate-800 text-amber-600 dark:text-amber-400 shadow-xs font-bold' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200'}"
        title="Скрипт на Scala (Spark API)"
      >
        Scala Spark
      </button>
      <button
        onclick={() => onLanguageChange('sql')}
        class="px-2.5 py-1 rounded-md transition cursor-pointer {language === 'sql' ? 'bg-white dark:bg-slate-800 text-amber-600 dark:text-amber-400 shadow-xs font-bold' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200'}"
        title="Spark SQL запрос"
      >
        Spark SQL
      </button>
    </div>
  </div>

  <!-- Правая часть: Метрики выполнения скрипта (время, строки) -->
  <div class="flex items-center gap-2 sm:gap-3 text-xs text-slate-500 dark:text-slate-400">
    {#if executionTimeMs > 0}
      <div class="flex items-center gap-1 bg-slate-50 dark:bg-slate-950 px-2 py-1 rounded-md border border-slate-200 dark:border-slate-800 text-[11px] font-mono">
        <Clock class="w-3.5 h-3.5 text-slate-400" />
        <span>{(executionTimeMs / 1000).toFixed(2)} с</span>
      </div>
    {/if}

    {#if rowsCount > 0}
      <div class="flex items-center gap-1 text-emerald-700 dark:text-emerald-400 font-mono text-[11px] bg-emerald-50 dark:bg-emerald-950/50 border border-emerald-200 dark:border-emerald-800 px-2 py-1 rounded-md font-medium">
        <Layers class="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400" />
        <span>{rowsCount.toLocaleString()} строк</span>
      </div>
    {/if}
  </div>
</div>
