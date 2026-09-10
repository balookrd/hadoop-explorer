<script lang="ts">
  import { Play, Square, Sparkles, Clock, Layers, ShieldCheck, Zap, BookOpen, AlignLeft, Wand2 } from 'lucide-svelte';

  let {
    isRunning,
    statusText,
    executionTimeMs,
    rowsCount,
    onRun,
    onCancel,
    onOpenAi,
    onFormat
  }: {
    isRunning: boolean;
    statusText: string;
    executionTimeMs: number;
    rowsCount: number;
    onRun: () => void;
    onCancel: () => void;
    onOpenAi?: (tab: 'check' | 'explain' | 'optimize' | 'generate') => void;
    onFormat?: () => void;
  } = $props();
</script>

<div class="h-11 bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-3 select-none shrink-0 shadow-2xs">
  <!-- Левая группа кнопок управления -->
  <div class="flex items-center gap-2">
    {#if !isRunning}
      <button
        onclick={onRun}
        class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-medium text-xs shadow-sm transition cursor-pointer"
        title="Выполнить текущий или выделенный запрос (Cmd+Enter / Ctrl+Enter)"
      >
        <Play class="w-3.5 h-3.5 fill-current" />
        <span>Выполнить</span>
        <span class="text-[10px] text-sky-200 font-mono ml-1 hidden sm:inline">⌘+↵</span>
      </button>
    {:else}
      <button
        onclick={onCancel}
        class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-red-600 hover:bg-red-500 text-white font-medium text-xs shadow-sm transition cursor-pointer"
        title="Прервать выполнение на кластере"
      >
        <Square class="w-3.5 h-3.5 fill-current" />
        <span>Остановить</span>
      </button>
    {/if}

    {#if onFormat}
      <button
        onclick={onFormat}
        class="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-slate-50 dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 text-xs font-medium border border-slate-200 dark:border-slate-700 transition cursor-pointer shadow-2xs"
        title="Автоматически отформатировать SQL (выравнивание, отступы, регистр)"
      >
        <AlignLeft class="w-3.5 h-3.5 text-slate-500 dark:text-slate-400" />
        <span class="hidden sm:inline">Формат</span>
      </button>
    {/if}

    <div class="h-4 w-px bg-slate-200 dark:bg-slate-800 mx-1"></div>

    <!-- Кнопки ИИ Ассистента -->
    {#if onOpenAi}
      <div class="flex items-center gap-1">
        <button
          onclick={() => onOpenAi && onOpenAi('generate')}
          class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-sky-50 dark:bg-sky-950/50 hover:bg-sky-100 dark:hover:bg-sky-900/50 text-sky-700 dark:text-sky-300 border border-sky-200/80 dark:border-sky-800 text-xs font-medium transition cursor-pointer shadow-2xs"
          title="Сгенерировать SQL запрос по описанию на естественном языке с помощью ИИ"
        >
          <Wand2 class="w-3.5 h-3.5 text-sky-600 dark:text-sky-400" />
          <span>ИИ Генератор</span>
        </button>

        <button
          onclick={() => onOpenAi && onOpenAi('check')}
          class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-indigo-50 dark:bg-indigo-950/50 hover:bg-indigo-100 dark:hover:bg-indigo-900/50 text-indigo-700 dark:text-indigo-300 border border-indigo-200/80 dark:border-indigo-800 text-xs font-medium transition cursor-pointer shadow-2xs"
          title="Проверить SQL на ошибки, антипаттерны и деструктивные операции"
        >
          <Sparkles class="w-3.5 h-3.5 text-indigo-600 dark:text-indigo-400" />
          <span>Анализ</span>
        </button>

        <button
          onclick={() => onOpenAi && onOpenAi('explain')}
          class="hidden md:flex items-center gap-1 px-2 py-1.5 rounded-lg text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 text-xs font-medium transition cursor-pointer"
          title="Объяснить логику SQL запроса"
        >
          <BookOpen class="w-3.5 h-3.5 text-slate-500 dark:text-slate-400" />
          <span>Объяснить</span>
        </button>

        <button
          onclick={() => onOpenAi && onOpenAi('optimize')}
          class="hidden md:flex items-center gap-1 px-2 py-1.5 rounded-lg text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 text-xs font-medium transition cursor-pointer"
          title="Оптимизировать производительность SQL"
        >
          <Zap class="w-3.5 h-3.5 text-slate-500 dark:text-slate-400" />
          <span>Оптимизировать</span>
        </button>
      </div>
    {/if}

  </div>


  <!-- Правая группа статусов и метрик -->
  <div class="flex items-center gap-2 text-xs shrink-0">
    {#if isRunning && statusText}
      <div class="flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-sky-50 dark:bg-sky-950/50 border border-sky-200 dark:border-sky-800 text-sky-800 dark:text-sky-300 animate-pulse">
        <div class="w-2 h-2 rounded-full bg-sky-500 animate-ping"></div>
        <span class="text-[11px] font-mono font-medium">{statusText}</span>
      </div>
    {/if}

    {#if executionTimeMs > 0}
      <div class="flex items-center gap-1 bg-slate-50 dark:bg-slate-950 px-2 py-1 rounded-md border border-slate-200 dark:border-slate-800 text-slate-600 dark:text-slate-400 font-mono text-[11px]">
        <Clock class="w-3 h-3 text-slate-400" />
        <span>{(executionTimeMs / 1000).toFixed(2)} с</span>
      </div>
    {/if}

    {#if rowsCount > 0}
      <div class="flex items-center gap-1 text-emerald-700 dark:text-emerald-400 font-mono text-[11px] bg-emerald-50 dark:bg-emerald-950/50 border border-emerald-200 dark:border-emerald-800 px-2 py-1 rounded-md font-medium">
        <Layers class="w-3 h-3 text-emerald-600 dark:text-emerald-400" />
        <span>{rowsCount.toLocaleString()} строк</span>
      </div>
    {/if}
  </div>
</div>
