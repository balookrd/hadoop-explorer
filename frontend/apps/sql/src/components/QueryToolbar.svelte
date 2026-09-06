<script lang="ts">
  import { Play, Square, Save, Sparkles, Clock, Layers, ShieldCheck, Zap, BookOpen, AlignLeft } from 'lucide-svelte';

  let {
    isRunning,
    statusText,
    executionTimeMs,
    rowsCount,
    onRun,
    onCancel,
    onSave,
    onOpenAi,
    onFormat
  }: {
    isRunning: boolean;
    statusText: string;
    executionTimeMs: number;
    rowsCount: number;
    onRun: () => void;
    onCancel: () => void;
    onSave: () => void;
    onOpenAi?: (tab: 'check' | 'explain' | 'optimize') => void;
    onFormat?: () => void;
  } = $props();
</script>

<div class="h-11 bg-white border-b border-slate-200 flex items-center justify-between px-3 select-none shrink-0 shadow-2xs">
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

    <button
      onclick={onSave}
      class="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-slate-50 hover:bg-slate-100 text-slate-700 text-xs font-medium border border-slate-200 transition cursor-pointer shadow-2xs"
      title="Сохранить в избранное"
    >
      <Save class="w-3.5 h-3.5 text-slate-500" />
      <span class="hidden sm:inline">Сохранить</span>
    </button>

    {#if onFormat}
      <button
        onclick={onFormat}
        class="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-slate-50 hover:bg-slate-100 text-slate-700 text-xs font-medium border border-slate-200 transition cursor-pointer shadow-2xs"
        title="Автоматически отформатировать SQL (выравнивание, отступы, регистр)"
      >
        <AlignLeft class="w-3.5 h-3.5 text-slate-500" />
        <span class="hidden sm:inline">Формат</span>
      </button>
    {/if}

    <div class="h-4 w-px bg-slate-200 mx-1"></div>

    <!-- Кнопки ИИ Ассистента -->
    {#if onOpenAi}
      <div class="flex items-center gap-1">
        <button
          onclick={() => onOpenAi && onOpenAi('check')}
          class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-indigo-50 hover:bg-indigo-100 text-indigo-700 border border-indigo-200/80 text-xs font-medium transition cursor-pointer shadow-2xs"
          title="Проверить SQL на ошибки, антипаттерны и деструктивные операции"
        >
          <Sparkles class="w-3.5 h-3.5 text-indigo-600" />
          <span>ИИ Анализ</span>
        </button>

        <button
          onclick={() => onOpenAi && onOpenAi('explain')}
          class="hidden md:flex items-center gap-1 px-2 py-1.5 rounded-lg text-slate-600 hover:text-slate-900 hover:bg-slate-100 text-xs font-medium transition cursor-pointer"
          title="Объяснить логику SQL запроса"
        >
          <BookOpen class="w-3.5 h-3.5 text-slate-500" />
          <span>Объяснить</span>
        </button>

        <button
          onclick={() => onOpenAi && onOpenAi('optimize')}
          class="hidden md:flex items-center gap-1 px-2 py-1.5 rounded-lg text-slate-600 hover:text-slate-900 hover:bg-slate-100 text-xs font-medium transition cursor-pointer"
          title="Оптимизировать производительность SQL"
        >
          <Zap class="w-3.5 h-3.5 text-slate-500" />
          <span>Оптимизировать</span>
        </button>
      </div>
    {/if}

  </div>


  <!-- Правая группа статусов и метрик -->
  <div class="flex items-center gap-3 text-xs">
    {#if statusText}
      <div class="flex items-center gap-1.5 px-2 py-1 rounded-md bg-slate-50 border border-slate-200 text-slate-700">
        {#if isRunning}
          <div class="w-2 h-2 rounded-full bg-sky-500 animate-ping"></div>
        {/if}
        <span class="text-[11px] font-mono font-medium">{statusText}</span>
      </div>
    {/if}

    {#if executionTimeMs > 0}
      <div class="flex items-center gap-1 text-slate-500 font-mono text-[11px]">
        <Clock class="w-3 h-3 text-slate-400" />
        <span>{(executionTimeMs / 1000).toFixed(2)} с</span>
      </div>
    {/if}

    {#if rowsCount > 0}
      <div class="flex items-center gap-1 text-emerald-700 font-mono text-[11px] bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded-md font-medium">
        <Layers class="w-3 h-3 text-emerald-600" />
        <span>{rowsCount.toLocaleString()} строк</span>
      </div>
    {/if}
  </div>
</div>
