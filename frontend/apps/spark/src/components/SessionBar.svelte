<script lang="ts">
  import { Play, Square, Settings, RefreshCw, Power, ExternalLink, Activity, Box } from 'lucide-svelte';
  import type { SparkSessionItem } from '../types';

  let {
    language = 'pyspark',
    session,
    isRunning,
    statusText = '',
    yarnClusterId,
    onLanguageChange,
    onRun,
    onCancel,
    onOpenSettings,
    onRestartSession,
    onStopSession
  }: {
    language: 'pyspark' | 'scalaspark' | 'sql';
    session: SparkSessionItem | null;
    isRunning: boolean;
    statusText?: string;
    yarnClusterId?: string | null;
    onLanguageChange: (lang: 'pyspark' | 'scalaspark' | 'sql') => void;
    onRun: () => void;
    onCancel?: () => void;
    onOpenSettings: () => void;
    onRestartSession: () => void;
    onStopSession: () => void;
  } = $props();

  function getStatusBadgeClass(status?: string): string {
    switch (status) {
      case 'idle':
        return 'bg-emerald-50 text-emerald-700 border-emerald-200';
      case 'starting':
        return 'bg-sky-50 text-sky-700 border-sky-200 animate-pulse';
      case 'busy':
        return 'bg-amber-50 text-amber-700 border-amber-200';
      case 'killed':
      case 'dead':
        return 'bg-red-50 text-red-700 border-red-200';
      default:
        return 'bg-slate-100 text-slate-600 border-slate-200';
    }
  }

  function getStatusText(status?: string): string {
    switch (status) {
      case 'idle':
        return 'Сессия готова (Idle)';
      case 'starting':
        return 'Аллокация в YARN...';
      case 'busy':
        return 'Выполняется расчет...';
      case 'killed':
      case 'dead':
        return 'Сессия остановлена';
      default:
        return 'Сессия не подключена';
    }
  }
</script>

<div class="h-12 bg-white border-b border-slate-200 px-4 flex items-center justify-between select-none shrink-0 shadow-2xs">
  <!-- Левая часть: Кнопка Run + Индикатор статуса + Переключатель языка -->
  <div class="flex items-center gap-3">
    {#if !isRunning}
      <button
        onclick={onRun}
        class="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-amber-500 hover:bg-amber-600 text-white font-semibold text-xs shadow-sm transition cursor-pointer"
        title="Запустить выполнение (Cmd+Enter / Ctrl+Enter)"
      >
        <Play class="w-3.5 h-3.5 fill-current" />
        <span>Выполнить</span>
        <span class="text-[10px] text-amber-100 font-mono ml-1 hidden sm:inline">⌘+↵</span>
      </button>
    {:else}
      <button
        onclick={onCancel}
        class="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-red-600 hover:bg-red-500 text-white font-semibold text-xs shadow-sm transition cursor-pointer"
        title="Прервать выполнение текущей задачи Spark"
      >
        <Square class="w-3.5 h-3.5 fill-current" />
        <span>Остановить</span>
      </button>

      {#if statusText || session?.status === 'starting'}
        <div class="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-50 border border-amber-200 text-amber-800 text-xs font-medium animate-pulse">
          <div class="w-2 h-2 rounded-full bg-amber-500"></div>
          <span>{statusText || 'Подготовка...'}</span>
        </div>
      {/if}
    {/if}

    <div class="h-4 w-px bg-slate-200"></div>

    <!-- Язык в редакторе -->
    <div class="flex items-center bg-slate-100 p-0.5 rounded-lg border border-slate-200 text-xs font-medium">
      <button
        onclick={() => onLanguageChange('pyspark')}
        class="px-2.5 py-1 rounded-md transition cursor-pointer {language === 'pyspark' ? 'bg-white text-amber-600 shadow-xs font-bold' : 'text-slate-600 hover:text-slate-900'}"
      >
        PySpark
      </button>
      <button
        onclick={() => onLanguageChange('scalaspark')}
        class="px-2.5 py-1 rounded-md transition cursor-pointer {language === 'scalaspark' ? 'bg-white text-amber-600 shadow-xs font-bold' : 'text-slate-600 hover:text-slate-900'}"
      >
        Scala Spark
      </button>
      <button
        onclick={() => onLanguageChange('sql')}
        class="px-2.5 py-1 rounded-md transition cursor-pointer {language === 'sql' ? 'bg-white text-amber-600 shadow-xs font-bold' : 'text-slate-600 hover:text-slate-900'}"
      >
        Spark SQL
      </button>
    </div>
  </div>

  <!-- Правая часть: Статус сессии, YARN App ID и Кнопки управления -->
  <div class="flex items-center gap-3">
    <!-- Бейдж статуса -->
    <div class="flex items-center gap-1.5 px-2.5 py-1 rounded-lg border text-xs font-semibold {getStatusBadgeClass(session?.status)}">
      <span class="w-2 h-2 rounded-full {session?.status === 'idle' ? 'bg-emerald-500' : session?.status === 'starting' ? 'bg-sky-500' : session?.status === 'busy' ? 'bg-amber-500' : (session?.status === 'killed' || session?.status === 'dead') ? 'bg-red-500' : 'bg-slate-400'}"></span>
      {#if session}
        <span class="font-mono text-[10px] px-1 py-0.5 rounded bg-black/5 mr-0.5 uppercase tracking-wider">{session.kind === 'spark' ? 'Scala' : 'PySpark'}</span>
      {/if}
      <span>{getStatusText(session?.status)}</span>
    </div>

    <!-- Инфо о YARN App -->
    {#if session?.yarn_application_id}
      <a
        href={yarnClusterId ? `/yarn/#/apps/${session.yarn_application_id}` : '#'}
        target="_blank"
        rel="noopener noreferrer"
        class="flex items-center gap-1 text-xs font-mono font-medium text-slate-600 hover:text-amber-600 bg-slate-50 border border-slate-200 px-2 py-1 rounded-lg transition"
        title="Открыть в YARN Explorer"
      >
        <span>{session.yarn_application_id}</span>
        <ExternalLink class="w-3 h-3 text-slate-400" />
      </a>
    {/if}

    <!-- Кнопки управления сессией -->
    <div class="flex items-center gap-1">
      <button
        onclick={onOpenSettings}
        class="p-1.5 rounded-lg border border-slate-200 text-slate-600 hover:bg-slate-100 hover:text-slate-900 transition cursor-pointer shadow-2xs"
        title="Параметры сессии Spark (версии, зависимости, очереди)"
      >
        <Settings class="w-4 h-4" />
      </button>

      {#if !session || session.status === 'not_started' || session.status === 'killed' || session.status === 'dead'}
        <button
          onclick={onRestartSession}
          class="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-semibold text-xs shadow-xs transition cursor-pointer ml-1"
          title="Запустить интерактивную сессию Spark (Livy / YARN)"
        >
          <Power class="w-3.5 h-3.5" />
          <span>Подключить</span>
        </button>
      {:else}
        <button
          onclick={onRestartSession}
          class="p-1.5 rounded-lg border border-slate-200 text-slate-600 hover:bg-slate-100 hover:text-slate-900 transition cursor-pointer shadow-2xs"
          title="Перезапустить сессию Spark"
        >
          <RefreshCw class="w-4 h-4" />
        </button>

        <button
          onclick={onStopSession}
          class="p-1.5 rounded-lg border border-red-200 text-red-600 hover:bg-red-50 transition cursor-pointer shadow-2xs"
          title="Остановить сессию и освободить ресурсы YARN"
        >
          <Power class="w-4 h-4" />
        </button>
      {/if}
    </div>
  </div>
</div>
