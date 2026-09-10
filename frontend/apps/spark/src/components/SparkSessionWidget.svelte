<script lang="ts">
  import { Settings, RefreshCw, Power, ExternalLink } from 'lucide-svelte';
  import type { SparkSessionItem } from '../types';

  let {
    session,
    yarnClusterId,
    onOpenSettings,
    onRestartSession,
    onStopSession
  }: {
    session: SparkSessionItem | null;
    yarnClusterId?: string | null;
    onOpenSettings: () => void;
    onRestartSession: () => void;
    onStopSession: () => void;
  } = $props();

  function getStatusBadgeClass(status?: string): string {
    switch (status) {
      case 'idle':
        return 'bg-emerald-50 dark:bg-emerald-950/50 text-emerald-700 dark:text-emerald-300 border-emerald-200 dark:border-emerald-800';
      case 'starting':
        return 'bg-sky-50 dark:bg-sky-950/50 text-sky-700 dark:text-sky-300 border-sky-200 dark:border-sky-800 animate-pulse';
      case 'busy':
        return 'bg-amber-50 dark:bg-amber-950/50 text-amber-700 dark:text-amber-300 border-amber-200 dark:border-amber-800';
      case 'killed':
      case 'dead':
        return 'bg-red-50 dark:bg-red-950/50 text-red-700 dark:text-red-300 border-red-200 dark:border-red-800';
      default:
        return 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-700';
    }
  }

  function getStatusDotClass(status?: string): string {
    switch (status) {
      case 'idle':
        return 'bg-emerald-500';
      case 'starting':
        return 'bg-sky-500';
      case 'busy':
        return 'bg-amber-500';
      case 'killed':
      case 'dead':
        return 'bg-red-500';
      default:
        return 'bg-slate-400';
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

<div class="flex items-center gap-1.5 sm:gap-2 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg p-1 sm:px-2 sm:py-1 shadow-2xs shrink-0 select-none">
  <!-- Бейдж статуса Livy сессии -->
  <div class="flex items-center gap-1.5 px-2 py-0.5 rounded text-xs font-semibold border {getStatusBadgeClass(session?.status)}">
    <span class="w-2 h-2 rounded-full {getStatusDotClass(session?.status)}"></span>
    {#if session}
      <span class="font-mono text-[9px] px-1 py-0.2 rounded bg-black/5 dark:bg-white/10 uppercase tracking-wider">
        {session.kind === 'spark' ? 'Scala' : 'PySpark'}
      </span>
    {/if}
    <span class="hidden md:inline text-[11px]">{getStatusText(session?.status)}</span>
  </div>

  <!-- YARN App ID (если есть) -->
  {#if session?.yarn_application_id}
    <a
      href={yarnClusterId ? `/yarn/#/apps/${session.yarn_application_id}` : '#'}
      target="_blank"
      rel="noopener noreferrer"
      class="hidden lg:flex items-center gap-1 text-[11px] font-mono font-medium text-slate-600 dark:text-slate-300 hover:text-amber-600 dark:hover:text-amber-400 bg-white dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-1.5 py-0.5 rounded transition"
      title="Открыть Application в YARN Explorer"
    >
      <span class="truncate max-w-[110px]">{session.yarn_application_id}</span>
      <ExternalLink class="w-3 h-3 text-slate-400" />
    </a>
  {/if}

  <div class="h-3.5 w-px bg-slate-300 dark:bg-slate-700 mx-0.5 hidden sm:block"></div>

  <!-- Кнопки управления сессией -->
  <div class="flex items-center gap-1">
    <button
      onclick={onOpenSettings}
      class="p-1 rounded-md border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 hover:text-slate-900 dark:hover:text-slate-100 transition cursor-pointer shadow-2xs"
      title="Параметры сессии Spark (версии, зависимости, очереди)"
    >
      <Settings class="w-3.5 h-3.5" />
    </button>

    {#if !session || session.status === 'not_started' || session.status === 'killed' || session.status === 'dead'}
      <button
        onclick={onRestartSession}
        class="flex items-center gap-1 px-2 py-1 rounded-md bg-emerald-600 hover:bg-emerald-500 text-white font-semibold text-[11px] shadow-xs transition cursor-pointer"
        title="Запустить интерактивную сессию Spark (Livy / YARN)"
      >
        <Power class="w-3 h-3" />
        <span class="hidden sm:inline">Подключить</span>
      </button>
    {:else}
      <button
        onclick={onRestartSession}
        class="p-1 rounded-md border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 hover:text-slate-900 dark:hover:text-slate-100 transition cursor-pointer shadow-2xs"
        title="Перезапустить сессию Spark"
      >
        <RefreshCw class="w-3.5 h-3.5" />
      </button>

      <button
        onclick={onStopSession}
        class="p-1 rounded-md border border-red-200 dark:border-red-900 bg-white dark:bg-slate-950 text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-950/40 transition cursor-pointer shadow-2xs"
        title="Остановить сессию и освободить ресурсы YARN"
      >
        <Power class="w-3.5 h-3.5" />
      </button>
    {/if}
  </div>
</div>
