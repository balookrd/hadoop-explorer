<script lang="ts">
  import { onMount, untrack } from 'svelte';

  import {
    Sparkles,
    CheckCircle2,
    AlertTriangle,
    AlertCircle,
    Info,
    ArrowRight,
    Copy,
    Check,
    Cpu,
    Zap,
    BookOpen,
    ShieldAlert,
    Wand2,
    X,
    ExternalLink
  } from 'lucide-svelte';
  import { api } from '../api/client';
  import type {
    AICheckResponse,
    AIExplainResponse,
    AIOptimizeResponse,
    AIFixResponse,
    AIStatusResponse,
    AIIssue
  } from '../types';

  let {
    isOpen = $bindable(false),
    initialTab = 'check', // 'check' | 'explain' | 'optimize' | 'fix'
    sqlQuery = '',
    clusterId = '',
    engineType = 'trino',
    errorMessage = '',
    onApplySql,
    onHighlightIssues,
    onNavigateToLine
  }: {
    isOpen: boolean;
    initialTab?: 'check' | 'explain' | 'optimize' | 'fix';
    sqlQuery: string;
    clusterId?: string;
    engineType?: string;
    errorMessage?: string;
    onApplySql?: (newSql: string) => void;
    onHighlightIssues?: (issues: AIIssue[]) => void;
    onNavigateToLine?: (line: number) => void;
  } = $props();

  let activeTab = $state<'check' | 'explain' | 'optimize' | 'fix'>('check');
  let isLoading = $state(false);
  let error = $state<string | null>(null);
  let copied = $state(false);

  let checkResult = $state<AICheckResponse | null>(null);
  let explainResult = $state<AIExplainResponse | null>(null);
  let optimizeResult = $state<AIOptimizeResponse | null>(null);
  let fixResult = $state<AIFixResponse | null>(null);
  let aiStatus = $state<AIStatusResponse | null>(null);

  let prevOpen = false;

  $effect(() => {
    if (isOpen && !prevOpen) {
      prevOpen = true;
      activeTab = initialTab;
      untrack(() => {
        checkResult = null;
        explainResult = null;
        optimizeResult = null;
        fixResult = null;
        error = null;
        loadAiStatus();
        runActionForTab(initialTab);
      });
    } else if (!isOpen) {
      prevOpen = false;
    }
  });

  async function loadAiStatus() {
    try {
      aiStatus = await api.getAiStatus();
    } catch (_) {}
  }

  async function runActionForTab(tab: 'check' | 'explain' | 'optimize' | 'fix') {
    if (!sqlQuery.trim()) {
      error = 'SQL-запрос пуст. Введите запрос в редактор.';
      return;
    }

    isLoading = true;
    error = null;

    try {
      if (tab === 'check') {
        checkResult = await api.checkSql(sqlQuery, clusterId, engineType);
        if (onHighlightIssues && checkResult.issues) {
          onHighlightIssues(checkResult.issues);
        }
      } else if (tab === 'explain') {
        explainResult = await api.explainSql(sqlQuery, clusterId, engineType);
      } else if (tab === 'optimize') {
        optimizeResult = await api.optimizeSql(sqlQuery, clusterId, engineType);
      } else if (tab === 'fix') {
        fixResult = await api.fixSql(sqlQuery, errorMessage || 'Синтаксическая ошибка или ошибка выполнения', clusterId, engineType);
      }
    } catch (err: any) {
      error = err?.message || 'Не удалось выполнить запрос к ИИ-ассистенту';
    } finally {
      isLoading = false;
    }
  }

  function handleTabChange(tab: 'check' | 'explain' | 'optimize' | 'fix') {
    activeTab = tab;
    // Если еще не загружено для этой вкладки
    if (
      (tab === 'check' && !checkResult) ||
      (tab === 'explain' && !explainResult) ||
      (tab === 'optimize' && !optimizeResult) ||
      (tab === 'fix' && !fixResult)
    ) {
      runActionForTab(tab);
    }
  }


  function copyToClipboard(text: string) {
    navigator.clipboard.writeText(text);
    copied = true;
    setTimeout(() => {
      copied = false;
    }, 2000);
  }

  function applySql(sql: string) {
    if (onApplySql) {
      onApplySql(sql);
      isOpen = false;
    }
  }

  const errorsCount = $derived(
    checkResult?.issues.filter((i) => i.severity === 'error').length || 0
  );
  const warningsCount = $derived(
    checkResult?.issues.filter((i) => i.severity === 'warning').length || 0
  );
  const infosCount = $derived(
    checkResult?.issues.filter((i) => i.severity === 'info').length || 0
  );
</script>

{#if isOpen}
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-xs p-4 sm:p-6 overflow-y-auto animate-fadeIn">
    <div class="bg-white rounded-2xl shadow-2xl border border-slate-200 w-full max-w-4xl max-h-[90vh] flex flex-col overflow-hidden text-slate-800">
      
      <!-- Шапка модального окна -->
      <div class="px-6 py-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/80 shrink-0">
        <div class="flex items-center gap-3">
          <div class="w-9 h-9 rounded-xl bg-gradient-to-tr from-sky-600 to-indigo-600 flex items-center justify-center text-white shadow-md shadow-sky-500/20">
            <Sparkles class="w-5 h-5" />
          </div>
          <div>
            <div class="flex items-center gap-2">
              <h3 class="text-base font-semibold text-slate-900">ИИ SQL Ассистент</h3>
              {#if aiStatus}
                <span
                  class="text-[11px] px-2.5 py-0.5 rounded-full font-medium flex items-center gap-1.5 {aiStatus.available ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' : 'bg-amber-50 text-amber-800 border border-amber-300'}"
                  title={aiStatus.message}
                >
                  <span class="w-1.5 h-1.5 rounded-full {aiStatus.available ? 'bg-emerald-500' : 'bg-amber-500'}"></span>
                  {#if aiStatus.available}
                    <span>LLM: {aiStatus.model}</span>
                  {:else}
                    <span>Mock-заглушка (без LLM)</span>
                  {/if}
                </span>
              {/if}
            </div>
            <p class="text-xs text-slate-500">Диалект: <span class="font-mono font-medium text-slate-700 uppercase">{engineType}</span></p>
          </div>
        </div>

        <button
          onclick={() => (isOpen = false)}
          class="p-2 text-slate-400 hover:text-slate-600 hover:bg-slate-200/60 rounded-lg transition"
          title="Закрыть"
        >
          <X class="w-5 h-5" />
        </button>
      </div>

      <!-- Вкладки режимов -->
      <div class="flex items-center gap-1 px-6 border-b border-slate-200 bg-white text-xs font-medium shrink-0">
        <button
          onclick={() => handleTabChange('check')}
          class="flex items-center gap-2 py-3 px-3 border-b-2 transition font-medium cursor-pointer {activeTab === 'check' ? 'border-sky-600 text-sky-600 font-semibold' : 'border-transparent text-slate-600 hover:text-slate-900'}"
        >
          <ShieldAlert class="w-4 h-4" />
          <span>Анализ и замечания</span>
          {#if checkResult && checkResult.issues.length > 0}
            <span class="px-1.5 py-0.2 rounded-full text-[10px] font-bold bg-amber-100 text-amber-800">
              {checkResult.issues.length}
            </span>
          {/if}
        </button>

        <button
          onclick={() => handleTabChange('explain')}
          class="flex items-center gap-2 py-3 px-3 border-b-2 transition font-medium cursor-pointer {activeTab === 'explain' ? 'border-sky-600 text-sky-600 font-semibold' : 'border-transparent text-slate-600 hover:text-slate-900'}"
        >
          <BookOpen class="w-4 h-4" />
          <span>Объяснение запроса</span>
        </button>

        <button
          onclick={() => handleTabChange('optimize')}
          class="flex items-center gap-2 py-3 px-3 border-b-2 transition font-medium cursor-pointer {activeTab === 'optimize' ? 'border-sky-600 text-sky-600 font-semibold' : 'border-transparent text-slate-600 hover:text-slate-900'}"
        >
          <Zap class="w-4 h-4" />
          <span>Оптимизация</span>
        </button>

        {#if errorMessage || activeTab === 'fix'}
          <button
            onclick={() => handleTabChange('fix')}
            class="flex items-center gap-2 py-3 px-3 border-b-2 transition font-medium cursor-pointer {activeTab === 'fix' ? 'border-red-600 text-red-600 font-semibold' : 'border-transparent text-red-500 hover:text-red-700'}"
          >
            <Wand2 class="w-4 h-4" />
            <span>Исправление ошибки</span>
          </button>
        {/if}
      </div>

      <!-- Тело контента -->
      <div class="flex-1 overflow-y-auto p-6 space-y-4 min-h-[320px]">
        {#if aiStatus && !aiStatus.available}
          <div class="p-3 rounded-xl bg-amber-50/90 border border-amber-200 text-amber-900 text-xs flex items-center justify-between gap-3 shadow-2xs">
            <div class="flex items-center gap-2.5">
              <Info class="w-4 h-4 text-amber-600 shrink-0" />
              <span>
                <strong>Автономный Mock-режим:</strong> Локальный сервер LLM не обнаружен на {aiStatus.base_url}. Запросы обрабатываются встроенным эвристическим анализатором (детерминированные правила).
              </span>
            </div>
          </div>
        {/if}

        {#if isLoading}
          <div class="h-64 flex flex-col items-center justify-center text-slate-400 gap-3">
            <div class="w-8 h-8 border-3 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
            <p class="text-sm font-medium text-slate-600">ИИ-агент анализирует SQL-запрос...</p>
          </div>
        {:else if error}
          <div class="p-4 rounded-xl bg-red-50 border border-red-200 text-red-800 flex items-start gap-3">
            <AlertCircle class="w-5 h-5 text-red-600 shrink-0 mt-0.5" />
            <div>
              <h4 class="text-sm font-semibold">Не удалось получить ответ</h4>
              <p class="text-xs mt-1">{error}</p>
            </div>
          </div>
        {:else}
          
          <!-- TAB 1: АНАЛИЗ И ЗАМЕЧАНИЯ -->
          {#if activeTab === 'check' && checkResult}
            <div class="space-y-4">
              <!-- Общая сводка -->
              <div class="p-4 rounded-xl border {checkResult.is_valid ? 'bg-emerald-50/70 border-emerald-200 text-emerald-900' : 'bg-amber-50/70 border-amber-200 text-amber-900'} flex items-start justify-between gap-4">
                <div class="flex items-start gap-3">
                  {#if checkResult.is_valid}
                    <CheckCircle2 class="w-5 h-5 text-emerald-600 shrink-0 mt-0.5" />
                  {:else}
                    <AlertTriangle class="w-5 h-5 text-amber-600 shrink-0 mt-0.5" />
                  {/if}
                  <div>
                    <h4 class="text-sm font-semibold">{checkResult.summary}</h4>
                    <p class="text-xs mt-1 text-slate-600">
                      Время анализа: {checkResult.execution_time_ms} мс | Движок: {checkResult.model}
                    </p>
                  </div>
                </div>

                <div class="flex items-center gap-2 shrink-0">
                  {#if errorsCount > 0}
                    <span class="px-2 py-0.5 text-xs font-semibold rounded-md bg-red-100 text-red-700">
                      {errorsCount} ошибок
                    </span>
                  {/if}
                  {#if warningsCount > 0}
                    <span class="px-2 py-0.5 text-xs font-semibold rounded-md bg-amber-100 text-amber-700">
                      {warningsCount} преудпр.
                    </span>
                  {/if}
                  {#if infosCount > 0}
                    <span class="px-2 py-0.5 text-xs font-semibold rounded-md bg-sky-100 text-sky-700">
                      {infosCount} инфо
                    </span>
                  {/if}
                </div>
              </div>

              <!-- Индекс сложности запроса (Complexity Score) -->
              {#if checkResult.complexity_score}
                <div class="p-3.5 rounded-xl bg-slate-50 border border-slate-200 flex flex-col gap-2">
                  <div class="flex items-center justify-between text-xs font-medium">
                    <div class="flex items-center gap-2">
                      <Zap class="w-4 h-4 text-amber-500" />
                      <span class="text-slate-700 font-semibold">Индекс сложности выполнения:</span>
                      <span class="px-2 py-0.5 rounded-md text-[11px] font-bold {checkResult.complexity_score <= 3 ? 'bg-emerald-100 text-emerald-800' : checkResult.complexity_score <= 6 ? 'bg-amber-100 text-amber-800' : 'bg-red-100 text-red-800'}">
                        {checkResult.complexity_score}/10 ({checkResult.complexity_level})
                      </span>
                    </div>

                    <span class="text-[11px] text-slate-500">
                      {checkResult.complexity_score <= 3 ? 'Быстрое интерактивное исполнение' : checkResult.complexity_score <= 6 ? 'Умеренная нагрузка на воркеры' : 'Высокая нагрузка / рекомендуется фоновое исполнение'}
                    </span>
                  </div>

                  <!-- Прогресс-бар сложности -->
                  <div class="w-full bg-slate-200 rounded-full h-1.5 overflow-hidden">
                    <div
                      class="h-1.5 rounded-full transition-all duration-300 {checkResult.complexity_score <= 3 ? 'bg-emerald-500' : checkResult.complexity_score <= 6 ? 'bg-amber-500' : 'bg-red-500'}"
                      style="width: {Math.min(100, checkResult.complexity_score * 10)}%"
                    ></div>
                  </div>

                  {#if checkResult.estimated_notes && checkResult.estimated_notes.length > 0}
                    <div class="flex flex-wrap gap-1.5 pt-1">
                      {#each checkResult.estimated_notes as note}
                        <span class="text-[10px] px-2 py-0.5 rounded bg-white border border-slate-200 text-slate-600 font-mono">
                          {note}
                        </span>
                      {/each}
                    </div>
                  {/if}
                </div>
              {/if}

              <!-- Список замечаний -->
              {#if checkResult.issues.length === 0}
                <div class="p-8 text-center text-slate-500 bg-slate-50 rounded-xl border border-slate-100">
                  <CheckCircle2 class="w-10 h-10 text-emerald-500 mx-auto mb-2" />
                  <p class="text-sm font-medium text-slate-700">Замечаний не найдено</p>
                  <p class="text-xs text-slate-500 mt-1">Запрос корректен и оптимизирован для выполнения в кластере {engineType.toUpperCase()}.</p>
                </div>
              {:else}

                <div class="space-y-3">
                  {#each checkResult.issues as issue}
                    <div class="p-4 rounded-xl border transition hover:shadow-xs {issue.severity === 'error' ? 'bg-red-50/40 border-red-200' : issue.severity === 'warning' ? 'bg-amber-50/40 border-amber-200' : 'bg-slate-50 border-slate-200'}">
                      <div class="flex items-start justify-between gap-2">
                        <div class="flex items-start gap-2.5">
                          {#if issue.severity === 'error'}
                            <AlertCircle class="w-4 h-4 text-red-600 shrink-0 mt-0.5" />
                          {:else if issue.severity === 'warning'}
                            <AlertTriangle class="w-4 h-4 text-amber-600 shrink-0 mt-0.5" />
                          {:else}
                            <Info class="w-4 h-4 text-sky-600 shrink-0 mt-0.5" />
                          {/if}

                          <div>
                            <div class="flex items-center gap-2">
                              <span class="text-xs font-bold text-slate-900">{issue.message}</span>
                              <span class="text-[10px] font-mono px-1.5 py-0.2 rounded bg-slate-200/80 text-slate-700">
                                {issue.rule}
                              </span>
                              <span class="text-[10px] px-1.5 py-0.2 rounded bg-slate-100 text-slate-600 uppercase font-semibold">
                                {issue.category}
                              </span>
                            </div>

                            {#if issue.suggestion}
                              <div class="mt-2 text-xs text-slate-700 bg-white/80 border border-slate-200/80 rounded-lg p-2 flex items-start gap-1.5">
                                <span class="font-semibold text-sky-700 shrink-0">💡 Совет:</span>
                                <span>{issue.suggestion}</span>
                              </div>
                            {/if}
                          </div>
                        </div>

                        {#if onNavigateToLine}
                          <button
                            onclick={() => onNavigateToLine && onNavigateToLine(issue.line)}
                            class="text-xs text-sky-600 hover:text-sky-700 font-medium flex items-center gap-1 shrink-0 px-2 py-1 rounded-md hover:bg-sky-50 transition cursor-pointer"
                            title="Перейти к строке в Monaco Editor"
                          >
                            <span>Стр. {issue.line}</span>
                            <ArrowRight class="w-3 h-3" />
                          </button>
                        {/if}
                      </div>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>

          <!-- TAB 2: ОБЪЯСНЕНИЕ ЗАПРОСА -->
          {:else if activeTab === 'explain' && explainResult}
            <div class="space-y-4">
              <!-- Сводка и теги -->
              <div class="p-4 rounded-xl bg-indigo-50/60 border border-indigo-100 space-y-3">
                <h4 class="text-xs font-semibold text-indigo-950 uppercase tracking-wider">Краткое резюме</h4>
                <p class="text-sm font-medium text-indigo-900">{explainResult.summary}</p>

                <div class="flex flex-wrap items-center gap-2 pt-2 border-t border-indigo-100/80">
                  {#if explainResult.tables_used.length > 0}
                    <div class="flex items-center gap-1 text-xs">
                      <span class="text-slate-500">Таблицы:</span>
                      {#each explainResult.tables_used as tbl}
                        <span class="px-2 py-0.5 rounded-md font-mono text-[11px] bg-white border border-indigo-200 text-indigo-700 font-medium">
                          {tbl}
                        </span>
                      {/each}
                    </div>
                  {/if}

                  {#if explainResult.operations.length > 0}
                    <div class="flex items-center gap-1 text-xs ml-auto">
                      <span class="text-slate-500">Операции:</span>
                      {#each explainResult.operations as op}
                        <span class="px-2 py-0.5 rounded-md text-[11px] bg-indigo-100 text-indigo-800 font-semibold">
                          {op}
                        </span>
                      {/each}
                    </div>
                  {/if}
                </div>
              </div>

              <!-- Подробное объяснение -->
              <div class="p-5 rounded-xl bg-slate-50 border border-slate-200 text-slate-800 text-xs leading-relaxed space-y-3 whitespace-pre-line font-sans">
                {explainResult.explanation}
              </div>
            </div>

          <!-- TAB 3: ОПТИМИЗАЦИЯ -->
          {:else if activeTab === 'optimize' && optimizeResult}
            <div class="space-y-4">
              <!-- Список улучшений или статус 'уже оптимизирован' -->
              {#if optimizeResult.optimizations.length > 0 && optimizeResult.optimized_sql.trim() !== sqlQuery.trim()}
                <div class="p-4 rounded-xl bg-emerald-50/60 border border-emerald-200 space-y-2">
                  <h4 class="text-xs font-semibold text-emerald-950 uppercase tracking-wider flex items-center gap-1.5">
                    <Zap class="w-3.5 h-3.5 text-emerald-600" />
                    <span>Примененные оптимизации ({optimizeResult.optimizations.length})</span>
                  </h4>
                  <ul class="space-y-1 text-xs text-emerald-900 list-disc list-inside">
                    {#each optimizeResult.optimizations as opt}
                      <li>{opt}</li>
                    {/each}
                  </ul>
                </div>
              {:else}
                <div class="p-4 rounded-xl bg-slate-50 border border-slate-200 space-y-1.5">
                  <h4 class="text-xs font-semibold text-slate-800 uppercase tracking-wider flex items-center gap-1.5">
                    <CheckCircle2 class="w-4 h-4 text-emerald-600" />
                    <span>Запрос уже оптимизирован</span>
                  </h4>
                  <p class="text-xs text-slate-600">
                    Текущий SQL-запрос уже структурирован оптимально, содержит необходимые лимиты и не требует дополнительных изменений.
                  </p>
                </div>
              {/if}

              <!-- Оптимизированный SQL -->
              <div class="space-y-2">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-semibold text-slate-700">Оптимизированный SQL:</span>
                  <div class="flex items-center gap-2">
                    <button
                      onclick={() => copyToClipboard(optimizeResult?.optimized_sql || '')}
                      class="px-2.5 py-1 text-xs font-medium rounded-lg bg-slate-100 hover:bg-slate-200 text-slate-700 flex items-center gap-1 transition cursor-pointer"
                    >
                      {#if copied}
                        <Check class="w-3.5 h-3.5 text-emerald-600" />
                        <span>Скопировано</span>
                      {:else}
                        <Copy class="w-3.5 h-3.5 text-slate-500" />
                        <span>Копировать</span>
                      {/if}
                    </button>

                    {#if optimizeResult.optimized_sql.trim() === sqlQuery.trim()}
                      <button
                        disabled
                        class="px-3 py-1 text-xs font-semibold rounded-lg bg-slate-100 text-slate-400 border border-slate-200 flex items-center gap-1 cursor-not-allowed"
                      >
                        <Check class="w-3.5 h-3.5 text-slate-400" />
                        <span>Уже применено</span>
                      </button>
                    {:else}
                      <button
                        onclick={() => applySql(optimizeResult?.optimized_sql || '')}
                        class="px-3 py-1 text-xs font-semibold rounded-lg bg-sky-600 hover:bg-sky-500 text-white shadow-xs transition flex items-center gap-1 cursor-pointer"
                      >
                        <Check class="w-3.5 h-3.5" />
                        <span>Применить в редактор</span>
                      </button>
                    {/if}
                  </div>
                </div>

                <div class="p-3.5 rounded-xl bg-slate-900 text-slate-100 font-mono text-xs overflow-x-auto border border-slate-800 max-h-72">
                  <pre>{optimizeResult.optimized_sql}</pre>
                </div>
              </div>
            </div>

          <!-- TAB 4: ИСПРАВЛЕНИЕ ОШИБКИ -->
          {:else if activeTab === 'fix' && fixResult}
            <div class="space-y-4">
              <div class="p-4 rounded-xl bg-red-50/80 border border-red-200 space-y-2">
                <h4 class="text-xs font-semibold text-red-950 uppercase tracking-wider flex items-center gap-1.5">
                  <Wand2 class="w-3.5 h-3.5 text-red-600" />
                  <span>Анализ ошибки выполнения</span>
                </h4>
                <p class="text-xs text-red-900">{fixResult.explanation}</p>
              </div>

              <!-- Исправленный SQL -->
              <div class="space-y-2">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-semibold text-slate-700">Исправленный вариант:</span>
                  <div class="flex items-center gap-2">
                    <button
                      onclick={() => copyToClipboard(fixResult?.fixed_sql || '')}
                      class="px-2.5 py-1 text-xs font-medium rounded-lg bg-slate-100 hover:bg-slate-200 text-slate-700 flex items-center gap-1 transition cursor-pointer"
                    >
                      {#if copied}
                        <Check class="w-3.5 h-3.5 text-emerald-600" />
                        <span>Скопировано</span>
                      {:else}
                        <Copy class="w-3.5 h-3.5 text-slate-500" />
                        <span>Копировать</span>
                      {/if}
                    </button>

                    <button
                      onclick={() => applySql(fixResult?.fixed_sql || '')}
                      class="px-3 py-1 text-xs font-semibold rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white shadow-xs transition flex items-center gap-1 cursor-pointer"
                    >
                      <Check class="w-3.5 h-3.5" />
                      <span>Применить исправление</span>
                    </button>
                  </div>
                </div>

                <div class="p-3.5 rounded-xl bg-slate-900 text-slate-100 font-mono text-xs overflow-x-auto border border-slate-800 max-h-72">
                  <pre>{fixResult.fixed_sql}</pre>
                </div>
              </div>
            </div>
          {/if}

        {/if}
      </div>

      <!-- Футер модалки -->
      <div class="px-6 py-3 bg-slate-50 border-t border-slate-100 flex items-center justify-between text-xs text-slate-500 shrink-0">
        <div class="flex items-center gap-2">
          <Cpu class="w-3.5 h-3.5 text-slate-400" />
          {#if aiStatus?.available}
            <span>Режим: On-premise LLM ({aiStatus.model})</span>
          {:else}
            <span>Режим: Автономный Mock-анализатор (встроенные правила)</span>
          {/if}
        </div>

        <button
          onclick={() => (isOpen = false)}
          class="px-4 py-1.5 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-700 font-medium transition cursor-pointer"
        >
          Закрыть
        </button>
      </div>


    </div>
  </div>
{/if}

<style>
  @keyframes fadeIn {
    from { opacity: 0; transform: scale(0.98); }
    to { opacity: 1; transform: scale(1); }
  }
  .animate-fadeIn {
    animation: fadeIn 0.15s cubic-bezier(0.16, 1, 0.3, 1) forwards;
  }
</style>
