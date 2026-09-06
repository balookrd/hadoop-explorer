<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '../api/client';
  import type { QueryHistoryItem } from '../types';
  import {
    ListOrdered,
    Trash2,
    Eye,
    Play,
    RefreshCw,
    CheckCircle2,
    XCircle,
    Clock,
    AlertTriangle,
    Layers,
    Square
  } from 'lucide-svelte';

  let {
    onLoadResult,
    onInsertQuery
  }: {
    onLoadResult: (queryId: string, clusterName: string) => void;
    onInsertQuery: (queryText: string) => void;
  } = $props();

  let queueItems = $state<QueryHistoryItem[]>([]);
  let loading = $state(false);
  let actionLoading = $state<Record<string, boolean>>({});

  export async function refreshQueue() {
    loading = true;
    try {
      queueItems = await api.getQueue();
    } catch (err) {
      console.error('Ошибка загрузки очереди задач', err);
    } finally {
      loading = false;
    }
  }

  async function handleDelete(item: QueryHistoryItem) {
    const isRunning = item.status === 'RUNNING' || item.status === 'QUEUED';
    const msg = isRunning
      ? `Остановить выполнение и удалить запрос из очереди?`
      : `Удалить запрос из списка очереди?`;

    if (!confirm(msg)) return;

    actionLoading[item.id] = true;
    try {
      await api.deleteFromQueue(item.id);
      await refreshQueue();
    } catch (err: any) {
      alert(`Ошибка удаления: ${err.message}`);
    } finally {
      actionLoading[item.id] = false;
    }
  }

  onMount(() => {
    refreshQueue();
    const timer = setInterval(refreshQueue, 3000);
    return () => clearInterval(timer);
  });
</script>

<div class="h-full flex flex-col select-none overflow-hidden bg-white">
  <!-- Верхняя строка управления очередью -->
  <div class="p-2.5 border-b border-slate-200 flex items-center justify-between bg-slate-50/70 shrink-0">
    <div class="flex items-center gap-1.5 text-xs text-slate-700 font-semibold">
      <ListOrdered class="w-3.5 h-3.5 text-sky-600" />
      <span>Очередь задач ({queueItems.filter((i) => i.status === 'RUNNING' || i.status === 'QUEUED').length} активных)</span>
    </div>

    <button
      onclick={refreshQueue}
      class="p-1 rounded-md hover:bg-slate-200 text-slate-500 hover:text-slate-800 transition cursor-pointer"
      title="Обновить очередь"
    >
      <RefreshCw class="w-3.5 h-3.5 {loading ? 'animate-spin' : ''}" />
    </button>
  </div>

  <!-- Список задач -->
  <div class="flex-1 overflow-y-auto p-2.5 space-y-2 text-xs">
    {#if queueItems.length === 0}
      <div class="text-center py-10 text-slate-400">
        В очереди нет задач.<br />
        <span class="text-[11px] text-slate-400">Все запущенные запросы сохраняются здесь</span>
      </div>
    {:else}
      {#each queueItems as item}
        <div
          class="p-2.5 rounded-lg bg-slate-50 hover:bg-sky-50/50 border border-slate-200 shadow-2xs transition flex flex-col gap-1.5 group"
        >
          <!-- Статус и кластер -->
          <div class="flex items-center justify-between">
            <span class="font-bold text-slate-800 text-[11px] truncate">{item.cluster_name}</span>

            <div class="flex items-center gap-1.5">
              {#if item.status === 'QUEUED'}
                <span class="px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-50 text-amber-700 border border-amber-200 flex items-center gap-1">
                  <Clock class="w-2.5 h-2.5" /> В очереди
                </span>
              {:else if item.status === 'RUNNING'}
                <span class="px-1.5 py-0.5 rounded text-[10px] font-medium bg-sky-50 text-sky-700 border border-sky-200 flex items-center gap-1 animate-pulse">
                  <div class="w-1.5 h-1.5 rounded-full bg-sky-500"></div> Исполняется...
                </span>
              {:else if item.status === 'FINISHED'}
                <span class="px-1.5 py-0.5 rounded text-[10px] font-medium bg-emerald-50 text-emerald-700 border border-emerald-200 flex items-center gap-1">
                  <CheckCircle2 class="w-2.5 h-2.5" /> Завершен
                </span>
              {:else if item.status === 'CANCELLED'}
                <span class="px-1.5 py-0.5 rounded text-[10px] font-medium bg-slate-100 text-slate-600 border border-slate-200 flex items-center gap-1">
                  Остановлен
                </span>
              {:else}
                <span class="px-1.5 py-0.5 rounded text-[10px] font-medium bg-red-50 text-red-700 border border-red-200 flex items-center gap-1">
                  <XCircle class="w-2.5 h-2.5" /> Ошибка
                </span>
              {/if}
            </div>
          </div>

          <!-- Текст SQL -->
          <div
            onclick={() => onInsertQuery(item.query_text)}
            title="Кликните, чтобы вставить SQL в редактор"
            class="font-mono text-[11px] text-slate-700 hover:text-sky-700 transition cursor-pointer line-clamp-2 break-all bg-white p-1.5 rounded-md border border-slate-200"
          >
            {item.query_text}
          </div>

          <!-- Метрики и кнопки действий -->
          <div class="flex items-center justify-between pt-1 border-t border-slate-200 text-[10px] text-slate-500">
            <div class="flex items-center gap-2">
              {#if item.status === 'FINISHED'}
                <span class="flex items-center gap-0.5 text-emerald-700 font-medium">
                  <Layers class="w-2.5 h-2.5" /> {item.rows_count.toLocaleString()} стр.
                </span>
              {/if}
              <span>{(item.execution_time_ms / 1000).toFixed(1)} с</span>
            </div>

            <!-- Действия -->
            <div class="flex items-center gap-1">
              <!-- Кнопка открыть результат -->
              {#if item.has_cached_result}
                <button
                  onclick={() => onLoadResult(item.id, item.cluster_name)}
                  class="flex items-center gap-1 px-2 py-0.5 rounded-md bg-sky-50 hover:bg-sky-100 border border-sky-200 text-sky-700 font-semibold transition cursor-pointer shadow-2xs"
                  title="Загрузить сохраненный результат в таблицу"
                >
                  <Eye class="w-3 h-3" />
                  <span>Результат</span>
                </button>
              {/if}

              <!-- Кнопка остановить и удалить -->
              <button
                onclick={() => handleDelete(item)}
                disabled={actionLoading[item.id]}
                class="flex items-center gap-1 p-1 rounded-md hover:bg-red-50 text-slate-400 hover:text-red-600 transition cursor-pointer disabled:opacity-50"
                title={item.status === 'RUNNING' || item.status === 'QUEUED' ? 'Остановить выполнение и удалить из очереди' : 'Удалить из очереди'}
              >
                {#if item.status === 'RUNNING' || item.status === 'QUEUED'}
                  <Square class="w-3 h-3 text-red-600 fill-current" />
                {:else}
                  <Trash2 class="w-3.5 h-3.5" />
                {/if}
              </button>
            </div>
          </div>
        </div>
      {/each}
    {/if}
  </div>
</div>
