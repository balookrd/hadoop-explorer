<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '../api/client';
  import type { ClusterDetailResponse, ColumnMeta, HistoryItem } from '../types';
  import { Database, Table, ChevronRight, ChevronDown, Clock, Search, RefreshCw, Play, Layers } from 'lucide-svelte';

  let {
    clusterDetails,
    selectedMetastoreId = $bindable(''),
    targetLanguage = 'pyspark',
    username = null,
    onSelectTable,
    onRestoreHistory
  }: {
    clusterDetails: ClusterDetailResponse | null;
    selectedMetastoreId: string;
    targetLanguage?: 'pyspark' | 'scalaspark' | 'sql';
    username?: string | null;
    onSelectTable: (tableName: string) => void;
    onRestoreHistory: (item: HistoryItem) => void;
  } = $props();

  let activeTab = $state<'schema' | 'history'>('schema');
  let searchQuery = $state('');

  // Состояние схемы
  let databases = $state<string[]>([]);
  let expandedDbs = $state<Record<string, boolean>>({});
  let tablesByDb = $state<Record<string, string[]>>({});
  let expandedTables = $state<Record<string, boolean>>({});
  let columnsByTable = $state<Record<string, ColumnMeta[]>>({});
  let loadingSchema = $state(false);

  // Состояние истории
  let historyItems = $state<HistoryItem[]>([]);
  let loadingHistory = $state(false);

  $effect(() => {
    if (clusterDetails) {
      if (!selectedMetastoreId && clusterDetails.metastores.length > 0) {
        selectedMetastoreId = clusterDetails.metastores.find((m) => m.is_default)?.id || clusterDetails.metastores[0].id;
      }
      loadDatabases();
    }
  });

  async function loadDatabases() {
    if (!clusterDetails) return;
    loadingSchema = true;
    try {
      databases = await api.getDatabases(clusterDetails.id, selectedMetastoreId);
      expandedDbs = {};
      if (databases.length > 0) {
        toggleDb(databases[0]);
      }
    } catch (err) {
      console.error('Ошибка загрузки баз данных', err);
    } finally {
      loadingSchema = false;
    }
  }

  async function toggleDb(db: string) {
    if (!clusterDetails) return;
    const dbKey = `${clusterDetails.id}.${selectedMetastoreId}.${db}`;
    expandedDbs[dbKey] = !expandedDbs[dbKey];
    if (expandedDbs[dbKey] && !tablesByDb[dbKey]) {
      try {
        const tbls = await api.getTables(clusterDetails.id, db, selectedMetastoreId);
        tablesByDb[dbKey] = tbls;
      } catch (err) {
        console.error('Ошибка загрузки таблиц', err);
      }
    }
  }

  async function toggleTable(db: string, tbl: string) {
    if (!clusterDetails) return;
    const key = `${clusterDetails.id}.${selectedMetastoreId}.${db}.${tbl}`;
    expandedTables[key] = !expandedTables[key];
    if (expandedTables[key] && !columnsByTable[key]) {
      try {
        const cols = await api.getColumns(clusterDetails.id, db, tbl, selectedMetastoreId);
        columnsByTable[key] = cols;
      } catch (err) {
        console.error('Ошибка загрузки колонок', err);
      }
    }
  }

  async function loadHistory() {
    if (!username) {
      historyItems = [];
      return;
    }
    loadingHistory = true;
    try {
      historyItems = await api.getHistory(50);
    } catch (err) {
      console.error('Ошибка загрузки истории', err);
    } finally {
      loadingHistory = false;
    }
  }

  let prevUsername = $state<string | null>(null);
  $effect(() => {
    if (username !== prevUsername) {
      prevUsername = username;
      historyItems = [];
      if (username && activeTab === 'history') {
        loadHistory();
      }
    }
  });

  export function refreshHistory() {
    if (username) {
      loadHistory();
    }
  }

  export function clearHistory() {
    historyItems = [];
  }

  function handleMetastoreChange(id: string) {
    selectedMetastoreId = id;
    loadDatabases();
  }
</script>

<aside class="w-full bg-slate-50 dark:bg-slate-900 border-r border-slate-200 dark:border-slate-800 flex flex-col h-full select-none shrink-0 overflow-hidden">
  <!-- Вкладки Сайдбара: Схема / История -->
  <div class="flex border-b border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900">
    <button
      onclick={() => (activeTab = 'schema')}
      class="flex-1 py-2.5 text-xs font-semibold flex items-center justify-center gap-1.5 border-b-2 transition cursor-pointer {activeTab === 'schema' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200'}"
    >
      <Database class="w-3.5 h-3.5" />
      <span>Метастор</span>
    </button>
    <button
      onclick={() => {
        activeTab = 'history';
        loadHistory();
      }}
      class="flex-1 py-2.5 text-xs font-semibold flex items-center justify-center gap-1.5 border-b-2 transition cursor-pointer {activeTab === 'history' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200'}"
    >
      <Clock class="w-3.5 h-3.5" />
      <span>История</span>
    </button>
  </div>

  {#if activeTab === 'schema'}
    <!-- Выпадающий список Metastore -->
    {#if clusterDetails && clusterDetails.metastores.length > 1}
      <div class="p-2 border-b border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900">
        <label class="block text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">Hive Metastore</label>
        <select
          class="w-full text-xs font-semibold border border-slate-200 dark:border-slate-800 rounded-lg p-1.5 bg-slate-50 dark:bg-slate-950 text-slate-800 dark:text-slate-200 focus:outline-none focus:border-amber-500"
          value={selectedMetastoreId}
          onchange={(e) => handleMetastoreChange(e.currentTarget.value)}
        >
          {#each clusterDetails.metastores as m}
            <option value={m.id}>{m.name}</option>
          {/each}
        </select>
      </div>
    {/if}

    <!-- Поиск по каталогу -->
    <div class="p-2 border-b border-slate-200 dark:border-slate-800 bg-white/50 dark:bg-slate-900/50">
      <div class="relative">
        <Search class="w-3.5 h-3.5 absolute left-2 top-2 text-slate-400" />
        <input
          type="text"
          placeholder="Поиск таблиц..."
          class="w-full pl-7 pr-2 py-1 text-xs bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-100 border border-slate-200 dark:border-slate-800 rounded-lg focus:outline-none focus:border-amber-500 placeholder-slate-400 dark:placeholder-slate-500"
          bind:value={searchQuery}
        />
      </div>
    </div>

    <!-- Дерево баз данных и таблиц -->
    <div class="flex-1 overflow-y-auto p-2 space-y-1 text-xs">
      {#if loadingSchema}
        <div class="flex items-center justify-center py-8 text-slate-400 gap-2">
          <RefreshCw class="w-4 h-4 animate-spin text-amber-500" />
          <span>Загрузка каталога...</span>
        </div>
      {:else}
        {#each databases as db}
          {@const dbKey = `${clusterDetails?.id}.${selectedMetastoreId}.${db}`}
          <div>
            <button
              onclick={() => toggleDb(db)}
              class="w-full flex items-center gap-1.5 px-2 py-1.5 rounded-lg text-slate-700 dark:text-slate-200 hover:bg-slate-200/60 dark:hover:bg-slate-800/60 font-medium transition cursor-pointer text-left truncate"
            >
              {#if expandedDbs[dbKey]}
                <ChevronDown class="w-3.5 h-3.5 text-slate-400 shrink-0" />
              {:else}
                <ChevronRight class="w-3.5 h-3.5 text-slate-400 shrink-0" />
              {/if}
              <Database class="w-3.5 h-3.5 text-amber-500 shrink-0" />
              <span class="truncate font-semibold">{db}</span>
            </button>

            {#if expandedDbs[dbKey] && tablesByDb[dbKey]}
              <div class="pl-4 ml-2 border-l border-slate-200/80 dark:border-slate-800 space-y-0.5 mt-0.5">
                {#each tablesByDb[dbKey].filter((t) => !searchQuery || t.toLowerCase().includes(searchQuery.toLowerCase())) as tbl}
                  {@const key = `${clusterDetails?.id}.${selectedMetastoreId}.${db}.${tbl}`}
                  <div>
                    <div class="flex items-center justify-between group rounded-lg hover:bg-slate-200/60 dark:hover:bg-slate-800/60 pr-1">
                      <button
                        onclick={() => toggleTable(db, tbl)}
                        class="flex-1 flex items-center gap-1.5 px-1.5 py-1 text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-slate-100 transition cursor-pointer text-left truncate"
                      >
                        {#if expandedTables[key]}
                          <ChevronDown class="w-3 h-3 text-slate-400 shrink-0" />
                        {:else}
                          <ChevronRight class="w-3 h-3 text-slate-400 shrink-0" />
                        {/if}
                        <Table class="w-3.5 h-3.5 text-sky-500 shrink-0" />
                        <span class="truncate">{tbl}</span>
                      </button>

                      <button
                        onclick={() => onSelectTable(`${db}.${tbl}`)}
                        class="opacity-0 group-hover:opacity-100 p-1 text-[10px] text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-950/60 rounded transition cursor-pointer"
                        title={targetLanguage === 'sql' ? 'Вставить SELECT в редактор' : 'Вставить чтение таблицы в код'}
                      >
                        Вставить
                      </button>
                    </div>

                    <!-- Колонки таблицы -->
                    {#if expandedTables[key] && columnsByTable[key]}
                      <div class="pl-4 ml-2 border-l border-slate-200/60 dark:border-slate-800 py-1 space-y-1">
                        {#each columnsByTable[key] as col}
                          <div class="flex items-center justify-between text-[11px] text-slate-500 dark:text-slate-400 px-1 font-mono">
                            <span class="truncate text-slate-700 dark:text-slate-300">{col.name}</span>
                            <span class="text-[10px] text-slate-400 shrink-0 ml-1">{col.type}</span>
                          </div>
                        {/each}
                      </div>
                    {/if}
                  </div>
                {/each}
              </div>
            {/if}
          </div>
        {/each}
      {/if}
    </div>

  {:else}
    <!-- Вкладка История -->
    <div class="flex-1 overflow-y-auto p-2 space-y-2 text-xs">
      {#if loadingHistory}
        <div class="flex items-center justify-center py-8 text-slate-400 gap-2">
          <RefreshCw class="w-4 h-4 animate-spin text-amber-500" />
          <span>Загрузка истории...</span>
        </div>
      {:else if historyItems.length === 0}
        <div class="py-8 text-center text-slate-400">
          <p>История пуста</p>
        </div>
      {:else}
        {#each historyItems as item}
          <button
            onclick={() => onRestoreHistory(item)}
            class="w-full text-left p-2.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:border-amber-400 dark:hover:border-amber-500/60 hover:shadow-xs transition cursor-pointer"
          >
            <div class="flex items-center justify-between text-[10px] mb-1">
              <span class="font-bold uppercase px-1.5 py-0.5 rounded {item.status === 'FINISHED' ? 'bg-emerald-50 dark:bg-emerald-950/50 text-emerald-700 dark:text-emerald-400' : 'bg-red-50 dark:bg-red-950/50 text-red-700 dark:text-red-400'}">
                {item.status}
              </span>
              <span class="text-slate-400 font-mono">
                {new Date(item.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </span>
            </div>
            <div class="font-mono text-[11px] text-slate-800 dark:text-slate-200 line-clamp-2 bg-slate-50 dark:bg-slate-950 p-1.5 rounded-md border border-slate-100 dark:border-slate-800 mb-1.5">
              {item.code}
            </div>
            <div class="flex items-center justify-between text-[10px] text-slate-500 dark:text-slate-400">
              <span>{item.language.toUpperCase()}</span>
              <span>{(item.execution_time_ms / 1000).toFixed(1)}с</span>
            </div>
          </button>
        {/each}
      {/if}
    </div>
  {/if}
</aside>
