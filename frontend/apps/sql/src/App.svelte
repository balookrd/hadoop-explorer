<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { api } from './api/client';
  import type { UserSession, ClusterSummary, ColumnMeta } from './types';
  import Header from './components/Header.svelte';
  import Sidebar from './components/Sidebar.svelte';
  import SqlEditor from './components/SqlEditor.svelte';
  import QueryToolbar from './components/QueryToolbar.svelte';
  import ResultsGrid from './components/ResultsGrid.svelte';
  import LoginModal from './components/LoginModal.svelte';
  import AIAssistantModal from './components/AIAssistantModal.svelte';
  import type { AIIssue } from './types';
  import { Plus, X, Terminal, Bell } from 'lucide-svelte';


  interface Tab {
    id: string;
    title: string;
    query: string;
    columns: ColumnMeta[];
    rows: any[][];
    totalRows: number;
    isRunning: boolean;
    statusText: string;
    executionTimeMs: number;
    errorMessage: string | null;
    queryId: string | null;
    closeStream?: () => void;
  }

  let user = $state<UserSession | null>(null);
  let clusters = $state<ClusterSummary[]>([]);
  let selectedClusterId = $state<string>('');
  let sidebarRef: Sidebar | null = null;
  let unsubscribeNotifications: (() => void) | null = null;

  // Вкладки редактора
  let tabs = $state<Tab[]>([
    {
      id: 'tab-1',
      title: 'Запрос 1',
      query: 'SELECT \n  custkey, \n  name, \n  acctbal, \n  mktsegment \nFROM tpch.sf1.customer \nWHERE acctbal > 5000 \nORDER BY acctbal DESC \nLIMIT 50;',
      columns: [],
      rows: [],
      totalRows: 0,
      isRunning: false,
      statusText: '',
      executionTimeMs: 0,
      errorMessage: null,
      queryId: null
    }
  ]);
  let activeTabId = $state<string>('tab-1');

  const activeTab = $derived(
    tabs.find((t) => t.id === activeTabId) || tabs[0]
  );

  let editorHeightPercent = $state(45);
  let runEditorTrigger: (() => void) | null = $state(null);
  let sqlEditorRef: any = $state(null);

  // Состояние ИИ-ассистента
  let isAiModalOpen = $state(false);
  let aiInitialTab = $state<'check' | 'explain' | 'optimize' | 'fix'>('check');


  onMount(async () => {
    // Запрос разрешения на браузерные системные уведомления
    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }

    try {
      user = await api.getMe();
      await loadClusters();
      initNotificationListener();
    } catch (_) {
      // Пользователь не авторизован
    }
  });

  onDestroy(() => {
    if (unsubscribeNotifications) {
      unsubscribeNotifications();
    }
  });

  function initNotificationListener() {
    if (unsubscribeNotifications) unsubscribeNotifications();
    unsubscribeNotifications = api.listenUserNotifications((event) => {
      if (event.type === 'QUERY_FINISHED') {
        if (sidebarRef) {
          sidebarRef.refreshHistory();
          sidebarRef.refreshQueue();
        }

        // HTML5 Desktop Notification
        if ('Notification' in window && Notification.permission === 'granted') {
          const title = event.status === 'FINISHED' ? '✓ Запрос успешно завершен' : '✗ Ошибка запроса';
          const body = `${event.cluster_name}: ${event.rows_count || 0} строк за ${(event.duration_ms / 1000).toFixed(1)} с`;
          new Notification(title, { body, icon: '/favicon.svg' });
        }
      } else if (event.type === 'QUERY_QUEUED' || event.type === 'QUERY_STARTED' || event.type === 'QUERY_REMOVED_FROM_QUEUE') {
        if (sidebarRef) {
          sidebarRef.refreshQueue();
        }
      }
    });
  }

  async function loadClusters() {
    try {
      clusters = await api.getClusters();
      if (clusters.length > 0 && !selectedClusterId) {
        selectedClusterId = clusters[0].id;
      }
    } catch (err) {
      console.error('Ошибка загрузки кластеров', err);
    }
  }

  function handleLoginSuccess(u: UserSession) {
    user = u;
    loadClusters();
    initNotificationListener();
  }

  async function handleLogout() {
    if (unsubscribeNotifications) {
      unsubscribeNotifications();
      unsubscribeNotifications = null;
    }
    await api.logout();
    user = null;
    clusters = [];
  }

  function createTab() {
    const newId = `tab-${Date.now()}`;
    tabs.push({
      id: newId,
      title: `Запрос ${tabs.length + 1}`,
      query: 'SELECT * FROM tpch.sf1.orders LIMIT 20;',
      columns: [],
      rows: [],
      totalRows: 0,
      isRunning: false,
      statusText: '',
      executionTimeMs: 0,
      errorMessage: null,
      queryId: null
    });
    activeTabId = newId;
  }

  function closeTab(tabId: string, event: MouseEvent) {
    event.stopPropagation();
    if (tabs.length === 1) return;
    const tabToClose = tabs.find((t) => t.id === tabId);
    if (tabToClose?.closeStream) {
      tabToClose.closeStream();
    }
    tabs = tabs.filter((t) => t.id !== tabId);
    if (activeTabId === tabId) {
      activeTabId = tabs[0].id;
    }
  }

  async function executeQuery(queryToRun: string) {
    const cleanQuery = queryToRun.trim().replace(/;+$/, '').trim();
    if (!activeTab || !selectedClusterId || activeTab.isRunning || !cleanQuery) return;

    activeTab.isRunning = true;
    activeTab.statusText = 'Постановка в очередь...';
    activeTab.errorMessage = null;
    activeTab.columns = [];
    activeTab.rows = [];
    activeTab.totalRows = 0;
    activeTab.executionTimeMs = 0;

    const startTime = Date.now();
    const timer = setInterval(() => {
      if (activeTab.isRunning) {
        activeTab.executionTimeMs = Date.now() - startTime;
      } else {
        clearInterval(timer);
      }
    }, 100);

    try {
      const resp = await api.executeQuery(selectedClusterId, cleanQuery);
      activeTab.queryId = resp.query_id;
      activeTab.statusText = resp.message;
      if (sidebarRef) sidebarRef.refreshQueue();

      activeTab.closeStream = api.streamQueryEvents(
        resp.query_id,
        (event) => {
          if (event.type === 'status') {
            activeTab.statusText = event.message || event.status;
          } else if (event.type === 'columns') {
            activeTab.columns = event.columns;
          } else if (event.type === 'rows') {
            activeTab.rows = [...activeTab.rows, ...event.rows];
            activeTab.totalRows = event.total_rows;
          } else if (event.type === 'finished') {
            activeTab.isRunning = false;
            activeTab.statusText = event.message;
            clearInterval(timer);
            if (sidebarRef) {
              sidebarRef.refreshHistory();
              sidebarRef.refreshQueue();
            }
          } else if (event.type === 'error') {
            activeTab.isRunning = false;
            activeTab.errorMessage = event.error;
            activeTab.statusText = 'Ошибка исполнения';
            clearInterval(timer);
            if (sidebarRef) {
              sidebarRef.refreshHistory();
              sidebarRef.refreshQueue();
            }
          } else if (event.type === 'stream_end') {
            activeTab.isRunning = false;
            if (event.duration_ms) {
              activeTab.executionTimeMs = event.duration_ms;
            }
            clearInterval(timer);
            if (sidebarRef) {
              sidebarRef.refreshHistory();
              sidebarRef.refreshQueue();
            }
          }
        },
        (err) => {
          activeTab.isRunning = false;
          clearInterval(timer);
        }
      );
    } catch (err: any) {
      activeTab.isRunning = false;
      activeTab.errorMessage = err.message || 'Ошибка отправки запроса';
      activeTab.statusText = 'Ошибка';
      clearInterval(timer);
    }
  }

  async function cancelQuery() {
    if (!activeTab || !activeTab.queryId) return;
    try {
      await api.deleteFromQueue(activeTab.queryId);
      activeTab.statusText = 'Запрос отменен и удален из очереди';
      activeTab.isRunning = false;
      if (activeTab.closeStream) activeTab.closeStream();
      if (sidebarRef) sidebarRef.refreshQueue();
    } catch (err: any) {
      console.error('Ошибка отмены', err);
    }
  }

  async function handleLoadCachedResult(queryId: string, clusterName: string) {
    if (!activeTab) return;
    try {
      activeTab.statusText = 'Загрузка сохраненного результата...';
      const cached = await api.getQueryResult(queryId);
      activeTab.columns = cached.columns;
      activeTab.rows = cached.rows;
      activeTab.totalRows = cached.total_rows;
      activeTab.errorMessage = null;
      activeTab.isRunning = false;
      activeTab.statusText = `Сохраненный результат (${cached.total_rows} строк, ${clusterName})`;
    } catch (err: any) {
      alert(`Не удалось загрузить результат: ${err.message}`);
    }
  }

  function handleSelectTable(tableName: string) {
    if (activeTab) {
      activeTab.query = `SELECT * \nFROM ${tableName} \nLIMIT 50;`;
    }
  }

  function handleSelectHistoryQuery(historyQuery: string) {
    if (activeTab) {
      activeTab.query = historyQuery;
    }
  }

  async function handleSaveQuery() {
    if (!activeTab) return;
    const title = prompt('Название сохраненного запроса:', activeTab.title);
    if (!title) return;
    try {
      await api.saveQuery(title, activeTab.query, selectedClusterId);
      alert('Запрос сохранен в избранное');
    } catch (err: any) {
      alert(`Ошибка: ${err.message}`);
    }
  }

  // Обработчики ИИ-ассистента
  function handleOpenAi(tab: 'check' | 'explain' | 'optimize' | 'fix' = 'check') {
    aiInitialTab = tab;
    isAiModalOpen = true;
  }

  function handleApplyAiSql(newSql: string) {
    if (activeTab) {
      activeTab.query = newSql;
    }
  }

  function handleHighlightIssues(issues: AIIssue[]) {
    if (sqlEditorRef && sqlEditorRef.setAiMarkers) {
      sqlEditorRef.setAiMarkers(issues);
    }
  }

  function handleNavigateToLine(line: number) {
    if (sqlEditorRef && sqlEditorRef.revealLine) {
      sqlEditorRef.revealLine(line);
    }
  }

  async function handleFormatSql() {
    if (!activeTab || !activeTab.query.trim()) return;
    try {
      const res = await api.formatSql(activeTab.query, selectedClusterId, currentCluster?.type);
      if (res && res.formatted_sql) {
        activeTab.query = res.formatted_sql;
      }
    } catch (err: any) {
      console.error('Ошибка форматирования SQL:', err);
    }
  }

  const currentCluster = $derived(
    clusters.find((c) => c.id === selectedClusterId) || clusters[0]
  );
</script>

<div class="h-screen w-screen flex flex-col bg-slate-50 text-slate-800 font-sans overflow-hidden">
  <Header
    {user}
    {clusters}
    bind:selectedClusterId
    onLogout={handleLogout}
  />

  <div class="flex-1 flex overflow-hidden">
    <Sidebar
      bind:this={sidebarRef}
      clusterId={selectedClusterId}
      onSelectTable={handleSelectTable}
      onSelectHistoryQuery={handleSelectHistoryQuery}
      onLoadCachedResult={handleLoadCachedResult}
    />

    <main class="flex-1 flex flex-col overflow-hidden bg-slate-50">
      <!-- Вкладки запросов -->
      <div class="h-10 bg-slate-100/80 border-b border-slate-200 flex items-center px-2.5 gap-1 overflow-x-auto select-none shrink-0">
        {#each tabs as tab}
          <div
            onclick={() => (activeTabId = tab.id)}
            class="group flex items-center gap-2 px-3 py-1.5 text-xs rounded-t-lg transition cursor-pointer border-t-2 {activeTabId === tab.id ? 'bg-white border-sky-600 text-sky-700 font-semibold shadow-xs' : 'border-transparent text-slate-600 hover:bg-slate-200/60 hover:text-slate-900'}"
          >
            <Terminal class="w-3.5 h-3.5 text-slate-400 {activeTabId === tab.id ? 'text-sky-600' : ''}" />
            <span>{tab.title}</span>
            <button
              onclick={(e) => closeTab(tab.id, e)}
              class="opacity-0 group-hover:opacity-100 p-0.5 rounded hover:bg-slate-100 text-slate-400 hover:text-slate-700 transition cursor-pointer"
            >
              <X class="w-3 h-3" />
            </button>
          </div>
        {/each}

        <button
          onclick={createTab}
          class="p-1 rounded-md hover:bg-slate-200 text-slate-500 hover:text-sky-600 transition cursor-pointer ml-1"
          title="Новая вкладка"
        >
          <Plus class="w-4 h-4" />
        </button>
      </div>

      {#if activeTab}
        <QueryToolbar
          isRunning={activeTab.isRunning}
          statusText={activeTab.statusText}
          executionTimeMs={activeTab.executionTimeMs}
          rowsCount={activeTab.totalRows}
          onRun={() => {
            if (runEditorTrigger) {
              runEditorTrigger();
            } else {
              executeQuery(activeTab.query);
            }
          }}
          onCancel={cancelQuery}
          onSave={handleSaveQuery}
          onOpenAi={handleOpenAi}
          onFormat={handleFormatSql}
        />

        <div style="height: {editorHeightPercent}%" class="w-full shrink-0 border-b border-slate-200 overflow-hidden bg-white">
          <SqlEditor
            bind:this={sqlEditorRef}
            bind:value={activeTab.query}
            onExecute={(q) => executeQuery(q)}
            registerTrigger={(fn) => { runEditorTrigger = fn; }}
          />
        </div>

        <div class="flex-1 w-full overflow-hidden">
          <ResultsGrid
            columns={activeTab.columns}
            rows={activeTab.rows}
            errorMessage={activeTab.errorMessage}
            totalRows={activeTab.totalRows}
            onFixWithAi={() => handleOpenAi('fix')}
          />
        </div>
      {/if}
    </main>
  </div>

  {#if !user}
    <LoginModal onLoginSuccess={handleLoginSuccess} />
  {/if}

  <AIAssistantModal
    bind:isOpen={isAiModalOpen}
    initialTab={aiInitialTab}
    sqlQuery={activeTab ? activeTab.query : ''}
    clusterId={selectedClusterId}
    engineType={currentCluster ? currentCluster.type : 'trino'}
    errorMessage={activeTab?.errorMessage || ''}
    onApplySql={handleApplyAiSql}
    onHighlightIssues={handleHighlightIssues}
    onNavigateToLine={handleNavigateToLine}
  />
</div>

