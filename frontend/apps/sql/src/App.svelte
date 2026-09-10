<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { api } from './api/client';
  import type { UserSession, ClusterSummary, ColumnMeta } from './types';
  import { Header, LoginModal } from '@hadoop-explorer/common';
  import Sidebar from './components/Sidebar.svelte';
  import SqlEditor from './components/SqlEditor.svelte';
  import QueryToolbar from './components/QueryToolbar.svelte';
  import ResultsGrid from './components/ResultsGrid.svelte';
  import type { AIIssue } from './types';
  import { Plus, X, Terminal, Bell, Database } from 'lucide-svelte';


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

  let authLoading = $state(true);
  let user = $state<UserSession | null>(null);
  let authErrorMessage = $state<string | null>(null);
  let clusters = $state<ClusterSummary[]>([]);
  let selectedClusterId = $state<string>('');
  let sidebarRef = $state<Sidebar | null>(null);
  let unsubscribeNotifications: (() => void) | null = null;
  let unsubscribeAuth: (() => void) | null = null;

  const DEFAULT_QUERY = 'SELECT \n  custkey, \n  name, \n  acctbal, \n  mktsegment \nFROM tpch.sf1.customer \nWHERE acctbal > 5000 \nORDER BY acctbal DESC \nLIMIT 50;';

  function createInitialTab(id = 'tab-1', title = 'Запрос 1'): Tab {
    return {
      id,
      title,
      query: DEFAULT_QUERY,
      columns: [],
      rows: [],
      totalRows: 0,
      isRunning: false,
      statusText: '',
      executionTimeMs: 0,
      errorMessage: null,
      queryId: null
    };
  }

  // Вкладки редактора
  let tabs = $state<Tab[]>([createInitialTab()]);
  let activeTabId = $state<string>('tab-1');

  const activeTab = $derived(
    tabs.find((t) => t.id === activeTabId) || tabs[0]
  );

  let sidebarWidth = $state(320);
  let isResizingSidebar = $state(false);
  let isResizingEditor = $state(false);
  let mainAreaRef = $state<HTMLDivElement | null>(null);

  let editorHeightPercent = $state(45);
  let runEditorTrigger: (() => void) | null = $state(null);
  let sqlEditorRef: any = $state(null);

  // Обработчики изменения размера сайдбара (каталогов)
  function handleSidebarMouseDown(e: MouseEvent) {
    e.preventDefault();
    isResizingSidebar = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    window.addEventListener('mousemove', handleSidebarMouseMove);
    window.addEventListener('mouseup', handleSidebarMouseUp);
  }

  function handleSidebarMouseMove(e: MouseEvent) {
    if (!isResizingSidebar) return;
    const newWidth = Math.max(200, Math.min(window.innerWidth * 0.55, e.clientX));
    sidebarWidth = Math.round(newWidth);
    window.dispatchEvent(new Event('resize'));
  }

  function handleSidebarMouseUp() {
    if (!isResizingSidebar) return;
    isResizingSidebar = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    window.removeEventListener('mousemove', handleSidebarMouseMove);
    window.removeEventListener('mouseup', handleSidebarMouseUp);
    saveStateToStorage(true);
    window.dispatchEvent(new Event('resize'));
  }

  // Обработчики изменения размера редактора кода и результатов
  function handleEditorMouseDown(e: MouseEvent) {
    e.preventDefault();
    isResizingEditor = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
    window.addEventListener('mousemove', handleEditorMouseMove);
    window.addEventListener('mouseup', handleEditorMouseUp);
  }

  function handleEditorMouseMove(e: MouseEvent) {
    if (!isResizingEditor || !mainAreaRef) return;
    const rect = mainAreaRef.getBoundingClientRect();
    if (rect.height <= 0) return;
    const relativeY = e.clientY - rect.top;
    const newPercent = (relativeY / rect.height) * 100;
    if (newPercent >= 15 && newPercent <= 85) {
      editorHeightPercent = Math.round(newPercent * 10) / 10;
      window.dispatchEvent(new Event('resize'));
    }
  }

  function handleEditorMouseUp() {
    if (!isResizingEditor) return;
    isResizingEditor = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    window.removeEventListener('mousemove', handleEditorMouseMove);
    window.removeEventListener('mouseup', handleEditorMouseUp);
    saveStateToStorage(true);
    window.dispatchEvent(new Event('resize'));
  }

  onDestroy(() => {
    window.removeEventListener('mousemove', handleSidebarMouseMove);
    window.removeEventListener('mouseup', handleSidebarMouseUp);
    window.removeEventListener('mousemove', handleEditorMouseMove);
    window.removeEventListener('mouseup', handleEditorMouseUp);
  });

  // Состояние ИИ-ассистента
  let isAiModalOpen = $state(false);
  let aiInitialTab = $state<'check' | 'explain' | 'optimize' | 'fix' | 'generate'>('check');

  let saveTimeout: any = null;

  function getStorageKey(username?: string | null): string {
    return `sql_workspace_${username || 'guest'}`;
  }

  function applyWorkspaceState(state: any): boolean {
    if (!state || typeof state !== 'object') return false;

    if (Array.isArray(state.tabs) && state.tabs.length > 0) {
      tabs = state.tabs.map((t: any, idx: number) => ({
        id: t.id || `tab-${idx + 1}`,
        title: t.title || `Запрос ${idx + 1}`,
        query: typeof t.query === 'string' ? t.query : DEFAULT_QUERY,
        columns: Array.isArray(t.columns) ? t.columns : [],
        rows: Array.isArray(t.rows) ? t.rows : [],
        totalRows: typeof t.totalRows === 'number' ? t.totalRows : (t.rows?.length || 0),
        isRunning: false,
        statusText: t.statusText || '',
        executionTimeMs: t.executionTimeMs || 0,
        errorMessage: t.errorMessage || null,
        queryId: t.queryId || null
      }));
    } else {
      tabs = [createInitialTab()];
    }

    if (state.activeTabId && tabs.some((t) => t.id === state.activeTabId)) {
      activeTabId = state.activeTabId;
    } else {
      activeTabId = tabs[0].id;
    }

    if (state.selectedClusterId) {
      selectedClusterId = state.selectedClusterId;
    }
    if (typeof state.sidebarWidth === 'number' && state.sidebarWidth >= 180 && state.sidebarWidth <= 800) {
      sidebarWidth = state.sidebarWidth;
    }
    if (typeof state.editorHeightPercent === 'number' && state.editorHeightPercent >= 15 && state.editorHeightPercent <= 85) {
      editorHeightPercent = state.editorHeightPercent;
    }
    return true;
  }

  async function loadUserWorkspace(currentUser: UserSession | null) {
    if (!currentUser) {
      tabs = [createInitialTab()];
      activeTabId = 'tab-1';
      return;
    }

    // 1. Пробуем восстановить состояние из базы данных через API
    try {
      const remoteWs = await api.getWorkspace();
      if (remoteWs && remoteWs.state && applyWorkspaceState(remoteWs.state)) {
        return;
      }
    } catch (err) {
      console.warn('Не удалось загрузить рабочее пространство из БД:', err);
    }

    // 2. Если в БД пусто, пробуем загрузить из локального кэша пользователя
    try {
      const raw = localStorage.getItem(getStorageKey(currentUser.username));
      if (raw) {
        const parsed = JSON.parse(raw);
        if (applyWorkspaceState(parsed)) {
          // Отправляем в БД
          api.saveWorkspace(parsed).catch(() => {});
          return;
        }
      }
    } catch {}

    // 3. Если данных нет — открываем свежую начальную вкладку
    tabs = [createInitialTab()];
    activeTabId = 'tab-1';
  }

  function saveStateToStorage(immediate = false) {
    if (saveTimeout) clearTimeout(saveTimeout);
    const saveFn = () => {
      try {
        const stateToSave = {
          selectedClusterId,
          activeTabId,
          sidebarWidth,
          editorHeightPercent,
          tabs: tabs.map((t) => ({
            id: t.id,
            title: t.title,
            query: t.query,
            columns: t.columns || [],
            rows: (t.rows || []).slice(0, 200),
            totalRows: t.totalRows || 0,
            executionTimeMs: t.executionTimeMs || 0,
            errorMessage: t.errorMessage || null,
            statusText: t.statusText || '',
            queryId: t.queryId || null
          }))
        };

        // Локальный кэш пользователя
        localStorage.setItem(getStorageKey(user?.username), JSON.stringify(stateToSave));

        // Персистентное сохранение в БД
        if (user) {
          api.saveWorkspace(stateToSave).catch((err) => {
            console.warn('Не удалось сохранить рабочее пространство в БД:', err);
          });
        }
      } catch (err) {
        console.warn('Не удалось сохранить состояние SQL Explorer:', err);
      }
    };

    if (immediate) {
      saveFn();
    } else {
      saveTimeout = setTimeout(saveFn, 400);
    }
  }

  // Автосохранение при редактировании запроса
  $effect(() => {
    const _cur = activeTab?.query;
    saveStateToStorage(false);
  });

  onMount(async () => {
    // Подписка на истечение сессии / 401 Unauthorized
    unsubscribeAuth = api.onUnauthorized((msg) => {
      user = null;
      clusters = [];
      authErrorMessage = msg;
      tabs = [createInitialTab()];
      activeTabId = 'tab-1';
      if (unsubscribeNotifications) {
        unsubscribeNotifications();
        unsubscribeNotifications = null;
      }
    });

    // Запрос разрешения на браузерные системные уведомления
    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }

    try {
      user = await api.getMe();
      await loadClusters();
      initNotificationListener();
      await loadUserWorkspace(user);
    } catch (_) {
      try {
        const autoUser = await api.tryAutoLogin();
        if (autoUser) {
          user = autoUser;
          await loadClusters();
          initNotificationListener();
          await loadUserWorkspace(autoUser);
        } else {
          await loadUserWorkspace(null);
        }
      } catch {
        await loadUserWorkspace(null);
      }
    } finally {
      authLoading = false;
    }
  });

  onDestroy(() => {
    if (saveTimeout) clearTimeout(saveTimeout);
    if (unsubscribeNotifications) {
      unsubscribeNotifications();
    }
    if (unsubscribeAuth) {
      unsubscribeAuth();
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

  async function handleLogin(u: string, p: string) {
    const res = await api.login(u, p);
    await handleLoginSuccess(res.user);
  }

  async function handleKerberosSso() {
    const res = await api.kerberosNegotiate();
    const u = res.user || (await api.getMe());
    await handleLoginSuccess(u);
  }

  async function handleLoginSuccess(u: UserSession) {
    user = u;
    await loadClusters();
    initNotificationListener();
    await loadUserWorkspace(u);
  }

  async function handleLogout() {
    if (saveTimeout) clearTimeout(saveTimeout);
    if (unsubscribeNotifications) {
      unsubscribeNotifications();
      unsubscribeNotifications = null;
    }
    await api.logout();
    user = null;
    clusters = [];
    authErrorMessage = null;
    tabs = [createInitialTab()];
    activeTabId = 'tab-1';
  }


  function createTab() {
    const newId = `tab-${Date.now()}`;
    tabs = [
      ...tabs,
      {
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
      }
    ];
    activeTabId = newId;
    saveStateToStorage(true);
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
    saveStateToStorage(true);
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
            saveStateToStorage(true);
            if (sidebarRef) {
              sidebarRef.refreshHistory();
              sidebarRef.refreshQueue();
            }
          } else if (event.type === 'error') {
            activeTab.isRunning = false;
            activeTab.errorMessage = event.error;
            activeTab.statusText = 'Ошибка исполнения';
            clearInterval(timer);
            saveStateToStorage(true);
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
            saveStateToStorage(true);
            if (sidebarRef) {
              sidebarRef.refreshHistory();
              sidebarRef.refreshQueue();
            }
          }
        },
        (err) => {
          activeTab.isRunning = false;
          clearInterval(timer);
          saveStateToStorage(true);
        }
      );
    } catch (err: any) {
      activeTab.isRunning = false;
      activeTab.errorMessage = err.message || 'Ошибка отправки запроса';
      activeTab.statusText = 'Ошибка';
      clearInterval(timer);
      saveStateToStorage(true);
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
      saveStateToStorage(true);
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
      saveStateToStorage(true);
    } catch (err: any) {
      alert(`Не удалось загрузить результат: ${err.message}`);
    }
  }

  function handleSelectTable(tableName: string) {
    if (activeTab) {
      activeTab.query = `SELECT * \nFROM ${tableName} \nLIMIT 50;`;
      saveStateToStorage(true);
    }
  }

  function handleSelectHistoryQuery(historyQuery: string) {
    if (activeTab) {
      activeTab.query = historyQuery;
      saveStateToStorage(true);
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
  function handleOpenAi(tab: 'check' | 'explain' | 'optimize' | 'fix' | 'generate' = 'check') {
    aiInitialTab = tab;
    isAiModalOpen = true;
  }

  function handleApplyAiSql(newSql: string) {
    if (activeTab) {
      activeTab.query = newSql;
      saveStateToStorage(true);
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
        saveStateToStorage(true);
      }
    } catch (err: any) {
      console.error('Ошибка форматирования SQL:', err);
    }
  }

  const currentCluster = $derived(
    clusters.find((c) => c.id === selectedClusterId) || clusters[0]
  );
</script>

{#if authLoading}
  <div class="min-h-screen bg-slate-50 flex flex-col items-center justify-center text-slate-800 gap-3">
    <div class="w-8 h-8 border-3 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
    <span class="text-xs font-medium text-slate-500">Проверка сессии...</span>
  </div>
{:else if !user}
  <LoginModal
    title="SQL Web Explorer"
    subtitle="Аутентификация LDAP & Kerberos SSO"
    icon={Database}
    isModal={false}
    initialError={authErrorMessage}
    onLogin={handleLogin}
    onKerberosSso={handleKerberosSso}
  />
{:else}
  <div class="h-screen w-screen flex flex-col bg-slate-50 text-slate-800 font-sans overflow-hidden">
    <Header
      title="SQL Web Explorer"
      subtitle="Trino & Hive"
      icon={Database}
      {user}
      {clusters}
      bind:selectedClusterId
      onLogout={handleLogout}
    />

  <div class="flex-1 flex overflow-hidden">
    <!-- Сайдбар каталогов и схем с настраиваемой шириной -->
    <div style="width: {sidebarWidth}px;" class="h-full shrink-0 flex">
      <Sidebar
        bind:this={sidebarRef}
        clusterId={selectedClusterId}
        {user}
        onSelectTable={handleSelectTable}
        onSelectHistoryQuery={handleSelectHistoryQuery}
        onLoadCachedResult={handleLoadCachedResult}
      />
    </div>

    <!-- Вертикальный разделитель (ширина каталогов) -->
    <div
      role="separator"
      aria-orientation="vertical"
      tabindex="0"
      onmousedown={handleSidebarMouseDown}
      ondblclick={() => { sidebarWidth = 320; saveStateToStorage(true); window.dispatchEvent(new Event('resize')); }}
      class="w-1.5 hover:w-2 bg-slate-200/80 hover:bg-sky-500 active:bg-sky-600 transition-all cursor-col-resize shrink-0 z-20 flex items-center justify-center group select-none"
      title="Изменить ширину каталогов (двойной клик для сброса)"
    >
      <div class="w-0.5 h-8 bg-slate-400/50 group-hover:bg-white rounded-full transition"></div>
    </div>

    <main class="flex-1 flex flex-col overflow-hidden bg-slate-50 min-w-0">
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
          onOpenAi={handleOpenAi}
          onFormat={handleFormatSql}
        />

        <!-- Область Редактора и Результатов с изменяемым разделением -->
        <div bind:this={mainAreaRef} class="flex-1 flex flex-col overflow-hidden relative min-h-0">
          <div style="height: {editorHeightPercent}%;" class="w-full shrink-0 border-b border-slate-200 overflow-hidden bg-white">
            <SqlEditor
              bind:this={sqlEditorRef}
              bind:value={activeTab.query}
              onExecute={(q) => executeQuery(q)}
              registerTrigger={(fn) => { runEditorTrigger = fn; }}
            />
          </div>

          <!-- Горизонтальный разделитель (высота редактора vs результаты) -->
          <div
            role="separator"
            aria-orientation="horizontal"
            tabindex="0"
            onmousedown={handleEditorMouseDown}
            ondblclick={() => { editorHeightPercent = 45; saveStateToStorage(true); window.dispatchEvent(new Event('resize')); }}
            class="h-1.5 hover:h-2 bg-slate-200/80 hover:bg-sky-500 active:bg-sky-600 transition-all cursor-row-resize shrink-0 z-20 flex items-center justify-center group select-none"
            title="Изменить размер редактора и результатов (двойной клик для сброса)"
          >
            <div class="h-0.5 w-8 bg-slate-400/50 group-hover:bg-white rounded-full transition"></div>
          </div>

          <div style="height: {100 - editorHeightPercent}%;" class="w-full overflow-hidden min-h-0">
            <ResultsGrid
              columns={activeTab.columns}
              rows={activeTab.rows}
              errorMessage={activeTab.errorMessage}
              totalRows={activeTab.totalRows}
              onFixWithAi={() => handleOpenAi('fix')}
            />
          </div>
        </div>
      {/if}
    </main>
  </div>

  {#if isAiModalOpen}
    {#await import('./components/AIAssistantModal.svelte') then { default: AIAssistantModal }}
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
    {/await}
  {/if}
</div>
{/if}

