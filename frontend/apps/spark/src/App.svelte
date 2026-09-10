<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { api } from './api/client';
  import type {
    UserSession,
    ClusterSummary,
    ClusterDetailResponse,
    SparkSessionItem,
    CreateSessionPayload,
    Tab,
    TabResultData,
    HistoryItem
  } from './types';
  import { Header, LoginModal } from '@hadoop-explorer/common';
  import Sidebar from './components/Sidebar.svelte';
  import SessionBar from './components/SessionBar.svelte';
  import SparkSessionWidget from './components/SparkSessionWidget.svelte';
  import SparkEditor from './components/SparkEditor.svelte';
  import ResultsView from './components/ResultsView.svelte';
  import { Flame, Plus, X, Server } from 'lucide-svelte';

  let user = $state<UserSession | null>(null);
  let authErrorMessage = $state<string | null>(null);
  let isAuthChecking = $state(true);
  let isLoginModalOpen = $state(false);
  let clusters = $state<ClusterSummary[]>([]);
  let selectedClusterId = $state<string>('');
  let clusterDetails = $state<ClusterDetailResponse | null>(null);
  let unsubscribeAuth: (() => void) | null = null;

  const mockUsers = [
    {
      username: 'admin_user',
      password: 'password123',
      displayName: 'Александр Админов',
      description: 'Администратор платформы, полный доступ',
      badgeColor: 'text-purple-600'
    },
    {
      username: 'de_user',
      password: 'password123',
      displayName: 'Иван Датаинженеров',
      description: 'Data Engineer, запуск PySpark и Scala',
      badgeColor: 'text-sky-600'
    },
    {
      username: 'analyst_user',
      password: 'password123',
      displayName: 'Анна Аналитикова',
      description: 'Data Analyst, интерактивные запросы',
      badgeColor: 'text-emerald-600'
    }
  ];

  // Сессии
  let activeSessions = $state<SparkSessionItem[]>([]);
  let currentSession = $state<SparkSessionItem | null>(null);
  let isConfigModalOpen = $state(false);
  let runEditorTrigger = $state<(() => void) | null>(null);
  let sessionPayload = $state<Partial<CreateSessionPayload>>({});
  let savedConfigByKind = $state<Record<'pyspark' | 'spark', Partial<CreateSessionPayload>>>({
    pyspark: {},
    spark: {}
  });

  // Выбранный Metastore
  let selectedMetastoreId = $state<string>('');

  const DEFAULT_CODES: Record<'pyspark' | 'scalaspark' | 'sql', string> = {
    pyspark: '# PySpark скрипт в Hadoop Explorer\n# Для отображения таблицы используйте функцию display(df)\n\ndf = spark.read.table("customers")\ndisplay(df.filter("balance > 10000"))\n',
    scalaspark: '// Scala Spark скрипт в Hadoop Explorer\nval df = spark.read.table("customers")\ndf.show(50, false)\n',
    sql: '-- Spark SQL запрос\nSELECT * FROM customers\nWHERE balance > 10000\nLIMIT 50;\n'
  };

  const STORAGE_KEY = 'spark_explorer_state_v2';

  function createEmptyResultData(): TabResultData {
    return {
      columns: [],
      rows: [],
      totalRows: 0,
      logs: '',
      executionTimeMs: 0,
      errorMessage: null,
      executionId: null,
      statusText: '',
      activeResultTab: 'table'
    };
  }

  function createTabObject(
    id: string,
    title: string,
    lang: 'pyspark' | 'scalaspark' | 'sql' = 'pyspark',
    withDefaults = false
  ): Tab {
    const code = withDefaults ? DEFAULT_CODES[lang] : '';
    return {
      id,
      title,
      language: lang,
      code,
      codeBuffers: {
        pyspark: withDefaults ? DEFAULT_CODES.pyspark : '',
        scalaspark: withDefaults ? DEFAULT_CODES.scalaspark : '',
        sql: withDefaults ? DEFAULT_CODES.sql : ''
      },
      resultBuffers: {
        pyspark: createEmptyResultData(),
        scalaspark: createEmptyResultData(),
        sql: createEmptyResultData()
      },
      columns: [],
      rows: [],
      totalRows: 0,
      logs: '',
      isRunning: false,
      statusText: '',
      executionTimeMs: 0,
      errorMessage: null,
      executionId: null,
      activeResultTab: 'table'
    };
  }

  // Вкладки редактора (только первая дефолтная вкладка инициализируется примером кода)
  let tabs = $state<Tab[]>([createTabObject('tab-1', 'Скрипт 1 (PySpark)', 'pyspark', true)]);
  let activeTabId = $state<string>('tab-1');

  const activeTab = $derived(
    tabs.find((t) => t.id === activeTabId) || tabs[0]
  );

  const currentTabKind = $derived<'pyspark' | 'spark'>(
    getTargetKind(activeTab?.language || 'pyspark')
  );

  let sidebarWidth = $state(280);
  let isResizingSidebar = $state(false);
  let isResizingEditor = $state(false);
  let mainAreaRef = $state<HTMLDivElement | null>(null);

  let editorHeightPercent = $state(50);
  let saveTimeout: any = null;

  function getStorageKey(uname?: string | null): string {
    return uname ? `spark_explorer_state_${uname}` : 'spark_explorer_state_anonymous';
  }

  function applyWorkspaceState(state: any): boolean {
    if (!state || !Array.isArray(state.tabs) || state.tabs.length === 0) return false;
    tabs = state.tabs.map((t: any) => {
      const lang = t.language || 'pyspark';
      const defaultBuf = {
        pyspark: createEmptyResultData(),
        scalaspark: createEmptyResultData(),
        sql: createEmptyResultData()
      };
      if (t.resultBuffers && typeof t.resultBuffers === 'object') {
        defaultBuf.pyspark = t.resultBuffers.pyspark || createEmptyResultData();
        defaultBuf.scalaspark = t.resultBuffers.scalaspark || createEmptyResultData();
        defaultBuf.sql = t.resultBuffers.sql || createEmptyResultData();
      } else {
        defaultBuf[lang as 'pyspark' | 'scalaspark' | 'sql'] = {
          columns: t.columns || [],
          rows: t.rows || [],
          totalRows: t.totalRows || 0,
          logs: t.logs || '',
          executionTimeMs: t.executionTimeMs || 0,
          errorMessage: t.errorMessage || null,
          executionId: t.executionId || null,
          statusText: t.statusText || '',
          activeResultTab: t.activeResultTab || 'table'
        };
      }

      const activeRes = defaultBuf[lang as 'pyspark' | 'scalaspark' | 'sql'] || createEmptyResultData();

      return {
        id: t.id,
        title: t.title,
        language: lang,
        code: t.code !== undefined ? t.code : (t.id === 'tab-1' ? DEFAULT_CODES[lang as 'pyspark' | 'scalaspark' | 'sql'] : ''),
        codeBuffers: t.codeBuffers || {
          pyspark: t.id === 'tab-1' ? DEFAULT_CODES.pyspark : '',
          scalaspark: t.id === 'tab-1' ? DEFAULT_CODES.scalaspark : '',
          sql: t.id === 'tab-1' ? DEFAULT_CODES.sql : ''
        },
        resultBuffers: defaultBuf,
        columns: activeRes.columns || [],
        rows: activeRes.rows || [],
        totalRows: activeRes.totalRows || 0,
        logs: activeRes.logs || '',
        isRunning: false,
        statusText: activeRes.statusText || '',
        executionTimeMs: activeRes.executionTimeMs || 0,
        errorMessage: activeRes.errorMessage || null,
        executionId: activeRes.executionId || null,
        activeResultTab: activeRes.activeResultTab || 'table'
      };
    });
    if (state.activeTabId && tabs.some((t) => t.id === state.activeTabId)) {
      activeTabId = state.activeTabId;
    } else {
      activeTabId = tabs[0].id;
    }
    if (state.savedConfigByKind && typeof state.savedConfigByKind === 'object') {
      savedConfigByKind = {
        pyspark: state.savedConfigByKind.pyspark || {},
        spark: state.savedConfigByKind.spark || {}
      };
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
    if (activeTab) {
      pickSessionForLanguage(activeTab.language);
    }
    return true;
  }

  async function loadUserWorkspace(currentUser: UserSession | null) {
    if (!currentUser) {
      tabs = [createTabObject('tab-1', 'Скрипт 1 (PySpark)', 'pyspark', true)];
      activeTabId = 'tab-1';
      return;
    }

    // 1. Сначала пробуем восстановить состояние из базы данных через API
    try {
      const remoteWs = await api.getWorkspace();
      if (remoteWs && remoteWs.state && applyWorkspaceState(remoteWs.state)) {
        return;
      }
    } catch (err) {
      console.warn('Не удалось загрузить рабочее пространство из БД:', err);
    }

    // 2. Если в БД пусто, пробуем загрузить локальный кэш пользователя
    try {
      const raw = localStorage.getItem(getStorageKey(currentUser.username));
      if (raw) {
        const parsed = JSON.parse(raw);
        if (applyWorkspaceState(parsed)) {
          // И отправляем его в БД
          api.saveWorkspace(parsed).catch(() => {});
          return;
        }
      }
    } catch {}

    // 3. Если сохраненной работы нет — открываем свежую начальную вкладку
    tabs = [createTabObject('tab-1', 'Скрипт 1 (PySpark)', 'pyspark', true)];
    activeTabId = 'tab-1';
    pickSessionForLanguage('pyspark');
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
          savedConfigByKind,
          tabs: tabs.map((t) => {
            // Синхронизируем текущий активный результат вкладки в ее resultBuffers
            const resBuf = t.resultBuffers || {
              pyspark: createEmptyResultData(),
              scalaspark: createEmptyResultData(),
              sql: createEmptyResultData()
            };
            resBuf[t.language] = {
              columns: t.columns || [],
              rows: (t.rows || []).slice(0, 100),
              totalRows: t.totalRows || 0,
              logs: t.logs || '',
              executionTimeMs: t.executionTimeMs || 0,
              errorMessage: t.errorMessage || null,
              executionId: t.executionId || null,
              statusText: t.statusText || '',
              activeResultTab: t.activeResultTab || 'table'
            };

            return {
              id: t.id,
              title: t.title,
              language: t.language,
              code: t.code,
              codeBuffers: t.codeBuffers || {
                pyspark: t.language === 'pyspark' ? t.code : '',
                scalaspark: t.language === 'scalaspark' ? t.code : '',
                sql: t.language === 'sql' ? t.code : ''
              },
              resultBuffers: {
                pyspark: {
                  columns: resBuf.pyspark?.columns || [],
                  rows: (resBuf.pyspark?.rows || []).slice(0, 100),
                  totalRows: resBuf.pyspark?.totalRows || 0,
                  logs: resBuf.pyspark?.logs || '',
                  executionTimeMs: resBuf.pyspark?.executionTimeMs || 0,
                  errorMessage: resBuf.pyspark?.errorMessage || null,
                  executionId: resBuf.pyspark?.executionId || null,
                  statusText: resBuf.pyspark?.statusText || '',
                  activeResultTab: resBuf.pyspark?.activeResultTab || 'table'
                },
                scalaspark: {
                  columns: resBuf.scalaspark?.columns || [],
                  rows: (resBuf.scalaspark?.rows || []).slice(0, 100),
                  totalRows: resBuf.scalaspark?.totalRows || 0,
                  logs: resBuf.scalaspark?.logs || '',
                  executionTimeMs: resBuf.scalaspark?.executionTimeMs || 0,
                  errorMessage: resBuf.scalaspark?.errorMessage || null,
                  executionId: resBuf.scalaspark?.executionId || null,
                  statusText: resBuf.scalaspark?.statusText || '',
                  activeResultTab: resBuf.scalaspark?.activeResultTab || 'table'
                },
                sql: {
                  columns: resBuf.sql?.columns || [],
                  rows: (resBuf.sql?.rows || []).slice(0, 100),
                  totalRows: resBuf.sql?.totalRows || 0,
                  logs: resBuf.sql?.logs || '',
                  executionTimeMs: resBuf.sql?.executionTimeMs || 0,
                  errorMessage: resBuf.sql?.errorMessage || null,
                  executionId: resBuf.sql?.executionId || null,
                  statusText: resBuf.sql?.statusText || '',
                  activeResultTab: resBuf.sql?.activeResultTab || 'table'
                }
              },
              columns: t.columns || [],
              rows: (t.rows || []).slice(0, 100),
              totalRows: t.totalRows || 0,
              logs: t.logs || '',
              executionTimeMs: t.executionTimeMs || 0,
              errorMessage: t.errorMessage || null,
              executionId: t.executionId || null,
              activeResultTab: t.activeResultTab || 'table'
            };
          })
        };
        // Локальное кэширование по пользователю
        localStorage.setItem(getStorageKey(user?.username), JSON.stringify(stateToSave));

        // Персистентное сохранение в базу данных
        if (user) {
          api.saveWorkspace(stateToSave).catch((err) => {
            console.warn('Не удалось сохранить рабочее пространство в БД:', err);
          });
        }
      } catch (err) {
        console.warn('Не удалось сохранить состояние Spark Explorer:', err);
      }
    };


    if (immediate) {
      saveFn();
    } else {
      saveTimeout = setTimeout(saveFn, 400);
    }
  }

  onMount(async () => {
    unsubscribeAuth = api.onUnauthorized((msg) => {
      user = null;
      clusters = [];
      clusterDetails = null;
      currentSession = null;
      activeSessions = [];
      authErrorMessage = msg;
      tabs = [createTabObject('tab-1', 'Скрипт 1 (PySpark)', 'pyspark', true)];
      activeTabId = 'tab-1';
      if (sessionPollingTimer) clearTimeout(sessionPollingTimer);
    });

    try {
      try {
        user = await api.getMe();
      } catch {
        const auto = await api.tryAutoLogin();
        user = auto ? ((auto as any).user || auto) : null;
      }

      if (user) {
        await loadUserWorkspace(user);
        await loadClusters();
        scheduleSessionPoll();
      }
    } catch {
      // Пользователь не авторизован
      user = null;
      tabs = [createTabObject('tab-1', 'Скрипт 1 (PySpark)', 'pyspark', true)];
      activeTabId = 'tab-1';
    } finally {
      isAuthChecking = false;
    }
  });

  async function handleLogin(u: string, p: string) {
    const res = await api.login(u, p);
    user = res.user || (await api.getMe());
    authErrorMessage = null;
    isLoginModalOpen = false;
    await loadUserWorkspace(user);
    await loadClusters();
    await refreshSessions();
    scheduleSessionPoll();
  }

  async function handleKerberosSso() {
    const res = await api.kerberosNegotiate();
    user = res.user || (await api.getMe());
    authErrorMessage = null;
    isLoginModalOpen = false;
    await loadUserWorkspace(user);
    await loadClusters();
    await refreshSessions();
    scheduleSessionPoll();
  }

  let sessionPollingTimer: any = null;

  async function handleLogout() {
    if (sessionPollingTimer) {
      clearTimeout(sessionPollingTimer);
      sessionPollingTimer = null;
    }
    try {
      await api.logout();
    } catch (err) {
      console.warn('Ошибка вызова logout API:', err);
    }
    user = null;
    authErrorMessage = null;
    currentSession = null;
    activeSessions = [];
    clusters = [];
    clusterDetails = null;
    tabs = [createTabObject('tab-1', 'Скрипт 1 (PySpark)', 'pyspark', true)];
    activeTabId = 'tab-1';
  }

  onDestroy(() => {
    if (sessionPollingTimer) {
      clearTimeout(sessionPollingTimer);
      sessionPollingTimer = null;
    }
    if (unsubscribeAuth) unsubscribeAuth();
  });

  function scheduleSessionPoll(urgent: boolean = false) {
    if (sessionPollingTimer) {
      clearTimeout(sessionPollingTimer);
      sessionPollingTimer = null;
    }
    if (!user || !selectedClusterId) return;
    const delay = urgent || currentSession?.status === 'starting' || currentSession?.status === 'busy' ? 1500 : 5000;
    sessionPollingTimer = setTimeout(async () => {
      if (!user) return;
      await refreshSessions();
      if (user) {
        scheduleSessionPoll();
      }
    }, delay);
  }

  async function loadClusters() {
    try {
      clusters = await api.getClusters();
      if (clusters.length > 0) {
        if (!selectedClusterId || !clusters.some((c) => c.id === selectedClusterId)) {
          selectedClusterId = clusters[0].id;
        }
        await handleClusterSelect(selectedClusterId);
      }
    } catch (err) {
      console.error('Ошибка загрузки кластеров:', err);
    }
  }

  async function handleClusterSelect(clusterId: string) {
    selectedClusterId = clusterId;
    saveStateToStorage();
    try {
      clusterDetails = await api.getClusterDetails(clusterId);
      if (clusterDetails && Array.isArray(clusterDetails.metastores) && clusterDetails.metastores.length > 0) {
        selectedMetastoreId = clusterDetails.metastores.find((m) => m.is_default)?.id || clusterDetails.metastores[0].id;
      }
      await refreshSessions();
      scheduleSessionPoll();
    } catch (err) {
      console.error('Ошибка загрузки деталей кластера:', err);
    }
  }

  function getTargetKind(lang: 'pyspark' | 'scalaspark' | 'sql'): 'pyspark' | 'spark' {
    if (lang === 'scalaspark') return 'spark';
    if (lang === 'sql') {
      // Для SQL: если текущая выбранная сессия жива, сохраняем ее тип (spark или pyspark)
      if (currentSession && currentSession.cluster_id === selectedClusterId && currentSession.status !== 'killed' && currentSession.status !== 'dead') {
        return currentSession.kind as ('pyspark' | 'spark');
      }
      // Приоритет 1: если есть активная Scala (spark) сессия
      const liveScala = activeSessions.find(
        (s) => s.cluster_id === selectedClusterId && s.kind === 'spark' && s.status !== 'killed' && s.status !== 'dead'
      );
      if (liveScala) return 'spark';

      // Приоритет 2: если есть активная PySpark (pyspark) сессия
      const livePySpark = activeSessions.find(
        (s) => s.cluster_id === selectedClusterId && s.kind === 'pyspark' && s.status !== 'killed' && s.status !== 'dead'
      );
      if (livePySpark) return 'pyspark';

      // Приоритет 3: если нет активных сессий — по умолчанию создаем Scala (spark)
      return 'spark';
    }
    return 'pyspark';
  }

  function pickSessionForLanguage(lang: 'pyspark' | 'scalaspark' | 'sql') {
    if (lang === 'sql') {
      if (currentSession && currentSession.cluster_id === selectedClusterId && currentSession.status !== 'killed' && currentSession.status !== 'dead') {
        return;
      }
      // Приоритет 1: активная Scala (spark) сессия
      const liveScala = activeSessions.find(
        (s) => s.cluster_id === selectedClusterId && s.kind === 'spark' && s.status !== 'killed' && s.status !== 'dead'
      );
      if (liveScala) {
        currentSession = liveScala;
        return;
      }
      // Приоритет 2: активная PySpark (pyspark) сессия
      const livePySpark = activeSessions.find(
        (s) => s.cluster_id === selectedClusterId && s.kind === 'pyspark' && s.status !== 'killed' && s.status !== 'dead'
      );
      currentSession = livePySpark || null;
      return;
    }

    const targetKind = getTargetKind(lang);
    const matching = activeSessions.find(
      (s) => s.cluster_id === selectedClusterId && s.kind === targetKind && s.status !== 'killed' && s.status !== 'dead'
    );
    if (matching) {
      currentSession = matching;
    } else if (currentSession && (currentSession.kind !== targetKind || currentSession.status === 'killed' || currentSession.status === 'dead')) {
      currentSession = null;
    }
  }

  async function refreshSessions() {
    if (!user || !selectedClusterId) return;
    try {
      activeSessions = await api.getSessions();
      const currentLang = activeTab?.language || 'pyspark';

      // Если текущая сессия уже выбрана, проверяем ее в свежем списке
      if (currentSession) {
        const updated = activeSessions.find((s) => s.id === currentSession?.id);
        const isCompatible = currentLang === 'sql' || (updated && updated.kind === getTargetKind(currentLang));
        if (updated && updated.status !== 'killed' && updated.status !== 'dead' && isCompatible) {
          currentSession = updated;
          return;
        } else if (!updated || updated.status === 'killed' || updated.status === 'dead') {
          currentSession = null;
        }
      }

      // Иначе ищем подходящую сессию
      pickSessionForLanguage(currentLang);
    } catch (err) {
      console.error('Ошибка получения сессий:', err);
    }
  }

  function selectTab(id: string) {
    activeTabId = id;
    const tab = tabs.find((t) => t.id === id);
    if (tab) {
      pickSessionForLanguage(tab.language);
    }
    saveStateToStorage(true);
  }

  function addTab() {
    const newId = `tab-${Date.now()}`;
    const newTab = createTabObject(newId, `Скрипт ${tabs.length + 1}`, 'pyspark');
    tabs = [...tabs, newTab];
    activeTabId = newId;
    pickSessionForLanguage('pyspark');
    saveStateToStorage(true);
  }

  function closeTab(id: string, e: MouseEvent) {
    e.stopPropagation();
    if (tabs.length === 1) return;
    tabs = tabs.filter((t) => t.id !== id);
    if (activeTabId === id) {
      const nextTab = tabs[tabs.length - 1];
      activeTabId = nextTab.id;
      pickSessionForLanguage(nextTab.language);
    }
    saveStateToStorage(true);
  }

  function handleLanguageChange(lang: 'pyspark' | 'scalaspark' | 'sql') {
    if (!activeTab || activeTab.language === lang) return;

    // 1. Инициализируем codeBuffers если еще не созданы
    if (!activeTab.codeBuffers) {
      activeTab.codeBuffers = {
        pyspark: activeTab.language === 'pyspark' ? activeTab.code : '',
        scalaspark: activeTab.language === 'scalaspark' ? activeTab.code : '',
        sql: activeTab.language === 'sql' ? activeTab.code : ''
      };
    }

    // 2. Инициализируем resultBuffers если еще не созданы
    if (!activeTab.resultBuffers) {
      activeTab.resultBuffers = {
        pyspark: createEmptyResultData(),
        scalaspark: createEmptyResultData(),
        sql: createEmptyResultData()
      };
    }

    // 3. Сохраняем текущий код и результаты в буферы текущего языка
    activeTab.codeBuffers[activeTab.language] = activeTab.code;
    activeTab.resultBuffers[activeTab.language] = {
      columns: activeTab.columns || [],
      rows: activeTab.rows || [],
      totalRows: activeTab.totalRows || 0,
      logs: activeTab.logs || '',
      executionTimeMs: activeTab.executionTimeMs || 0,
      errorMessage: activeTab.errorMessage || null,
      executionId: activeTab.executionId || null,
      statusText: activeTab.statusText || '',
      activeResultTab: activeTab.activeResultTab || 'table'
    };

    // 4. Переключаем язык
    activeTab.language = lang;

    // 5. Загружаем код для нового языка из его персонального буфера
    activeTab.code = activeTab.codeBuffers[lang] ?? '';

    // 6. Восстанавливаем результаты для нового выбранного языка
    const targetRes = activeTab.resultBuffers[lang] || createEmptyResultData();
    activeTab.columns = targetRes.columns || [];
    activeTab.rows = targetRes.rows || [];
    activeTab.totalRows = targetRes.totalRows || 0;
    activeTab.logs = targetRes.logs || '';
    activeTab.executionTimeMs = targetRes.executionTimeMs || 0;
    activeTab.errorMessage = targetRes.errorMessage || null;
    activeTab.executionId = targetRes.executionId || null;
    activeTab.statusText = targetRes.statusText || '';
    activeTab.activeResultTab = targetRes.activeResultTab || 'table';

    // 7. Переключаем сессию на соответствующий вид (PySpark / Scala)
    pickSessionForLanguage(lang);

    // 8. Персистим в localStorage и БД
    saveStateToStorage(true);
  }


  function handleCodeChange(newCode: string) {
    if (activeTab) {
      activeTab.code = newCode;
      if (!activeTab.codeBuffers) {
        activeTab.codeBuffers = {
          pyspark: activeTab.language === 'pyspark' ? newCode : '',
          scalaspark: activeTab.language === 'scalaspark' ? newCode : '',
          sql: activeTab.language === 'sql' ? newCode : ''
        };
      } else {
        activeTab.codeBuffers[activeTab.language] = newCode;
      }
      saveStateToStorage();
    }
  }

  function openSessionConfigModal() {
    const targetKind = currentTabKind;
    const baseConfig = savedConfigByKind[targetKind] || {};
    sessionPayload = {
      ...baseConfig,
      kind: targetKind,
      cluster_id: selectedClusterId || baseConfig.cluster_id || undefined,
      metastore_id: selectedMetastoreId || baseConfig.metastore_id || undefined
    };

    if (currentSession && currentSession.kind === targetKind) {
      if (currentSession.spark_version_id) sessionPayload.spark_version_id = currentSession.spark_version_id;
      if (currentSession.python_env_id && targetKind === 'pyspark') {
        sessionPayload.python_env_id = currentSession.python_env_id;
        sessionPayload.custom_python_archive = currentSession.custom_python_archive || undefined;
        sessionPayload.custom_python_path = currentSession.custom_python_path || undefined;
      }
    }

    isConfigModalOpen = true;
  }

  async function handleSaveSessionConfig(payload: CreateSessionPayload) {
    isConfigModalOpen = false;
    const kind = (payload.kind || currentTabKind) as 'pyspark' | 'spark';
    savedConfigByKind[kind] = payload;
    sessionPayload = payload;
    saveStateToStorage(true);
    try {
      currentSession = await api.createSession(payload);
      scheduleSessionPoll(true);
      await refreshSessions();
    } catch (err: any) {
      alert(`Не удалось запустить сессию: ${err.message}`);
    }
  }

  async function handleRestartSession() {
    if (!clusterDetails) return;
    try {
      if (currentSession && currentSession.status !== 'killed' && currentSession.status !== 'dead') {
        await api.stopSession(currentSession.id);
      }

      const defVer = clusterDetails.spark_versions.find((v) => v.is_default)?.id || clusterDetails.spark_versions[0]?.id;
      const vObj = clusterDetails.spark_versions.find((v) => v.id === defVer);
      const defPy = vObj?.python_versions.find((p) => p.is_default)?.id || vObj?.python_versions[0]?.id;
      const defMeta = clusterDetails.metastores.find((m) => m.is_default)?.id || clusterDetails.metastores[0]?.id;
      const targetKind = currentTabKind;
      const baseConfig = savedConfigByKind[targetKind] || {};

      const payload: CreateSessionPayload = {
        cluster_id: selectedClusterId,
        spark_version_id: currentSession?.spark_version_id || baseConfig.spark_version_id || defVer,
        python_env_id: targetKind === 'pyspark' ? (currentSession?.python_env_id || baseConfig.python_env_id || defPy) : undefined,
        custom_python_archive: targetKind === 'pyspark' ? (currentSession?.custom_python_archive || baseConfig.custom_python_archive) : undefined,
        custom_python_path: targetKind === 'pyspark' ? (currentSession?.custom_python_path || baseConfig.custom_python_path) : undefined,
        metastore_id: currentSession?.metastore_id || baseConfig.metastore_id || defMeta,
        yarn_queue: currentSession?.yarn_queue || baseConfig.yarn_queue || clusterDetails.default_queue || 'root.analytics',
        resource_profile: currentSession?.resource_profile || baseConfig.resource_profile || clusterDetails.resource_profiles[0]?.id || 'small',
        kind: targetKind,
        packages: baseConfig.packages || [],
        jars: baseConfig.jars || [],
        py_files: targetKind === 'pyspark' ? (baseConfig.py_files || []) : [],
        spark_conf: baseConfig.spark_conf || {}
      };

      currentSession = await api.createSession(payload);
      scheduleSessionPoll(true);
      await refreshSessions();
    } catch (err: any) {
      alert(`Ошибка подключения сессии: ${err.message}`);
    }
  }

  async function handleStopSession() {
    if (!currentSession) return;
    try {
      await api.stopSession(currentSession.id);
      currentSession = null;
      await refreshSessions();
      scheduleSessionPoll();
    } catch (err: any) {
      alert(`Ошибка остановки: ${err.message}`);
    }
  }

  async function handleRun(codeToRun?: string) {
    if (!activeTab || !clusterDetails) return;

    const targetCode = (typeof codeToRun === 'string' && codeToRun.trim())
      ? codeToRun.trim()
      : (activeTab.code || '').trim();

    if (!targetCode) {
      activeTab.errorMessage = 'Скрипт пуст. Введите код для выполнения.';
      return;
    }

    const targetKind = getTargetKind(activeTab.language);

    // СРАЗУ переводим интерфейс в статус выполнения и подготовки
    const isFragment = targetCode !== (activeTab.code || '').trim();
    activeTab.isRunning = true;
    activeTab.statusText = 'Подготовка...';
    activeTab.errorMessage = null;
    activeTab.columns = [];
    activeTab.rows = [];
    activeTab.logs = isFragment
      ? `[Выполнение выделенного фрагмента (${targetCode.split('\n').length} строк)]\n\n${targetCode}\n\nПодготовка сессии Spark...`
      : 'Подготовка сессии Spark...';

    // Проверяем актуальность currentSession
    const isSessionValid = currentSession &&
      currentSession.cluster_id === selectedClusterId &&
      (activeTab.language === 'sql' || currentSession.kind === targetKind) &&
      currentSession.status !== 'killed' &&
      currentSession.status !== 'dead';

    if (!isSessionValid) {
      // Ищем подходящую живую сессию среди существующих
      const existing = activeSessions.find(
        (s) => s.cluster_id === selectedClusterId &&
               (activeTab.language === 'sql' || s.kind === targetKind) &&
               s.status !== 'killed' && s.status !== 'dead'
      );

      if (existing) {
        currentSession = existing;
      } else {
        // Создаем новую сессию
        activeTab.statusText = 'Аллокация в YARN...';
        activeTab.logs += '\nАллокация ресурсов и запуск сессии Spark в YARN...';
        try {
          const defVer = clusterDetails.spark_versions.find((v) => v.is_default)?.id || clusterDetails.spark_versions[0].id;
          const vObj = clusterDetails.spark_versions.find((v) => v.id === defVer);
          const defPy = vObj?.python_versions.find((p) => p.is_default)?.id || vObj?.python_versions[0]?.id;
          const defMeta = clusterDetails.metastores.find((m) => m.is_default)?.id || clusterDetails.metastores[0].id;
          const baseConfig = savedConfigByKind[targetKind] || {};

          const payload: CreateSessionPayload = {
            cluster_id: selectedClusterId,
            spark_version_id: baseConfig.spark_version_id || defVer,
            python_env_id: targetKind === 'pyspark' ? (baseConfig.python_env_id || defPy) : undefined,
            custom_python_archive: targetKind === 'pyspark' ? baseConfig.custom_python_archive : undefined,
            custom_python_path: targetKind === 'pyspark' ? baseConfig.custom_python_path : undefined,
            metastore_id: baseConfig.metastore_id || defMeta,
            yarn_queue: baseConfig.yarn_queue || clusterDetails.default_queue || 'root.analytics',
            resource_profile: baseConfig.resource_profile || clusterDetails.resource_profiles[0]?.id || 'small',
            kind: targetKind,
            packages: baseConfig.packages || [],
            jars: baseConfig.jars || [],
            py_files: targetKind === 'pyspark' ? (baseConfig.py_files || []) : [],
            spark_conf: baseConfig.spark_conf || {}
          };
          currentSession = await api.createSession(payload);
          scheduleSessionPoll(true);
          await refreshSessions();
        } catch (err: any) {
          activeTab.isRunning = false;
          activeTab.statusText = '';
          activeTab.errorMessage = `Не удалось создать сессию Spark: ${err.message}`;
          return;
        }
      }
    }

    if (!currentSession) {
      activeTab.isRunning = false;
      activeTab.statusText = '';
      activeTab.errorMessage = 'Ошибка: сессия Spark не инициализирована';
      return;
    }

    activeTab.statusText = 'Отправка...';
    activeTab.logs += '\nОтправка задачи в Apache Spark...';

    try {
      const execRes = await api.executeCode(currentSession.id, targetCode, activeTab.language);
      activeTab.executionId = execRes.execution_id;
      const currentExecutionId = execRes.execution_id;
      activeTab.statusText = 'Выполнение...';

      let isCompleted = false;

      const finishExecution = async (status: string, eventLogs?: string, error?: string, execTime?: number) => {
        if (isCompleted || !activeTab) return;
        isCompleted = true;
        if (cleanupStream) cleanupStream();
        if (pollInterval) clearInterval(pollInterval);

        activeTab.isRunning = false;
        activeTab.statusText = '';
        activeTab.executionTimeMs = execTime || 0;
        if (eventLogs) {
          activeTab.logs = eventLogs;
        }

        if (status === 'FAILED') {
          activeTab.errorMessage = error || 'Ошибка исполнения Spark';
          activeTab.activeResultTab = 'logs';
        } else if (status === 'CANCELLED') {
          activeTab.errorMessage = 'Выполнение прервано пользователем';
          activeTab.activeResultTab = 'logs';
        } else {
          try {
            const fullRes = await api.getResult(currentExecutionId, 0, 100);
            activeTab.columns = fullRes.columns;
            activeTab.rows = fullRes.rows;
            activeTab.totalRows = fullRes.total_rows;
            if (fullRes.logs) {
              activeTab.logs = fullRes.logs;
            }
            activeTab.activeResultTab = fullRes.rows.length > 0 ? 'table' : 'logs';
          } catch {}
        }
        if (!activeTab.resultBuffers) {
          activeTab.resultBuffers = {
            pyspark: createEmptyResultData(),
            scalaspark: createEmptyResultData(),
            sql: createEmptyResultData()
          };
        }
        activeTab.resultBuffers[activeTab.language] = {
          columns: activeTab.columns || [],
          rows: activeTab.rows || [],
          totalRows: activeTab.totalRows || 0,
          logs: activeTab.logs || '',
          executionTimeMs: activeTab.executionTimeMs || 0,
          errorMessage: activeTab.errorMessage || null,
          executionId: activeTab.executionId || null,
          statusText: activeTab.statusText || '',
          activeResultTab: activeTab.activeResultTab || 'table'
        };
        saveStateToStorage();
        await refreshSessions();
      };

      // 1. Подписываемся на SSE стриминг
      const cleanupStream = api.streamExecution(currentExecutionId, async (event) => {
        if (!activeTab || isCompleted) return;
        if (event.logs !== undefined && event.logs !== null && event.logs !== '') {
          activeTab.logs = event.logs;
        }
        if (event.type === 'finished' || event.status === 'FINISHED' || event.status === 'FAILED' || event.status === 'CANCELLED') {
          await finishExecution(event.status || 'FINISHED', event.logs, event.error, event.execution_time_ms);
        }
      });

      // 2. Fallback Polling (гарантированное завершение даже при сетевых сбоях SSE)
      const pollInterval = setInterval(async () => {
        if (isCompleted || !activeTab) {
          clearInterval(pollInterval);
          return;
        }
        try {
          const check = await api.getResult(currentExecutionId, 0, 10);
          if (check.status && check.status !== 'RUNNING' && check.status !== 'QUEUED') {
            await finishExecution(check.status, check.logs || undefined, check.error_message || undefined, check.execution_time_ms);
          }
        } catch {}
      }, 1500);

    } catch (err: any) {
      activeTab.isRunning = false;
      activeTab.statusText = '';
      activeTab.errorMessage = err.message;
      activeTab.activeResultTab = 'logs';
      if (!activeTab.resultBuffers) {
        activeTab.resultBuffers = {
          pyspark: createEmptyResultData(),
          scalaspark: createEmptyResultData(),
          sql: createEmptyResultData()
        };
      }
      activeTab.resultBuffers[activeTab.language] = {
        columns: activeTab.columns || [],
        rows: activeTab.rows || [],
        totalRows: activeTab.totalRows || 0,
        logs: activeTab.logs || '',
        executionTimeMs: activeTab.executionTimeMs || 0,
        errorMessage: activeTab.errorMessage || null,
        executionId: activeTab.executionId || null,
        statusText: activeTab.statusText || '',
        activeResultTab: 'logs'
      };
      saveStateToStorage();
    }

  }

  async function handleCancel() {
    if (!activeTab) return;
    const targetExecutionId = activeTab.executionId;
    activeTab.isRunning = false;
    activeTab.logs = (activeTab.logs || '') + '\n\n[Выполнение прервано пользователем]';

    if (targetExecutionId) {
      try {
        await api.cancelStatement(targetExecutionId);
      } catch (err: any) {
        console.warn('Ошибка при отмене statement:', err);
      }
    }
    saveStateToStorage();
    await refreshSessions();
  }

  function handleSelectTable(tableName: string) {
    if (!activeTab) return;
    let snippet = '';
    if (activeTab.language === 'sql') {
      snippet = activeTab.code.trim()
        ? `\n\nSELECT * FROM ${tableName}\nLIMIT 50;\n`
        : `SELECT * FROM ${tableName}\nLIMIT 50;\n`;
    } else if (activeTab.language === 'scalaspark') {
      snippet = activeTab.code.trim()
        ? `\n\nval df = spark.read.table("${tableName}")\ndf.show(50, false)\n`
        : `val df = spark.read.table("${tableName}")\ndf.show(50, false)\n`;
    } else {
      snippet = activeTab.code.trim()
        ? `\n\ndf = spark.read.table("${tableName}")\ndisplay(df)\n`
        : `df = spark.read.table("${tableName}")\ndisplay(df)\n`;
    }
    activeTab.code += snippet;
    if (activeTab.codeBuffers) {
      activeTab.codeBuffers[activeTab.language] = activeTab.code;
    }
    saveStateToStorage(true);
  }

  function handleRestoreHistory(item: HistoryItem) {
    if (!activeTab) return;
    const lang = (item.language || 'pyspark') as 'pyspark' | 'scalaspark' | 'sql';
    activeTab.code = item.code;
    activeTab.language = lang;
    if (activeTab.codeBuffers) {
      activeTab.codeBuffers[lang] = item.code;
    }
    pickSessionForLanguage(activeTab.language);
    saveStateToStorage(true);
    if (item.has_cached_result) {
      api.getResult(item.id, 0, 100).then((res) => {
        if (activeTab) {
          activeTab.columns = res.columns;
          activeTab.rows = res.rows;
          activeTab.totalRows = res.total_rows;
          activeTab.logs = res.logs || '';
          if (!activeTab.resultBuffers) {
            activeTab.resultBuffers = {
              pyspark: createEmptyResultData(),
              scalaspark: createEmptyResultData(),
              sql: createEmptyResultData()
            };
          }
          activeTab.resultBuffers[activeTab.language] = {
            columns: res.columns,
            rows: res.rows,
            totalRows: res.total_rows,
            logs: res.logs || '',
            executionTimeMs: item.execution_time_ms || 0,
            errorMessage: item.error_message || null,
            executionId: item.id,
            statusText: 'Загружено из кэша',
            activeResultTab: 'table'
          };
          saveStateToStorage(true);
        }
      });
    }
  }


  // Обработчики изменения размера сайдбара (каталогов Hive Metastore)
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
</script>

{#if isAuthChecking}
  <div class="min-h-screen bg-slate-50 flex flex-col items-center justify-center text-slate-800 gap-3">
    <div class="w-8 h-8 border-3 border-amber-500 border-t-transparent rounded-full animate-spin"></div>
    <span class="text-xs font-medium text-slate-500">Проверка сессии...</span>
  </div>
{:else if !user}
  <LoginModal
    title="Spark Explorer"
    subtitle="Аутентификация LDAP & Kerberos SSO"
    icon={Flame}
    isModal={false}
    initialError={authErrorMessage}
    onLogin={handleLogin}
    onKerberosSso={handleKerberosSso}
  />
{:else}
  <div class="h-screen w-screen flex flex-col overflow-hidden bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-100">
  <!-- Главный Header платформы -->
  <Header
    title="Spark Explorer"
    subtitle="PySpark & Scala"
    icon={Flame}
    user={user}
    clusters={clusters}
    selectedClusterId={selectedClusterId}
    onClusterSelect={handleClusterSelect}
    onLogout={handleLogout}
    onLoginClick={() => (isLoginModalOpen = true)}
  />

  <div class="flex-1 flex overflow-hidden">
    <!-- Сайдбар каталогов и истории с настраиваемой шириной -->
    <div style="width: {sidebarWidth}px;" class="h-full shrink-0 flex">
      <Sidebar
        clusterDetails={clusterDetails}
        bind:selectedMetastoreId
        targetLanguage={activeTab?.language || 'pyspark'}
        username={user?.username}
        onSelectTable={handleSelectTable}
        onRestoreHistory={handleRestoreHistory}
      />
    </div>

    <!-- Вертикальный разделитель (ширина каталогов Metastore) -->
    <div
      role="separator"
      aria-orientation="vertical"
      tabindex="0"
      onmousedown={handleSidebarMouseDown}
      ondblclick={() => { sidebarWidth = 280; saveStateToStorage(true); window.dispatchEvent(new Event('resize')); }}
      class="w-1.5 hover:w-2 bg-slate-200/80 dark:bg-slate-800 hover:bg-amber-500 active:bg-amber-600 transition-all cursor-col-resize shrink-0 z-20 flex items-center justify-center group select-none"
      title="Изменить ширину каталогов (двойной клик для сброса)"
    >
      <div class="w-0.5 h-8 bg-slate-400/50 group-hover:bg-white rounded-full transition"></div>
    </div>

    <!-- Основная рабочая область -->
    <main class="flex-1 flex flex-col overflow-hidden min-w-0 bg-slate-50 dark:bg-slate-900">
      <!-- Вкладки редактора скриптов и статус сессии -->
      <div class="h-11 bg-slate-100/90 dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between px-2.5 gap-2 shrink-0 select-none">
        <!-- Слева: скроллируемые вкладки скриптов -->
        <div class="flex items-center gap-1 overflow-x-auto min-w-0 py-1">
          {#each tabs as tab}
            <div
              role="button"
              tabindex="0"
              onclick={() => selectTab(tab.id)}
              onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') selectTab(tab.id); }}
              class="group flex items-center gap-1.5 px-3 py-1.5 rounded-t-lg text-xs font-medium cursor-pointer transition border-t-2 shrink-0 {tab.id === activeTabId ? 'bg-white dark:bg-slate-900 border-amber-500 text-slate-900 dark:text-slate-100 font-semibold shadow-xs' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200 hover:bg-slate-200/60 dark:hover:bg-slate-800'}"
            >
              <span class="truncate max-w-[150px]">{tab.title}</span>
              <span class="text-[9px] font-mono uppercase px-1 rounded bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400">{tab.language}</span>
              {#if tabs.length > 1}
                <button
                  onclick={(e) => closeTab(tab.id, e)}
                  class="opacity-0 group-hover:opacity-100 hover:bg-slate-200 dark:hover:bg-slate-700 p-0.5 rounded text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 transition"
                >
                  <X class="w-3 h-3" />
                </button>
              {/if}
            </div>
          {/each}

          <button
            onclick={addTab}
            class="p-1 rounded-md hover:bg-slate-200 dark:hover:bg-slate-800 text-slate-500 hover:text-amber-600 transition cursor-pointer ml-0.5 shrink-0"
            title="Новый скрипт"
          >
            <Plus class="w-4 h-4" />
          </button>
        </div>

        <!-- Справа: Статус и управление сессией Spark -->
        <div class="shrink-0 flex items-center">
          <SparkSessionWidget
            session={currentSession}
            yarnClusterId={clusterDetails?.yarn_cluster_id}
            onOpenSettings={openSessionConfigModal}
            onRestartSession={handleRestartSession}
            onStopSession={handleStopSession}
          />
        </div>
      </div>

      {#if activeTab}
        <!-- Тулбар активного скрипта -->
        <SessionBar
          language={activeTab.language}
          isRunning={activeTab.isRunning}
          statusText={activeTab.statusText}
          executionTimeMs={activeTab.executionTimeMs}
          rowsCount={activeTab.totalRows}
          onLanguageChange={handleLanguageChange}
          onRun={() => {
            if (runEditorTrigger) {
              runEditorTrigger();
            } else {
              handleRun();
            }
          }}
          onCancel={handleCancel}
        />
      {/if}

      <!-- Рабочая зона: Редактор и Результаты (Resizable Split) -->
      <div bind:this={mainAreaRef} class="flex-1 flex flex-col overflow-hidden relative min-h-0">
        <!-- Редактор кода -->
        <div style="height: {editorHeightPercent}%;" class="w-full overflow-hidden shrink-0">
          {#if activeTab}
            <SparkEditor
              bind:code={activeTab.code}
              language={activeTab.language}
              onCodeChange={handleCodeChange}
              onRun={(customCode) => handleRun(customCode)}
              registerTrigger={(fn) => { runEditorTrigger = fn; }}
            />
          {/if}
        </div>

        <!-- Разделитель (Split Handle) -->
        <div
          role="separator"
          aria-orientation="horizontal"
          tabindex="0"
          onmousedown={handleEditorMouseDown}
          ondblclick={() => { editorHeightPercent = 50; saveStateToStorage(true); window.dispatchEvent(new Event('resize')); }}
          class="h-1.5 hover:h-2 bg-slate-200/80 hover:bg-amber-500 active:bg-amber-600 transition-all cursor-row-resize shrink-0 z-20 flex items-center justify-center group select-none"
          title="Изменить размер редактора и результатов (двойной клик для сброса)"
        >
          <div class="h-0.5 w-8 bg-slate-400/50 group-hover:bg-white rounded-full transition"></div>
        </div>

        <!-- Результаты и Логи -->
        <div style="height: {100 - editorHeightPercent}%;" class="w-full overflow-hidden min-h-0">
          {#if activeTab}
            <ResultsView
              columns={activeTab.columns}
              rows={activeTab.rows}
              totalRows={activeTab.totalRows}
              logs={activeTab.logs}
              errorMessage={activeTab.errorMessage}
              executionTimeMs={activeTab.executionTimeMs}
              bind:activeTab={activeTab.activeResultTab}
              onTabChange={() => saveStateToStorage(true)}
            />
          {/if}
        </div>
      </div>
    </main>
  </div>

  <!-- Модальное окно конфигурации сессии Spark (Lazy Loaded) -->
  {#if isConfigModalOpen}
    {#await import('./components/SessionConfigModal.svelte') then { default: SessionConfigModal }}
      <SessionConfigModal
        isOpen={isConfigModalOpen}
        clusterDetails={clusterDetails}
        initialValues={sessionPayload}
        targetKind={currentTabKind}
        targetLanguage={activeTab?.language || 'pyspark'}
        onClose={() => (isConfigModalOpen = false)}
        onSave={handleSaveSessionConfig}
      />
    {/await}
  {/if}

  {#if isLoginModalOpen}
    <LoginModal
      title="Spark Explorer"
      subtitle="Аутентификация LDAP & Kerberos SSO"
      icon={Flame}
      isModal={true}
      initialError={authErrorMessage}
      onClose={() => (isLoginModalOpen = false)}
      onLogin={handleLogin}
      onKerberosSso={handleKerberosSso}
    />
  {/if}
</div>
{/if}
