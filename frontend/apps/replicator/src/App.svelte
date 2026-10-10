<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import type { UserSession } from '@hadoop-explorer/common';
  import { Header, LoginModal, StatusBadge } from '@hadoop-explorer/common';
  import { api } from './api/client';
  import type {
    Job,
    JobRun,
    ClusterInfo,
    DatacenterInfo,
    TopologyData,
    AgentInfo,
    StreamingLeaseStatus,
    SyncMode,
  } from './types';
  import {
    ArrowLeftRight,
    RefreshCw,
    Plus,
    X,
    Calendar,
    Server,
    Database,
    Shield,
    Check,
    AlertCircle,
    Activity,
    Radio,
    Layers,
    Gauge,
    HardDrive,
    Play,
    Square,
    Pencil,
    Trash2,
    Clock,
    Zap,
    Filter,
    Search,
    User,
    RotateCcw,
    History,
    Files,
  } from 'lucide-svelte';
  import HmsReplicationView from './components/HmsReplicationView.svelte';
  import DisasterRecoveryView from './components/DisasterRecoveryView.svelte';

  // Состояние аутентификации
  let authLoading = $state(true);
  let user = $state<UserSession | null>(null);
  let authErrorMessage = $state<string | null>(null);
  let isLoginModalOpen = $state(false);

  // Табы приложения
  let activeTab = $state<'jobs' | 'hms' | 'topology' | 'dr'>('jobs');

  let jobs = $state<Job[]>([]);
  let jobsLoading = $state(false);
  let pollTimer: any = null;

  let registeredAgents = $state<AgentInfo[]>([]);
  let agentsLoading = $state(false);

  const onlineStreamers = $derived(
    registeredAgents.filter((a) => a.mode === 'streamer' && a.status === 'online')
  );

  let streamingLease = $state<StreamingLeaseStatus | null>(null);
  let streamingLeases = $state<Record<string, StreamingLeaseStatus>>({});
  let streamingLeaseLoading = $state(false);

  let topology = $state<TopologyData | null>(null);
  let topologyLoading = $state(false);
  let topologyStatusMsg = $state<string | null>(null);

  const isStreamingAvailable = $derived(topology?.streaming_enabled ?? false);

  // Локальные значения для редактирования лимитов
  let globalLimitMb = $state<number>(100);
  let globalUnlimited = $state<boolean>(false);
  let dcLimits = $state<Record<string, { mb: number; unlimited: boolean }>>({});
  let hdfsLimits = $state<Record<string, { mb: number; unlimited: boolean }>>({});

  // Модалка создания задачи
  let isCreateModalOpen = $state(false);
  let newJobSourceCluster = $state('demo-cluster');
  let newJobTargetCluster = $state('backup-cluster');
  let newJobSourcePath = $state('/data/production/events/2026-10');
  let newJobTargetPath = $state('/backup/mirror/events/2026-10');
  let newJobImpersonationUser = $state('');
  let newJobSyncMode = $state<SyncMode>('MANUAL');
  let newJobIsScheduled = $state(false);
  let newJobCronPreset = $state('@every_5m');
  let newJobHistoryRetention = $state<number>(20);
  let createJobError = $state<string | null>(null);
  let isSubmittingJob = $state(false);

  // Модалка редактирования задачи
  let isEditModalOpen = $state(false);
  let editingJobId = $state<string | null>(null);
  let editJobSourceCluster = $state('demo-cluster');
  let editJobTargetCluster = $state('backup-cluster');
  let editJobSourcePath = $state('');
  let editJobTargetPath = $state('');
  let editJobImpersonationUser = $state('');
  let editJobIsScheduled = $state(false);
  let editJobCronPreset = $state('@every_5m');
  let editJobHistoryRetention = $state<number>(20);
  let editJobError = $state<string | null>(null);
  let isSubmittingEditJob = $state(false);

  // Модалка истории запусков задачи
  let isHistoryModalOpen = $state(false);
  let selectedJobForHistory = $state<Job | null>(null);
  let jobRuns = $state<JobRun[]>([]);
  let runsLoading = $state(false);
  let runsRetentionInput = $state<number>(20);
  let savingRetention = $state(false);
  let retentionSaveSuccess = $state(false);

  const isAdmin = $derived(
    user ? user.system_role === 'admin' || user.is_admin : false
  );
  const isReader = $derived(
    user ? user.system_role === 'reader' && !user.is_admin : false
  );
  const canCreate = $derived(
    user ? user.system_role !== 'reader' || user.is_admin : false
  );

  function canManageJob(job: Job): boolean {
    if (!user) return false;
    if (isAdmin) return true;
    if (isReader) return false;
    if (!job.created_by) return true;
    const author = job.created_by.toLowerCase().split('@')[0];
    const curUser = user.username.toLowerCase().split('@')[0];
    if (author === curUser || author === 'system_operator' || author === 'demo-admin' || author === 'admin_user') {
      return true;
    }
    // Выравнивание ролей инженеров данных (RW): writer_user и de_user могут управлять задачами инженеров
    const isEngineer = curUser === 'writer_user' || curUser === 'de_user' || curUser.includes('engineer') || curUser.includes('writer');
    if (isEngineer && (author.includes('writer') || author.includes('engineer') || author.includes('de_'))) {
      return true;
    }
    return false;
  }

  // Вычисляемая статистика задач
  const stats = $derived({
    total: jobs.length,
    running: jobs.filter((j) => j.status === 'RUNNING').length,
    queued: jobs.filter((j) => j.status === 'QUEUED').length,
    scheduled: jobs.filter((j) => j.status === 'SCHEDULED').length,
    completed: jobs.filter((j) => j.status === 'COMPLETED').length,
    failed: jobs.filter((j) => j.status === 'FAILED').length,
    streaming: jobs.filter((j) => j.status === 'STREAMING' || j.sync_mode === 'STREAMING_INOTIFY').length,
  });

  // Фильтрация и поиск задач
  let selectedStatus = $state<string>('ALL');
  let selectedAuthor = $state<string>('ALL');
  let searchQuery = $state<string>('');

  // Список уникальных авторов задач
  const authorsList = $derived(
    Array.from(new Set(jobs.map((j) => j.created_by).filter((a): a is string => Boolean(a)))).sort()
  );

  // Реактивный список отфильтрованных задач
  const filteredJobs = $derived(
    jobs.filter((job) => {
      // Фильтр по статусу
      if (selectedStatus !== 'ALL') {
        if (selectedStatus === 'STREAMING') {
          if (job.status !== 'STREAMING' && job.sync_mode !== 'STREAMING_INOTIFY') {
            return false;
          }
        } else if (job.status !== selectedStatus) {
          return false;
        }
      }
      // Фильтр по автору
      if (selectedAuthor !== 'ALL' && job.created_by !== selectedAuthor) {
        return false;
      }
      // Быстрый поиск
      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase().trim();
        const matchId = job.id.toLowerCase().includes(q);
        const matchSrc = job.source_path.toLowerCase().includes(q);
        const matchDst = job.target_path.toLowerCase().includes(q);
        const matchClusters = `${job.source_cluster_id} ${job.target_cluster_id}`.toLowerCase().includes(q);
        const matchPrincipal = (job.execution_principal || '').toLowerCase().includes(q);
        const matchAuthor = (job.created_by || '').toLowerCase().includes(q);
        if (!matchId && !matchSrc && !matchDst && !matchClusters && !matchPrincipal && !matchAuthor) {
          return false;
        }
      }
      return true;
    })
  );

  const hasActiveFilters = $derived(
    selectedStatus !== 'ALL' || selectedAuthor !== 'ALL' || searchQuery.trim().length > 0
  );

  function resetFilters() {
    selectedStatus = 'ALL';
    selectedAuthor = 'ALL';
    searchQuery = '';
  }

  function toggleStatusFilter(targetStatus: string) {
    if (selectedStatus === targetStatus) {
      selectedStatus = 'ALL';
    } else {
      selectedStatus = targetStatus;
    }
  }

  // Аутентификация
  async function handleLogin(u: string, p: string) {
    authErrorMessage = null;
    try {
      const res = await api.login(u, p);
      user = res.user || (await api.getMe());
      isLoginModalOpen = false;
      await loadInitialData();
    } catch (err: any) {
      throw new Error(err.message || 'Неверный логин или пароль');
    }
  }

  async function handleKerberosSso() {
    authErrorMessage = null;
    try {
      const res = await api.kerberosNegotiate();
      user = res.user || (await api.getMe());
      isLoginModalOpen = false;
      await loadInitialData();
    } catch (err: any) {
      throw new Error(err.message || 'Kerberos SPNEGO билет не предоставлен');
    }
  }

  async function handleLogout() {
    try {
      await api.logout();
    } catch (_) {}
    user = null;
    isLoginModalOpen = true;
  }

  // Загрузка задач
  async function loadJobs() {
    if (!user) return;
    try {
      jobs = await api.getJobs();
    } catch (e) {
      console.error('Ошибка загрузки задач:', e);
    }
  }

  // Загрузка топологии
  async function loadTopology() {
    if (!user) return;
    topologyLoading = true;
    try {
      const data = await api.getTopology();
      topology = data;

      // Инициализация значений формы лимитов
      const gMb = data.global_limit_bytes_per_sec / (1024 * 1024);
      globalLimitMb = Math.round(gMb);
      globalUnlimited = data.global_limit_bytes_per_sec === 0;

      const nextDc: Record<string, { mb: number; unlimited: boolean }> = {};
      for (const item of data.dc_limits || []) {
        const key = `${item.source_dc}->${item.target_dc}`;
        nextDc[key] = {
          mb: Math.round(item.limit_mb_per_sec),
          unlimited: item.limit_mb_per_sec === 0 || item.limit_bytes_per_sec === 0,
        };
      }
      dcLimits = nextDc;

      const nextHdfs: Record<string, { mb: number; unlimited: boolean }> = {};
      for (const item of data.hdfs_limits || []) {
        const key = `${item.source_cluster}->${item.target_cluster}`;
        nextHdfs[key] = {
          mb: Math.round(item.limit_mb_per_sec),
          unlimited: item.limit_mb_per_sec === 0 || item.limit_bytes_per_sec === 0,
        };
      }
      hdfsLimits = nextHdfs;

      // Если кластеры загрузились, настроим defaults создания
      if (data.clusters && data.clusters.length > 0) {
        if (!newJobSourceCluster || newJobSourceCluster === 'demo-cluster') {
          const dc1 = data.clusters.find((c) => c.dc_id === 'dc1' || c.id === 'dc1');
          newJobSourceCluster = dc1 ? dc1.id : data.clusters[0].id;
        }
        if (!newJobTargetCluster || newJobTargetCluster === 'backup-cluster') {
          const dc2 = data.clusters.find((c) => c.dc_id === 'dc2' || c.id === 'dc2');
          newJobTargetCluster = dc2 ? dc2.id : (data.clusters.length > 1 ? data.clusters[1].id : data.clusters[0].id);
        }
      }
    } catch (e) {
      console.error('Ошибка загрузки топологии:', e);
    } finally {
      topologyLoading = false;
    }
  }

  // Загрузка динамически зарегистрированных агентов
  async function loadAgents() {
    if (!user) return;
    agentsLoading = true;
    try {
      registeredAgents = await api.getAgents(true);
    } catch (e) {
      console.error('Ошибка загрузки агентов:', e);
    } finally {
      agentsLoading = false;
    }
  }

  // Загрузка статуса стриминговой аренды по всем кластерам (DC1, DC2, ...)
  async function loadStreamingStatus() {
    if (!user) return;
    streamingLeaseLoading = true;
    try {
      const map = await api.getStreamingLeaseStatuses();
      streamingLeases = map || {};
      streamingLease = (map && (map['dc1'] || Object.values(map)[0])) || null;
    } catch (e) {
      console.error('Ошибка загрузки статуса аренды стриминга:', e);
    } finally {
      streamingLeaseLoading = false;
    }
  }

  async function loadInitialData() {
    await Promise.all([loadJobs(), loadTopology(), loadAgents(), loadStreamingStatus()]);
  }

  // Открытие модалки создания задачи
  function openCreateModal() {
    newJobImpersonationUser = user?.username || '';
    newJobSyncMode = 'MANUAL';
    newJobIsScheduled = false;
    createJobError = null;
    isCreateModalOpen = true;
  }

  // Создание задачи
  async function handleCreateJob(e: Event) {
    e.preventDefault();
    createJobError = null;
    isSubmittingJob = true;

    try {
      const chosenUser = (isAdmin && newJobImpersonationUser.trim())
        ? newJobImpersonationUser.trim()
        : (user?.username || 'writer_user');
      const executionPrincipal = chosenUser.includes('@') ? chosenUser : `${chosenUser}@REALM.LOCAL`;
      const isScheduled = newJobSyncMode === 'SCHEDULED';

      const payload: any = {
        source_cluster_id: newJobSourceCluster,
        target_cluster_id: newJobTargetCluster,
        source_path: newJobSourcePath.trim(),
        target_path: newJobTargetPath.trim(),
        run_as_service_account: false,
        execution_principal: executionPrincipal,
        is_scheduled: isScheduled,
        sync_mode: newJobSyncMode,
        history_retention_runs: Number(newJobHistoryRetention) || 20,
      };

      if (isScheduled) {
        payload.cron_expression = newJobCronPreset;
      }

      await api.createJob(payload);
      isCreateModalOpen = false;
      await loadJobs();
    } catch (e: any) {
      createJobError = e.message || 'Ошибка связи с сервером';
    } finally {
      isSubmittingJob = false;
    }
  }

  // Управление жизненным циклом задач: Запуск / Стоп / Редактирование / Удаление
  async function handleStartJob(id: string) {
    try {
      await api.startJob(id);
      await loadJobs();
    } catch (e: any) {
      alert(e.message || 'Не удалось запустить задачу');
    }
  }

  async function handleStopJob(id: string) {
    if (!confirm(`Остановить задачу ${id.substring(0, 8)}...?`)) return;
    try {
      await api.stopJob(id);
      await loadJobs();
    } catch (e: any) {
      alert(e.message || 'Не удалось остановить задачу');
    }
  }

  function openEditModal(job: Job) {
    editingJobId = job.id;
    editJobSourceCluster = job.source_cluster_id;
    editJobTargetCluster = job.target_cluster_id;
    editJobSourcePath = job.source_path;
    editJobTargetPath = job.target_path;
    const initialUser = job.execution_principal
      ? (job.execution_principal.includes('@') ? job.execution_principal.split('@')[0] : job.execution_principal)
      : (job.created_by || '');
    editJobImpersonationUser = initialUser;
    editJobIsScheduled = job.is_scheduled ?? false;
    editJobCronPreset = job.cron_expression || '@every_5m';
    editJobHistoryRetention = job.history_retention_runs ?? 20;
    editJobError = null;
    isEditModalOpen = true;
  }

  async function handleSaveEditJob(e: Event) {
    e.preventDefault();
    if (!editingJobId) return;
    editJobError = null;
    isSubmittingEditJob = true;

    try {
      const payload: any = {
        source_cluster_id: editJobSourceCluster,
        target_cluster_id: editJobTargetCluster,
        source_path: editJobSourcePath.trim(),
        target_path: editJobTargetPath.trim(),
        is_scheduled: editJobIsScheduled,
        history_retention_runs: Number(editJobHistoryRetention) || 20,
      };

      if (isAdmin && editJobImpersonationUser.trim()) {
        const u = editJobImpersonationUser.trim();
        payload.execution_principal = u.includes('@') ? u : `${u}@REALM.LOCAL`;
      }

      if (editJobIsScheduled) {
        payload.cron_expression = editJobCronPreset;
      }

      await api.updateJob(editingJobId, payload);
      isEditModalOpen = false;
      await loadJobs();
    } catch (e: any) {
      editJobError = e.message || 'Ошибка связи с сервером';
    } finally {
      isSubmittingEditJob = false;
    }
  }

  async function handleDeleteJob(id: string) {
    if (!confirm(`Вы действительно хотите удалить задачу ${id.substring(0, 8)}...? Это действие необратимо.`)) return;
    try {
      await api.deleteJob(id);
      await loadJobs();
    } catch (e: any) {
      alert(e.message || 'Не удалось удалить задачу');
    }
  }

  // История всех запусков задачи
  async function openHistoryModal(job: Job) {
    selectedJobForHistory = job;
    runsRetentionInput = job.history_retention_runs ?? 20;
    retentionSaveSuccess = false;
    isHistoryModalOpen = true;
    await loadJobRuns(job.id);
  }

  async function loadJobRuns(jobId: string) {
    runsLoading = true;
    try {
      jobRuns = await api.getJobRuns(jobId);
    } catch (e) {
      console.error('Ошибка загрузки истории запусков:', e);
      jobRuns = [];
    } finally {
      runsLoading = false;
    }
  }

  async function handleSaveRetention(jobId: string) {
    const val = Number(runsRetentionInput);
    if (!val || val < 1 || val > 500) {
      alert('Глубина истории должна быть от 1 до 500 запусков');
      return;
    }
    savingRetention = true;
    retentionSaveSuccess = false;
    try {
      await api.updateJobRetention(jobId, val);
      retentionSaveSuccess = true;
      if (selectedJobForHistory) {
        selectedJobForHistory.history_retention_runs = val;
      }
      await loadJobs();
      await loadJobRuns(jobId);
      setTimeout(() => {
        retentionSaveSuccess = false;
      }, 3000);
    } catch (e: any) {
      alert(e.message || 'Не удалось сохранить глубину истории');
    } finally {
      savingRetention = false;
    }
  }

  function formatDuration(seconds?: number): string {
    if (seconds === undefined || seconds === null) return '—';
    if (seconds < 60) return `${seconds.toFixed(1)} с`;
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return `${m} мин ${s} с`;
  }

  // Сохранение лимитов
  async function saveGlobalLimit() {
    topologyStatusMsg = null;
    const limitMb = globalUnlimited ? 0 : Number(globalLimitMb) || 0;
    try {
      await api.saveGlobalLimit(limitMb * 1024 * 1024);
      topologyStatusMsg = 'Глобальный лимит WAN успешно обновлен';
      await loadTopology();
    } catch (e: any) {
      topologyStatusMsg = `Ошибка: ${e.message || 'Не удалось сохранить'}`;
    }
  }

  async function saveDcLimit(key: string) {
    topologyStatusMsg = null;
    const parts = key.split('->');
    if (parts.length !== 2) return;
    const item = dcLimits[key];
    const mb = item.unlimited ? 0 : Number(item.mb) || 0;

    try {
      await api.saveDcLimit(parts[0], parts[1], mb);
      topologyStatusMsg = `Лимит для канала ${key} успешно обновлен`;
      await loadTopology();
    } catch (e: any) {
      topologyStatusMsg = `Ошибка: ${e.message || 'Не удалось сохранить'}`;
    }
  }

  async function saveHdfsLimit(key: string) {
    topologyStatusMsg = null;
    const parts = key.split('->');
    if (parts.length !== 2) return;
    const item = hdfsLimits[key];
    const mb = item.unlimited ? 0 : Number(item.mb) || 0;

    try {
      await api.saveHdfsLimit(parts[0], parts[1], mb);
      topologyStatusMsg = `Лимит для кластеров ${key} успешно обновлен`;
      await loadTopology();
    } catch (e: any) {
      topologyStatusMsg = `Ошибка: ${e.message || 'Не удалось сохранить'}`;
    }
  }

  function formatBytes(bytes: number): string {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  function formatTime(isoString?: string): string {
    if (!isoString) return '—';
    try {
      const d = new Date(isoString);
      if (isNaN(d.getTime())) return '—';
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch {
      return '—';
    }
  }

  function getProgressPercent(job: Job): number {
    if (job.progress_percent !== undefined && !isNaN(job.progress_percent) && job.progress_percent > 0) {
      return Math.min(100, Math.max(0, Math.round(job.progress_percent)));
    }
    if (job.total_bytes > 0 && job.copied_bytes > 0) {
      return Math.min(100, Math.max(0, Math.round((job.copied_bytes / job.total_bytes) * 100)));
    }
    return job.status === 'COMPLETED' ? 100 : 0;
  }

  function formatSpeed(job: Job): string {
    const speed = job.average_speed_mb_s ?? job.transfer_speed_mb_s;
    if (speed !== undefined && speed !== null && speed > 0) {
      return `${speed.toFixed(1)} МБ/с`;
    }
    if (job.copied_bytes > 0 && (job.started_at || job.created_at)) {
      const start = new Date(job.started_at || job.created_at!).getTime();
      const end = job.completed_at ? new Date(job.completed_at).getTime() : Date.now();
      const elapsedSec = (end - start) / 1000;
      if (elapsedSec > 0) {
        const mb = (job.copied_bytes / (1024 * 1024)) / elapsedSec;
        if (mb > 0) return `${mb.toFixed(1)} МБ/с`;
      }
    }
    return job.status === 'RUNNING' ? '0.0 МБ/с' : '—';
  }

  function formatEta(job: Job): string {
    if (job.status === 'COMPLETED') {
      return formatTime(job.completed_at || job.updated_at);
    }
    if (job.status === 'CANCELLED') {
      return 'Отменена';
    }
    if (job.status === 'FAILED') {
      return 'Ошибка';
    }
    if (job.status === 'QUEUED') {
      return 'В очереди';
    }
    if (job.estimated_completion_at) {
      const etaTime = formatTime(job.estimated_completion_at);
      const diffMs = new Date(job.estimated_completion_at).getTime() - Date.now();
      const diffMin = Math.round(diffMs / 60000);
      const remainingStr = diffMin > 0 ? `~${diffMin} мин` : '< 1 мин';
      return `~${etaTime} (${remainingStr})`;
    }
    if (job.total_bytes > 0 && job.copied_bytes > 0 && (job.started_at || job.created_at)) {
      const start = new Date(job.started_at || job.created_at!).getTime();
      const elapsedSec = (Date.now() - start) / 1000;
      if (elapsedSec > 0) {
        const bytesPerSec = job.copied_bytes / elapsedSec;
        const remainingBytes = Math.max(0, job.total_bytes - job.copied_bytes);
        if (bytesPerSec > 0 && remainingBytes > 0) {
          const remainingSec = Math.round(remainingBytes / bytesPerSec);
          const etaDate = new Date(Date.now() + remainingSec * 1000);
          const etaTime = etaDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
          const remMin = Math.round(remainingSec / 60);
          const remStr = remMin > 0 ? `~${remMin} мин` : `${remainingSec} сек`;
          return `~${etaTime} (${remStr})`;
        }
      }
    }
    return 'Оценка времени...';
  }

  function formatDcLabel(key: string): string {
    const parts = key.split('->');
    if (parts.length !== 2) return key;
    const d1 = topology?.datacenters.find((d) => d.id === parts[0]);
    const d2 = topology?.datacenters.find((d) => d.id === parts[1]);
    const name1 = d1 ? d1.id.toUpperCase() : parts[0].toUpperCase();
    const name2 = d2 ? d2.id.toUpperCase() : parts[1].toUpperCase();
    return `${name1} → ${name2}`;
  }

  function formatDcSub(key: string): string {
    const parts = key.split('->');
    if (parts.length !== 2) return 'магистраль ЦОД';
    const d1 = topology?.datacenters.find((d) => d.id === parts[0]);
    const d2 = topology?.datacenters.find((d) => d.id === parts[1]);
    const loc1 = d1?.name ? d1.name.split(' ')[0] + ' ' + d1.name.split(' ')[1] : parts[0];
    const loc2 = d2?.name ? d2.name.split(' ')[0] + ' ' + d2.name.split(' ')[1] : parts[1];
    return `${loc1} → ${loc2}`;
  }

  function formatHdfsLabel(key: string): string {
    const parts = key.split('->');
    if (parts.length !== 2) return key;
    const c1 = topology?.clusters.find((c) => c.id === parts[0]);
    const c2 = topology?.clusters.find((c) => c.id === parts[1]);
    const name1 = c1 ? c1.name : parts[0];
    const name2 = c2 ? c2.name : parts[1];
    return `${name1} → ${name2}`;
  }


  onMount(async () => {
    // Подписка на 401 Unauthorized / истечение сессии
    api.onUnauthorized((msg) => {
      user = null;
      authErrorMessage = msg;
    });

    try {
      user = await api.getMe();
      await loadInitialData();
    } catch {
      try {
        const autoUser = await api.tryAutoLogin();
        if (autoUser) {
          user = autoUser;
          await loadInitialData();
        }
      } catch {}
    } finally {
      authLoading = false;
    }

    pollTimer = setInterval(() => {
      if (user) {
        if (activeTab === 'jobs') {
          loadJobs();
        } else if (activeTab === 'topology') {
          loadAgents();
          loadStreamingStatus();
        }
      }
    }, 3000);
  });

  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
  });
</script>

{#snippet headerCenterNav()}
  <!-- Навигационные вкладки -->
  <nav class="flex items-center gap-1 bg-slate-100 dark:bg-slate-950/80 p-1 rounded-lg border border-slate-200 dark:border-slate-800">
    <button
      onclick={() => (activeTab = 'jobs')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer {activeTab === 'jobs' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      HDFS Replication
    </button>
    <button
      onclick={() => (activeTab = 'hms')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer {activeTab === 'hms' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      HMS Replication
    </button>
    <button
      onclick={() => (activeTab = 'topology')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer {activeTab === 'topology' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      Топология ЦОД и Полоса
    </button>
    <button
      onclick={() => (activeTab = 'dr')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer flex items-center gap-1.5 {activeTab === 'dr' ? 'bg-rose-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      <span class="w-1.5 h-1.5 rounded-full {activeTab === 'dr' ? 'bg-white' : 'bg-rose-500'}"></span>
      Disaster Recovery
    </button>
  </nav>
{/snippet}

{#if authLoading}
  <div class="min-h-screen bg-slate-50 dark:bg-slate-950 flex flex-col items-center justify-center text-slate-800 dark:text-slate-100 gap-3">
    <div class="w-8 h-8 border-3 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
    <span class="text-xs font-medium text-slate-500 dark:text-slate-400">Проверка сессии...</span>
  </div>
{:else if !user}
  <!-- ЕДИНАЯ ФОРМА АУТЕНТИФИКАЦИИ ДЛЯ ВСЕХ СЕРВИСОВ ПЛАТФОРМЫ -->
  <LoginModal
    title="Hadoop gRPC Replicator"
    subtitle="Аутентификация LDAP & Kerberos SSO"
    icon={ArrowLeftRight}
    isModal={false}
    app="replicator"
    initialError={authErrorMessage}
    onLogin={handleLogin}
    onKerberosSso={handleKerberosSso}
  />
{:else}
  <!-- ОСНОВНОЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ -->
  <div class="min-h-screen flex flex-col bg-slate-50 dark:bg-slate-950 text-slate-900 dark:text-slate-100 antialiased">
    <!-- ЕДИНЫЙ HEADER ПЛАТФОРМЫ -->
    <Header
      title="Hadoop gRPC Replicator"
      subtitle="WAN Replication"
      icon={ArrowLeftRight}
      {user}
      centerContent={headerCenterNav}
      onLogout={handleLogout}
      onLoginClick={() => (isLoginModalOpen = true)}
    />

    <!-- МОДАЛЬНОЕ ОКНО ПЕРЕКЛЮЧЕНИЯ ПОЛЬЗОВАТЕЛЯ (ЕДИНАЯ ФОРМА LOGINMODAL) -->
    {#if isLoginModalOpen}
      <LoginModal
        title="Hadoop gRPC Replicator"
        subtitle="Смена пользователя LDAP & Kerberos SSO"
        icon={ArrowLeftRight}
        isModal={true}
        app="replicator"
        initialError={authErrorMessage}
        onClose={() => (isLoginModalOpen = false)}
        onLogin={handleLogin}
        onKerberosSso={handleKerberosSso}
      />
    {/if}

    <!-- КОНТЕНТ ВКЛАДКИ 1: ЗАДАЧИ РЕПЛИКАЦИИ -->
    {#if activeTab === 'jobs'}
      <main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
        <!-- Верхняя панель: Статистика и Кнопка создания -->
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <!-- Карточки статистики (с интерактивным фильтром статуса) -->
          <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-3 flex-1">
            <button
              type="button"
              onclick={() => (selectedStatus = 'ALL')}
              class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'ALL'
                ? 'bg-slate-100 dark:bg-slate-800 border-slate-400 dark:border-slate-500 ring-2 ring-slate-400/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300 dark:hover:border-slate-700'}"
            >
              <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Всего задач</span>
              <span class="text-xl font-bold text-slate-900 dark:text-slate-100">{stats.total}</span>
            </button>

            <button
              type="button"
              onclick={() => toggleStatusFilter('RUNNING')}
              class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'RUNNING'
                ? 'bg-sky-50 dark:bg-sky-950/40 border-sky-400 dark:border-sky-600 ring-2 ring-sky-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-sky-300 dark:hover:border-sky-800'}"
            >
              <span class="text-[11px] font-medium text-sky-600 dark:text-sky-400 block">В работе</span>
              <span class="text-xl font-bold text-sky-600 dark:text-sky-400">{stats.running}</span>
            </button>

            <button
              type="button"
              onclick={() => toggleStatusFilter('QUEUED')}
              class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'QUEUED'
                ? 'bg-amber-50 dark:bg-amber-950/40 border-amber-400 dark:border-amber-600 ring-2 ring-amber-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-amber-300 dark:hover:border-amber-800'}"
            >
              <span class="text-[11px] font-medium text-amber-600 dark:text-amber-400 block">В очереди</span>
              <span class="text-xl font-bold text-amber-600 dark:text-amber-400">{stats.queued}</span>
            </button>

            <button
              type="button"
              onclick={() => toggleStatusFilter('SCHEDULED')}
              class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'SCHEDULED'
                ? 'bg-indigo-50 dark:bg-indigo-950/40 border-indigo-400 dark:border-indigo-600 ring-2 ring-indigo-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-indigo-300 dark:hover:border-indigo-800'}"
            >
              <span class="text-[11px] font-medium text-indigo-600 dark:text-indigo-400 block">По расписанию</span>
              <span class="text-xl font-bold text-indigo-600 dark:text-indigo-400">{stats.scheduled}</span>
            </button>

            <button
              type="button"
              onclick={() => toggleStatusFilter('STREAMING')}
              class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'STREAMING'
                ? 'bg-purple-50 dark:bg-purple-950/40 border-purple-400 dark:border-purple-600 ring-2 ring-purple-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-purple-300 dark:hover:border-purple-800'}"
            >
              <div class="flex items-center justify-between">
                <span class="text-[11px] font-medium text-purple-600 dark:text-purple-400 block">Live Inotify</span>
                {#if stats.streaming > 0}
                  <span class="w-2 h-2 rounded-full bg-purple-500 animate-pulse" title="Активный поток HDFS событий"></span>
                {/if}
              </div>
              <span class="text-xl font-bold text-purple-600 dark:text-purple-400">{stats.streaming}</span>
            </button>

            <button
              type="button"
              onclick={() => toggleStatusFilter('COMPLETED')}
              class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'COMPLETED'
                ? 'bg-emerald-50 dark:bg-emerald-950/40 border-emerald-400 dark:border-emerald-600 ring-2 ring-emerald-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-emerald-300 dark:hover:border-emerald-800'}"
            >
              <span class="text-[11px] font-medium text-emerald-600 dark:text-emerald-400 block">Завершено</span>
              <span class="text-xl font-bold text-emerald-600 dark:text-emerald-400">{stats.completed}</span>
            </button>

            <button
              type="button"
              onclick={() => toggleStatusFilter('FAILED')}
              class="p-3 rounded-xl border text-left transition-all cursor-pointer shadow-2xs hover:shadow-xs {selectedStatus === 'FAILED'
                ? 'bg-rose-50 dark:bg-rose-950/40 border-rose-400 dark:border-rose-600 ring-2 ring-rose-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-rose-300 dark:hover:border-rose-800'}"
            >
              <span class="text-[11px] font-medium text-rose-600 dark:text-rose-400 block">Ошибки</span>
              <span class="text-xl font-bold text-rose-600 dark:text-rose-400">{stats.failed}</span>
            </button>
          </div>

          <!-- Кнопка Новая задача -->
          <div class="flex items-center gap-2 self-end sm:self-auto shrink-0">
            <button
              onclick={loadJobs}
              class="p-2.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-300 transition cursor-pointer shadow-2xs"
              title="Обновить список"
            >
              <RefreshCw class="w-4 h-4" />
            </button>
            {#if canCreate}
              <button
                onclick={openCreateModal}
                class="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-sky-600 hover:bg-sky-500 text-white font-semibold text-xs transition cursor-pointer shadow-md shadow-sky-600/20"
              >
                <Plus class="w-4 h-4" />
                <span>Новая задача репликации</span>
              </button>
            {:else}
              <button
                disabled
                class="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-400 dark:text-slate-500 font-semibold text-xs opacity-60 cursor-not-allowed"
                title="Доступ только для чтения (READER): создание задач запрещено"
              >
                <Plus class="w-4 h-4" />
                <span>Создание недоступно (RO)</span>
              </button>
            {/if}
          </div>
        </div>

        <!-- Панель фильтров и поиска -->
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-3.5 shadow-2xs">
          <div class="flex flex-col md:flex-row items-stretch md:items-center justify-between gap-3">
            <!-- Левая часть: Поиск + Фильтр Статус + Фильтр Автор -->
            <div class="flex flex-wrap items-center gap-3 flex-1">
              <!-- Поиск -->
              <div class="relative min-w-[220px] flex-1 max-w-sm">
                <Search class="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
                <input
                  type="text"
                  bind:value={searchQuery}
                  placeholder="Поиск по ID, путям, кластерам, principal..."
                  class="w-full pl-9 pr-8 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 text-xs text-slate-800 dark:text-slate-200 placeholder-slate-400 focus:ring-2 focus:ring-sky-500 focus:outline-none transition"
                />
                {#if searchQuery}
                  <button
                    type="button"
                    onclick={() => (searchQuery = '')}
                    class="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 cursor-pointer p-0.5"
                    title="Очистить поиск"
                  >
                    <X class="w-3.5 h-3.5" />
                  </button>
                {/if}
              </div>

              <!-- Фильтр по статусу -->
              <div class="flex items-center gap-2">
                <label for="filter-status" class="text-xs text-slate-500 dark:text-slate-400 font-medium flex items-center gap-1.5 whitespace-nowrap">
                  <Filter class="w-3.5 h-3.5 text-slate-400" />
                  <span>Статус:</span>
                </label>
                <select
                  id="filter-status"
                  bind:value={selectedStatus}
                  class="px-3 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 text-xs text-slate-700 dark:text-slate-300 focus:ring-2 focus:ring-sky-500 focus:outline-none cursor-pointer"
                >
                  <option value="ALL">Все статусы ({jobs.length})</option>
                  <option value="RUNNING">В работе ({stats.running})</option>
                  <option value="QUEUED">В очереди ({stats.queued})</option>
                  <option value="SCHEDULED">По расписанию ({stats.scheduled})</option>
                  <option value="STREAMING">Live Inotify ({stats.streaming})</option>
                  <option value="COMPLETED">Завершено ({stats.completed})</option>
                  <option value="FAILED">Ошибки ({stats.failed})</option>
                  <option value="CANCELLED">Отменено ({jobs.filter((j) => j.status === 'CANCELLED').length})</option>
                </select>
              </div>

              <!-- Фильтр по автору -->
              <div class="flex items-center gap-2">
                <label for="filter-author" class="text-xs text-slate-500 dark:text-slate-400 font-medium flex items-center gap-1.5 whitespace-nowrap">
                  <User class="w-3.5 h-3.5 text-slate-400" />
                  <span>Автор:</span>
                </label>
                <select
                  id="filter-author"
                  bind:value={selectedAuthor}
                  class="px-3 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 text-xs text-slate-700 dark:text-slate-300 focus:ring-2 focus:ring-sky-500 focus:outline-none cursor-pointer"
                >
                  <option value="ALL">Все авторы ({authorsList.length})</option>
                  {#each authorsList as author}
                    <option value={author}>
                      {author} ({jobs.filter((j) => j.created_by === author).length})
                    </option>
                  {/each}
                </select>
              </div>
            </div>

            <!-- Правая часть: Счетчик и Сброс фильтров -->
            <div class="flex items-center justify-between md:justify-end gap-3 shrink-0 pt-2 md:pt-0 border-t md:border-t-0 border-slate-100 dark:border-slate-800/80">
              <span class="text-xs text-slate-500 dark:text-slate-400 font-medium whitespace-nowrap">
                Показано: <strong class="text-slate-800 dark:text-slate-200 font-semibold">{filteredJobs.length}</strong> из {jobs.length}
              </span>

              {#if hasActiveFilters}
                <button
                  type="button"
                  onclick={resetFilters}
                  class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 transition cursor-pointer"
                  title="Сбросить активные фильтры"
                >
                  <RotateCcw class="w-3.5 h-3.5 text-slate-400" />
                  <span>Сбросить</span>
                </button>
              {/if}
            </div>
          </div>
        </div>

        <!-- Таблица задач -->
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-xs overflow-hidden">
          <div class="overflow-x-auto">
            <table class="w-full table-fixed text-left border-collapse text-xs min-w-[960px]">
              <thead>
                <tr class="bg-slate-50/80 dark:bg-slate-950/60 border-b border-slate-200 dark:border-slate-800 text-slate-500 dark:text-slate-400 font-semibold uppercase tracking-wider text-[10px]">
                  <th scope="col" class="py-3 px-4 w-32">ID задачи</th>
                  <th scope="col" class="py-3 px-4 w-44">Маршрут</th>
                  <th scope="col" class="py-3 px-4">HDFS Пути</th>
                  <th scope="col" class="py-3 px-4 w-40">Автор и Principal</th>
                  <th scope="col" class="py-3 px-4 w-64">Прогресс</th>
                  <th scope="col" class="py-3 px-4 w-32">Статус</th>
                  <th scope="col" class="py-3 px-4 w-44 text-right">Действия</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60">
                {#if jobs.length === 0}
                  <tr>
                    <td colspan="7" class="py-12 text-center text-slate-400 dark:text-slate-500">
                      <div class="flex flex-col items-center gap-2">
                        <ArrowLeftRight class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                        <span>Нет активных или выполненных задач репликации</span>
                        <button
                          onclick={openCreateModal}
                          class="mt-2 text-xs text-sky-600 dark:text-sky-400 hover:underline font-semibold cursor-pointer"
                        >
                          Создать первую задачу
                        </button>
                      </div>
                    </td>
                  </tr>
                {:else if filteredJobs.length === 0}
                  <tr>
                    <td colspan="7" class="py-12 text-center text-slate-400 dark:text-slate-500">
                      <div class="flex flex-col items-center gap-2">
                        <Filter class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                        <span class="font-medium text-slate-700 dark:text-slate-300">Задачи по заданным фильтрам не найдены</span>
                        <span class="text-[11px] text-slate-400">Попробуйте изменить статус, автора или строку поиска</span>
                        <button
                          type="button"
                          onclick={resetFilters}
                          class="mt-2 flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-xs font-medium text-slate-700 dark:text-slate-200 transition cursor-pointer"
                        >
                          <RotateCcw class="w-3.5 h-3.5 text-slate-400" />
                          <span>Сбросить все фильтры</span>
                        </button>
                      </div>
                    </td>
                  </tr>
                {:else}
                  {#each filteredJobs as job (job.id)}
                    {@const isActive = job.status === 'RUNNING' || job.status === 'QUEUED'}
                    <tr class="hover:bg-slate-50/60 dark:hover:bg-slate-850/50 transition-colors">
                      <!-- ID -->
                      <td class="py-3.5 px-4 font-mono font-medium text-slate-800 dark:text-slate-200">
                        <span title={job.id}>{job.id.substring(0, 8)}...</span>
                        {#if job.is_scheduled}
                          <span class="inline-flex items-center gap-0.5 px-1.5 py-0.2 rounded text-[9px] font-mono bg-indigo-50 dark:bg-indigo-950/50 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800 ml-1" title="Периодическая задача по расписанию">
                            <Calendar class="w-2.5 h-2.5" />
                            {job.cron_expression || 'cron'}
                          </span>
                        {/if}
                        {#if job.sync_mode === 'STREAMING_INOTIFY' || job.status === 'STREAMING'}
                          <span class="inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[9px] font-semibold bg-purple-50 dark:bg-purple-950/60 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-800 ml-1" title="Потоковая репликация HDFS Inotify (Near-Zero RPO)">
                            <span class="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
                            Inotify
                          </span>
                        {/if}
                      </td>

                      <!-- Маршрут -->
                      <td class="py-3.5 px-4">
                        <div class="flex items-center gap-1.5 font-medium">
                          <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-mono text-[11px]">
                            {job.source_cluster_id}
                          </span>
                          <span class="text-slate-400">→</span>
                          <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-mono text-[11px]">
                            {job.target_cluster_id}
                          </span>
                        </div>
                      </td>

                      <!-- Пути -->
                      <td class="py-3.5 px-4 overflow-hidden">
                        <div class="truncate font-mono text-[11px] text-slate-700 dark:text-slate-300" title={job.source_path}>
                          <span class="text-slate-400">src:</span> {job.source_path}
                        </div>
                        <div class="truncate font-mono text-[11px] text-slate-500 dark:text-slate-400" title={job.target_path}>
                          <span class="text-slate-400">dst:</span> {job.target_path}
                        </div>
                      </td>

                      <!-- Владелец -->
                      <td class="py-3.5 px-4 truncate">
                        <div class="font-medium text-slate-800 dark:text-slate-200 truncate">{job.created_by || 'system'}</div>
                        <div class="text-[10px] font-mono truncate" title={job.execution_principal || ''}>
                          {#if job.run_as_service_account}
                            <span class="text-slate-400">🤖 {job.execution_principal || 'tech-service'}</span>
                          {:else}
                            <span class="text-sky-600 dark:text-sky-400 inline-flex items-center gap-1">
                              <span>👤 {job.execution_principal || job.created_by}</span>
                              <span class="text-[9px] px-1 py-0.2 rounded bg-sky-100 dark:bg-sky-950/60 font-semibold text-sky-700 dark:text-sky-300">doAs</span>
                            </span>
                          {/if}
                        </div>
                      </td>

                      <!-- Прогресс -->
                      <td class="py-3.5 px-4">
                        {#if job.sync_mode === 'STREAMING_INOTIFY' || job.status === 'STREAMING'}
                          <div class="p-2 rounded-lg bg-purple-50/60 dark:bg-purple-950/30 border border-purple-200/60 dark:border-purple-800/40 space-y-1">
                            <div class="flex items-center justify-between text-[11px] font-semibold">
                              <span class="text-purple-700 dark:text-purple-300 flex items-center gap-1 font-mono">
                                <span class="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
                                Live Sync
                              </span>
                              <span class="font-mono text-[10px] text-slate-500 dark:text-slate-400">
                                TxID: {job.last_processed_txid ?? 0}
                              </span>
                            </div>
                            <div class="flex items-center justify-between text-[10px]">
                              <span class="text-slate-400">Лаг событий:</span>
                              {#if (job.txid_lag ?? 0) === 0}
                                <span class="font-mono font-semibold text-emerald-600 dark:text-emerald-400">0 tx (In-sync)</span>
                              {:else}
                                <span class="font-mono font-semibold text-amber-600 dark:text-amber-400">+{job.txid_lag} tx</span>
                              {/if}
                            </div>
                          </div>
                        {:else}
                          <div class="flex items-center justify-between text-[11px] font-semibold mb-1">
                            <span class="{job.status === 'FAILED' ? 'text-rose-600 dark:text-rose-400' : job.status === 'COMPLETED' ? 'text-emerald-600 dark:text-emerald-400' : 'text-sky-600 dark:text-sky-400'} font-mono">
                              {getProgressPercent(job)}%
                            </span>
                            <span class="text-slate-400 font-mono text-[10px]">
                              {formatBytes(job.copied_bytes)} / {formatBytes(job.total_bytes)}
                            </span>
                          </div>
                          <div class="w-full bg-slate-100 dark:bg-slate-800 h-2 rounded-full overflow-hidden shadow-inner">
                            <div
                              class="h-full rounded-full transition-all duration-300 {job.status === 'FAILED' ? 'bg-rose-500' : job.status === 'COMPLETED' ? 'bg-emerald-500' : 'bg-gradient-to-r from-sky-500 to-indigo-500'}"
                              style="width: {getProgressPercent(job)}%"
                            ></div>
                          </div>
                        {/if}

                        <!-- Детализация таймингов и скорости передачи -->
                        <div class="mt-2 space-y-1 text-[10px] text-slate-500 dark:text-slate-400 border-t border-slate-100 dark:border-slate-800/80 pt-1.5">
                          <div class="flex items-center justify-between">
                            <span class="flex items-center gap-1 text-slate-400">
                              <Play class="w-2.5 h-2.5 text-sky-500 shrink-0" />
                              <span>Старт:</span>
                            </span>
                            <span class="font-mono text-slate-700 dark:text-slate-300 font-medium">
                              {formatTime(job.started_at || job.created_at)}
                            </span>
                          </div>

                          <div class="flex items-center justify-between">
                            <span class="flex items-center gap-1 text-slate-400">
                              <Clock class="w-2.5 h-2.5 text-indigo-500 shrink-0" />
                              <span>{job.status === 'COMPLETED' ? 'Финиш:' : 'ETA:'}</span>
                            </span>
                            <span class="font-mono text-slate-700 dark:text-slate-300 font-medium truncate max-w-[150px]" title={formatEta(job)}>
                              {formatEta(job)}
                            </span>
                          </div>

                          <div class="flex items-center justify-between">
                            <span class="flex items-center gap-1 text-slate-400">
                              <Zap class="w-2.5 h-2.5 text-amber-500 shrink-0" />
                              <span>Ср. скорость:</span>
                            </span>
                            <span class="font-mono font-semibold {job.status === 'RUNNING' ? 'text-sky-600 dark:text-sky-400' : 'text-slate-700 dark:text-slate-300'}">
                              {formatSpeed(job)}
                            </span>
                          </div>

                          {#if (job.total_objects ?? 0) > 0}
                            <div class="flex items-center justify-between">
                              <span class="flex items-center gap-1 text-slate-400">
                                <Files class="w-2.5 h-2.5 text-teal-500 shrink-0" />
                                <span>Объекты:</span>
                              </span>
                              <span class="font-mono text-slate-700 dark:text-slate-300 font-medium">
                                {(job.transferred_objects ?? 0) + (job.skipped_objects ?? 0)} / {job.total_objects}
                                {#if (job.skipped_objects ?? 0) > 0}
                                  <span class="text-slate-400 text-[9px]">(проп. {job.skipped_objects})</span>
                                {/if}
                                {#if (job.failed_objects ?? 0) > 0}
                                  <span class="text-rose-500 text-[9px] font-bold">(! {job.failed_objects})</span>
                                {/if}
                              </span>
                            </div>
                          {/if}
                        </div>
                      </td>

                      <!-- Статус -->
                      <td class="py-3.5 px-4">
                        {#if job.status === 'STREAMING'}
                          <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-purple-50 dark:bg-purple-950/60 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-800">
                            <span class="w-2 h-2 rounded-full bg-purple-500 animate-pulse"></span>
                            STREAMING
                          </span>
                        {:else}
                          <StatusBadge status={job.status} />
                        {/if}
                        {#if job.error_message}
                          <div class="text-[10px] text-rose-500 truncate max-w-[150px] mt-0.5" title={job.error_message}>
                            {job.error_message}
                          </div>
                        {:else if job.message}
                          <div class="text-[10px] text-slate-500 truncate max-w-[150px] mt-0.5" title={job.message}>
                            {job.message}
                          </div>
                        {/if}
                      </td>

                      <!-- Действия: Старт / Стоп / Редактировать / Удалить / История -->
                      <td class="py-3.5 px-4 text-right">
                        <div class="flex items-center justify-end gap-1">
                          <!-- 1. Старт / Перезапуск -->
                          <button
                            onclick={() => handleStartJob(job.id)}
                            disabled={isActive || !canManageJob(job)}
                            class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-emerald-50 dark:hover:bg-emerald-950/40 hover:border-emerald-300 dark:hover:border-emerald-700 text-emerald-600 dark:text-emerald-400"
                            title={!canManageJob(job) ? 'Режим только чтения: управление недоступно' : (isActive ? 'Задача уже активна (в очереди или выполняется)' : 'Запустить задачу')}
                          >
                            <Play class="w-3.5 h-3.5 fill-current" />
                          </button>

                          <!-- 2. Стоп -->
                          <button
                            onclick={() => handleStopJob(job.id)}
                            disabled={!isActive || !canManageJob(job)}
                            class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-rose-50 dark:hover:bg-rose-950/40 hover:border-rose-300 dark:hover:border-rose-700 text-rose-600 dark:text-rose-400"
                            title={!canManageJob(job) ? 'Режим только чтения: управление недоступно' : (isActive ? 'Остановить задачу' : 'Задача не активна (остановлена)')}
                          >
                            <Square class="w-3.5 h-3.5 fill-current" />
                          </button>

                          <!-- 3. Редактировать -->
                          <button
                            onclick={() => openEditModal(job)}
                            disabled={isActive || !canManageJob(job)}
                            class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-sky-50 dark:hover:bg-sky-950/40 hover:border-sky-300 dark:hover:border-sky-700 text-sky-600 dark:text-sky-400"
                            title={!canManageJob(job) ? 'Режим только чтения: редактирование недоступно' : (isActive ? 'Нельзя редактировать активную задачу (сначала остановите)' : 'Редактировать параметры')}
                          >
                            <Pencil class="w-3.5 h-3.5" />
                          </button>

                          <!-- 4. История всех запусков со статистикой -->
                          <button
                            onclick={() => openHistoryModal(job)}
                            class="relative p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer hover:bg-indigo-50 dark:hover:bg-indigo-950/40 hover:border-indigo-300 dark:hover:border-indigo-700 text-indigo-600 dark:text-indigo-400"
                            title="История всех запусков со статистикой"
                          >
                            <History class="w-3.5 h-3.5" />
                            {#if job.runs_count && job.runs_count > 0}
                              <span class="absolute -top-1 -right-1 px-1 py-0.2 text-[8px] font-mono font-bold bg-indigo-600 text-white rounded-full leading-none shadow-xs">
                                {job.runs_count}
                              </span>
                            {/if}
                          </button>

                          <!-- 5. Удалить -->
                          <button
                            onclick={() => handleDeleteJob(job.id)}
                            disabled={isActive || !canManageJob(job)}
                            class="p-1.5 rounded-lg border border-slate-200 dark:border-slate-800 transition-colors shadow-2xs cursor-pointer disabled:opacity-25 disabled:cursor-not-allowed hover:bg-rose-50 dark:hover:bg-rose-950/40 hover:border-rose-400 dark:hover:border-rose-800 text-slate-400 hover:text-rose-600 dark:hover:text-rose-400"
                            title={!canManageJob(job) ? 'Режим только чтения: удаление недоступно' : (isActive ? 'Нельзя удалить активную задачу (сначала остановите)' : 'Удалить задачу')}
                          >
                            <Trash2 class="w-3.5 h-3.5" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  {/each}
                {/if}
              </tbody>
            </table>
          </div>
        </div>
      </main>

    <!-- КОНТЕНТ ВКЛАДКИ: РЕПЛИКАЦИЯ HIVE METASTORE (HMS REPLICATION) -->
    {:else if activeTab === 'hms'}
      <main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
        <HmsReplicationView {topology} {user} />
      </main>

    <!-- КОНТЕНТ ВКЛАДКИ: ТОПОЛОГИЯ ЦОД И ПОЛОСА ПРОПУСКАНИЯ -->
    {:else if activeTab === 'topology'}
      <main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
        <!-- Заголовок и статус -->
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <div>
            <h2 class="text-lg font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
              <Layers class="w-5 h-5 text-sky-600" />
              Топология ЦОД и Управление полосой WAN
            </h2>
            <p class="text-xs text-slate-500 dark:text-slate-400">
              Настройка ограничений сетевого канала между Дата-центрами (DC-DC) и кластерами HDFS в рантайме.
            </p>
          </div>
          <button
            onclick={loadTopology}
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-xs font-medium text-slate-600 dark:text-slate-300 transition cursor-pointer self-start sm:self-auto shadow-2xs"
          >
            <RefreshCw class="w-3.5 h-3.5 {topologyLoading ? 'animate-spin' : ''}" />
            <span>Обновить топологию</span>
          </button>
        </div>

        {#if !isAdmin}
          <!-- Предупреждение для не-админов -->
          <div class="p-3 bg-amber-50 dark:bg-amber-950/40 border border-amber-200 dark:border-amber-800 rounded-xl flex items-center gap-2.5 text-xs text-amber-800 dark:text-amber-200 font-medium">
            <Shield class="w-4 h-4 text-amber-600 shrink-0" />
            <span>Режим только для чтения: изменять сетевые лимиты и топологию каналов могут только администраторы платформы (ADM).</span>
          </div>
        {/if}

        {#if topologyStatusMsg}
          <div class="p-3 bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800 rounded-xl flex items-center gap-2.5 text-xs text-emerald-800 dark:text-emerald-200 font-medium">
            <Check class="w-4 h-4 text-emerald-600 shrink-0" />
            <span>{topologyStatusMsg}</span>
          </div>
        {/if}

        <!-- 1. Дата-центры и привязка кластеров (2 ЦОД по 50% экрана) -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          {#if topology && topology.datacenters}
            {#each topology.datacenters as dc}
              <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs flex flex-col justify-between">
                <div>
                  <div class="flex items-center justify-between mb-2">
                    <span class="px-2 py-0.5 rounded font-mono font-bold text-xs bg-sky-50 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300 border border-sky-200 dark:border-sky-800">
                      {dc.id.toUpperCase()}
                    </span>
                    <span class="text-[11px] text-slate-400 font-medium">{dc.location}</span>
                  </div>
                  <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">{dc.name}</h3>
                  <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{dc.description}</p>
                </div>

                <!-- Кластеры в этом ЦОД -->
                <div class="mt-4 pt-3 border-t border-slate-100 dark:border-slate-800">
                  <div class="flex items-center justify-between mb-2">
                    <span class="text-[10px] font-semibold uppercase text-slate-400 tracking-wider">
                      Кластеры HDFS ({(topology.clusters || []).filter((c) => c.dc_id === dc.id).length}):
                    </span>
                  </div>
                  <div class="space-y-2">
                    {#each (topology.clusters || []).filter((c) => c.dc_id === dc.id) as cluster}
                      <div class="flex items-center justify-between p-2.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/60 dark:border-slate-800 text-xs">
                        <div class="flex items-center gap-2 font-medium">
                          <HardDrive class="w-4 h-4 text-sky-500 shrink-0" />
                          <div class="flex flex-col">
                            <span class="font-semibold text-slate-800 dark:text-slate-200">{cluster.name}</span>
                            <span class="text-[10px] text-slate-400 font-mono">{cluster.default_path || '/'}</span>
                          </div>
                        </div>
                        <span class="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-200/60 dark:bg-slate-800 text-slate-600 dark:text-slate-300 shrink-0">
                          {cluster.id}
                        </span>
                      </div>
                    {/each}
                  </div>
                </div>
              </div>
            {/each}
          {/if}
        </div>

        <!-- 2. Глобальный лимит WAN (Token Bucket Throttler) -->
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
          <div class="flex items-center gap-2 mb-3">
            <Gauge class="w-5 h-5 text-sky-600" />
            <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">Глобальный лимит сетевого канала (Global WAN Throttler)</h3>
          </div>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">
            Максимальная совокупная скорость передачи данных по всей платформе репликации между всеми воркерами.
          </p>

          <div class="flex flex-wrap items-center gap-4 bg-slate-50 dark:bg-slate-950/60 p-4 rounded-xl border border-slate-200/80 dark:border-slate-800">
            <div class="flex items-center gap-2">
              <label class="text-xs font-semibold text-slate-700 dark:text-slate-300">Лимит полосы:</label>
              <input
                type="number"
                min="0"
                step="5"
                disabled={!isAdmin || globalUnlimited}
                bind:value={globalLimitMb}
                class="w-24 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg text-xs font-mono font-bold focus:outline-none focus:ring-1 focus:ring-sky-500 disabled:opacity-50"
              />
              <span class="text-xs text-slate-500 font-medium">МБ/с</span>
            </div>

            <label class="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer select-none">
              <input
                type="checkbox"
                disabled={!isAdmin}
                bind:checked={globalUnlimited}
                class="rounded border-slate-300 text-sky-600 focus:ring-sky-500 cursor-pointer disabled:opacity-50"
              />
              <span>Без лимита (Unlimited / 0 МБ/с)</span>
            </label>

            {#if isAdmin}
              <button
                onclick={saveGlobalLimit}
                class="px-4 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-xs transition cursor-pointer"
              >
                Сохранить
              </button>
            {/if}
          </div>
        </div>

        <!-- 3. Матрица лимитов между ЦОД (DC-DC WAN) -->
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
          <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100 mb-1">Ограничения между ЦОД (DC-DC WAN Channels)</h3>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">Индивидуальные ограничения магистральных каналов связи между дата-центрами.</p>

          {#if Object.keys(dcLimits).length === 0}
            <div class="py-6 text-center text-xs text-slate-400 dark:text-slate-500">
              Каналы между ЦОД не обнаружены в топологии
            </div>
          {:else}
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
              {#each Object.keys(dcLimits) as key}
                <div class="p-3.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 flex flex-col justify-between gap-3 shadow-2xs">
                  <div class="flex items-center justify-between">
                    <span class="font-mono font-bold text-xs text-slate-800 dark:text-slate-200 uppercase">{formatDcLabel(key)}</span>
                    <span class="text-[10px] text-slate-400">{formatDcSub(key)}</span>
                  </div>
                  <div class="flex items-center gap-2">
                    <input
                      type="number"
                      min="0"
                      step="5"
                      disabled={!isAdmin || dcLimits[key].unlimited}
                      bind:value={dcLimits[key].mb}
                      class="w-20 px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold disabled:opacity-50"
                    />
                    <span class="text-xs text-slate-500">МБ/с</span>
                    <label class="flex items-center gap-1.5 text-[11px] text-slate-600 dark:text-slate-300 ml-auto cursor-pointer select-none">
                      <input
                        type="checkbox"
                        disabled={!isAdmin}
                        bind:checked={dcLimits[key].unlimited}
                        class="rounded border-slate-300 text-sky-600 focus:ring-sky-500 cursor-pointer disabled:opacity-50"
                      />
                      <span>Без лимита</span>
                    </label>
                    {#if isAdmin}
                      <button
                        onclick={() => saveDcLimit(key)}
                        class="px-2.5 py-1 rounded-md bg-sky-600 hover:bg-sky-500 text-white text-[11px] font-semibold transition cursor-pointer shadow-2xs shrink-0"
                        title="Сохранить лимит канала"
                      >
                        Сохранить
                      </button>
                    {/if}
                  </div>
                </div>
              {/each}
            </div>
          {/if}
        </div>

        <!-- 4. Ограничения каналов между кластерами HDFS-HDFS -->
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
          <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100 mb-1">Лимиты каналов HDFS-HDFS</h3>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">Тонкая настройка полосы репликации между конкретными парами NameNode.</p>

          {#if Object.keys(hdfsLimits).length === 0}
            <div class="py-6 text-center text-xs text-slate-400 dark:text-slate-500">
              Каналы между кластерами HDFS не обнаружены в топологии
            </div>
          {:else}
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
              {#each Object.keys(hdfsLimits) as key}
                <div class="p-3.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 flex flex-col justify-between gap-3 shadow-2xs">
                  <div class="flex items-center justify-between">
                    <span class="font-mono font-bold text-xs text-slate-800 dark:text-slate-200 truncate pr-2" title={formatHdfsLabel(key)}>
                      {formatHdfsLabel(key)}
                    </span>
                    <span class="text-[10px] font-mono text-slate-400 shrink-0">{key}</span>
                  </div>
                  <div class="flex items-center gap-2">
                    <input
                      type="number"
                      min="0"
                      step="5"
                      disabled={!isAdmin || hdfsLimits[key].unlimited}
                      bind:value={hdfsLimits[key].mb}
                      class="w-20 px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold disabled:opacity-50"
                    />
                    <span class="text-xs text-slate-500">МБ/с</span>
                    <label class="flex items-center gap-1.5 text-[11px] text-slate-600 dark:text-slate-300 ml-auto cursor-pointer select-none">
                      <input
                        type="checkbox"
                        disabled={!isAdmin}
                        bind:checked={hdfsLimits[key].unlimited}
                        class="rounded border-slate-300 text-sky-600 focus:ring-sky-500 cursor-pointer disabled:opacity-50"
                      />
                      <span>Без лимита</span>
                    </label>
                    {#if isAdmin}
                      <button
                        onclick={() => saveHdfsLimit(key)}
                        class="px-2.5 py-1 rounded-md bg-sky-600 hover:bg-sky-500 text-white text-[11px] font-semibold transition cursor-pointer shadow-2xs shrink-0"
                        title="Сохранить лимит канала"
                      >
                        Сохранить
                      </button>
                    {/if}
                  </div>
                </div>
              {/each}
            </div>
          {/if}
        </div>

        <!-- 5. Динамически зарегистрированные агенты (Keepalive Registry) -->
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
          <div class="flex items-center justify-between mb-2">
            <div class="flex items-center gap-2">
              <Server class="w-5 h-5 text-sky-600" />
              <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">
                Зарегистрированные агенты репликации (Keepalive Registry)
              </h3>
            </div>
            <div class="flex items-center gap-2">
              <span class="text-xs text-slate-500 font-medium">
                Онлайн: {registeredAgents.filter((a) => a.status === 'online').length} из {registeredAgents.length}
              </span>
              <button
                onclick={loadAgents}
                class="p-1 rounded-lg border border-slate-200 dark:border-slate-800 hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-500 cursor-pointer shadow-2xs"
                title="Обновить список агентов"
              >
                <RefreshCw class="w-3.5 h-3.5 {agentsLoading ? 'animate-spin' : ''}" />
              </button>
            </div>
          </div>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">
            Агенты динамически регистрируются на Оркестраторе при старте, передают свой внешний gRPC адрес и подтверждают жизнеспособность через keepalive-пинги (таймаут 15 сек).
          </p>

          <!-- Баннер отказоустойчивости Inotify Streamers (HA) -->
          {#if topology?.streaming_enabled || onlineStreamers.length > 0}
            {#if onlineStreamers.length < 2}
              <div class="mb-4 p-3 bg-amber-50 dark:bg-amber-950/40 border border-amber-300 dark:border-amber-800 rounded-xl flex items-start gap-3 text-xs text-amber-800 dark:text-amber-300">
                <AlertCircle class="w-4 h-4 text-amber-600 dark:text-amber-400 mt-0.5 shrink-0" />
                <div class="space-y-0.5">
                  <div class="font-bold flex items-center gap-1.5">
                    <span>Предупреждение отказоустойчивости стриминга (NO_REDUNDANCY)</span>
                    <span class="px-1.5 py-0.2 rounded text-[10px] font-mono bg-amber-200 dark:bg-amber-900/80 text-amber-900 dark:text-amber-200">
                      Онлайн: {onlineStreamers.length} из 2 требуемых
                    </span>
                  </div>
                  <p class="text-[11px] text-amber-700/90 dark:text-amber-300/80">
                    Для обеспечения отказоустойчивости потоковой репликации HDFS Inotify и автоматического failover запустите как минимум 2 экземпляра стримера (<code class="font-mono">AGENT_MODE=streamer</code>).
                  </p>
                </div>
              </div>
            {:else}
              <div class="mb-4 p-3 bg-emerald-50/70 dark:bg-emerald-950/40 border border-emerald-300 dark:border-emerald-800 rounded-xl flex items-center justify-between text-xs text-emerald-800 dark:text-emerald-300">
                <div class="flex items-center gap-2.5">
                  <Shield class="w-4 h-4 text-emerald-600 dark:text-emerald-400 shrink-0" />
                  <div>
                    <span class="font-bold">Высокая доступность Inotify Streamers (HA): Active-Standby кластер активен</span>
                    <span class="text-[11px] text-emerald-700/90 dark:text-emerald-300/80 block">
                      Онлайн: {onlineStreamers.length} стримера в кластерах ({Object.keys(streamingLeases).join(', ') || 'dc1'}) · Готовность к прямому потоку и обратной синхронизации при Failover
                    </span>
                  </div>
                </div>
                <span class="px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-100 dark:bg-emerald-900 text-emerald-800 dark:text-emerald-200 border border-emerald-300 dark:border-emerald-700">
                  HEALTHY HA
                </span>
              </div>
            {/if}
          {/if}

          {#if registeredAgents.length === 0}
            <div class="py-8 text-center text-xs text-slate-400 dark:text-slate-500 border border-dashed border-slate-200 dark:border-slate-800 rounded-xl">
              Нет активных зарегистрированных агентов. При запуске агенты `backend.replicator.agent` автоматически появятся здесь.
            </div>
          {:else}
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
              {#each registeredAgents as agent}
                <div class="p-3.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 flex flex-col justify-between gap-3 shadow-2xs">
                  <div class="flex items-center justify-between">
                    <div class="flex flex-col">
                      <span class="font-mono font-bold text-xs text-slate-800 dark:text-slate-200">
                        {agent.agent_id}
                      </span>
                      {#if agent.mode === 'streamer'}
                        {@const agentClusterLease = streamingLeases[agent.cluster_id || 'dc1'] || streamingLease}
                        <div class="flex items-center gap-1.5 mt-1">
                          <span class="inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[10px] font-bold bg-purple-50 dark:bg-purple-950/70 text-purple-700 dark:text-purple-300 border border-purple-300 dark:border-purple-800">
                            <Radio class="w-2.5 h-2.5 text-purple-600 dark:text-purple-400" />
                            <span>Inotify Streamer</span>
                          </span>
                          {#if (agentClusterLease?.active_agent_id || agentClusterLease?.active_streamer_id) === agent.agent_id}
                            <span class="px-1.5 py-0.2 rounded text-[9px] font-mono font-bold bg-emerald-100 dark:bg-emerald-950/80 text-emerald-800 dark:text-emerald-300 border border-emerald-300 dark:border-emerald-700">
                              ACTIVE (Ep {agentClusterLease?.epoch ?? 1})
                            </span>
                          {:else}
                            <span class="px-1.5 py-0.2 rounded text-[9px] font-mono font-medium bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300">
                              STANDBY
                            </span>
                          {/if}
                        </div>
                      {/if}
                    </div>
                    {#if agent.status === 'online'}
                      <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-50 dark:bg-emerald-950/60 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800">
                        <span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
                        ONLINE
                      </span>
                    {:else if agent.status === 'stale'}
                      <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-amber-50 dark:bg-amber-950/60 text-amber-700 dark:text-amber-300 border border-amber-200 dark:border-amber-800">
                        <span class="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
                        STALE ({agent.heartbeat_age_seconds}с)
                      </span>
                    {:else}
                      <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 border border-slate-200 dark:border-slate-700">
                        OFFLINE
                      </span>
                    {/if}
                  </div>

                  <div class="space-y-1 text-[11px]">
                    <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                      <span class="text-slate-400">Кластер:</span>
                      <span class="font-mono font-medium">{agent.cluster_id || 'любой'}</span>
                    </div>
                    <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                      <span class="text-slate-400">gRPC адрес:</span>
                      <span class="font-mono font-semibold text-sky-600 dark:text-sky-400">
                        {agent.grpc_address || '—'}
                      </span>
                    </div>
                    <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                      <span class="text-slate-400">Режим / Нагрузка:</span>
                      <span class="font-mono">
                        {#if agent.mode === 'streamer'}
                          <span class="text-purple-600 dark:text-purple-400 font-semibold">Streamer (изолирован от задач)</span>
                        {:else}
                          {agent.mode} (активно: {agent.active_transfers})
                        {/if}
                      </span>
                    </div>
                    <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                      <span class="text-slate-400">Лимит полосы:</span>
                      <span class="font-mono {agent.max_bandwidth_mb_s ? 'text-amber-600 dark:text-amber-400 font-semibold' : 'text-slate-400'}">
                        {agent.max_bandwidth_mb_s ? `${agent.max_bandwidth_mb_s} МБ/с` : 'без ограничений'}
                      </span>
                    </div>
                    <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                      <span class="text-slate-400">Keepalive:</span>
                      <span class="text-[10px] text-slate-400 font-mono">
                        {agent.heartbeat_age_seconds < 5 ? 'только что' : `${agent.heartbeat_age_seconds}с назад`}
                      </span>
                    </div>
                  </div>
                </div>
              {/each}
            </div>
          {/if}
        </div>
      </main>

    <!-- КОНТЕНТ ВКЛАДКИ: DISASTER RECOVERY & FAILOVER (DR CONSOLE) -->
    {:else if activeTab === 'dr'}
      <main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
        <DisasterRecoveryView {user} />
      </main>
    {/if}
  </div>

  <!-- МОДАЛЬНОЕ ОКНО СОЗДАНИЯ ЗАДАЧИ РЕПЛИКАЦИИ -->
  {#if isCreateModalOpen}
    <div
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
      onclick={(e) => { if (e.target === e.currentTarget) isCreateModalOpen = false; }}
    >
      <div
        class="w-full max-w-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[90vh] overflow-y-auto"
      >
        <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
          <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-xl bg-sky-50 dark:bg-sky-950/60 border border-sky-200 dark:border-sky-800 flex items-center justify-center text-sky-600 dark:text-sky-400">
              <Plus class="w-5 h-5" />
            </div>
            <div>
              <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">Новая задача репликации</h2>
              <p class="text-xs text-slate-500 dark:text-slate-400">Межкластерная передача файлов и каталогов по gRPC</p>
            </div>
          </div>
          <button
            onclick={() => (isCreateModalOpen = false)}
            class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded cursor-pointer"
          >
            <X class="w-5 h-5" />
          </button>
        </div>

        {#if createJobError}
          <div class="p-3 bg-rose-50 dark:bg-rose-950/50 border border-rose-200 dark:border-rose-900 rounded-xl flex items-center gap-2.5 text-xs text-rose-700 dark:text-rose-300 font-medium">
            <AlertCircle class="w-4 h-4 shrink-0" />
            <span>{createJobError}</span>
          </div>
        {/if}

        <form onsubmit={handleCreateJob} class="flex flex-col gap-4">
          <!-- Кластеры источника и назначения -->
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
                Исходный кластер (Source)
              </label>
              <select
                bind:value={newJobSourceCluster}
                class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
              >
                {#if topology && topology.clusters}
                  {#each topology.clusters as c}
                    <option value={c.id}>{c.name} [{c.dc_id.toUpperCase()}]</option>
                  {/each}
                {:else}
                  <option value="demo-cluster">HDFS Primary [DC1]</option>
                {/if}
              </select>
            </div>

            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
                Целевой кластер (Target)
              </label>
              <select
                bind:value={newJobTargetCluster}
                class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
              >
                {#if topology && topology.clusters}
                  {#each topology.clusters as c}
                    <option value={c.id}>{c.name} [{c.dc_id.toUpperCase()}]</option>
                  {/each}
                {:else}
                  <option value="backup-cluster">HDFS DR [DC2]</option>
                {/if}
              </select>
            </div>
          </div>

          <!-- Исходный путь в HDFS -->
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Исходный путь в HDFS (Source Path)
            </label>
            <input
              type="text"
              required
              bind:value={newJobSourcePath}
              placeholder="/data/production/events/2026-10"
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>

          <!-- Целевой путь в HDFS -->
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Целевой путь назначения (Target Path)
            </label>
            <input
              type="text"
              required
              bind:value={newJobTargetPath}
              placeholder="/backup/mirror/events/2026-10"
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>

          <!-- Пользователь для имперсонации (doAs) и аудит Ranger -->
          <div>
            <label for="create-impersonation-user" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Пользователь имперсонации HDFS (doAs username)
            </label>
            <div class="relative">
              {#if isAdmin}
                <input
                  id="create-impersonation-user"
                  type="text"
                  bind:value={newJobImpersonationUser}
                  placeholder={user?.username || 'hdfs'}
                  class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
                />
                <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-sky-100 dark:bg-sky-900/60 text-sky-700 dark:text-sky-300 font-semibold">
                  admin
                </span>
              {:else}
                <input
                  id="create-impersonation-user"
                  type="text"
                  value={user?.username || 'текущий пользователь'}
                  disabled
                  class="w-full px-3 py-2 bg-slate-100 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-mono text-slate-500 dark:text-slate-400 cursor-not-allowed"
                />
                <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-300 font-semibold">
                  doAs
                </span>
              {/if}
            </div>
            <div class="mt-1.5 p-2.5 bg-sky-50/60 dark:bg-sky-950/25 border border-sky-200/70 dark:border-sky-800/50 rounded-xl flex items-start gap-2">
              <Shield class="w-3.5 h-3.5 text-sky-600 dark:text-sky-400 mt-0.5 shrink-0" />
              <div class="text-[11px] text-slate-500 dark:text-slate-400 leading-relaxed">
                {#if isAdmin}
                  Администратор может указать username для выполнения HDFS репликации от его имени через Hadoop Proxy User.
                {:else}
                  Доступ к HDFS выполняется от вашего имени <strong class="font-mono text-slate-700 dark:text-slate-300">({user?.username || 'текущий пользователь'})</strong> через Kerberos Proxy User.
                {/if}
              </div>
            </div>
          </div>

          <!-- Режим синхронизации (Sync Mode) -->
          <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-3">
            <div>
              <span class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Режим синхронизации</span>
              <p class="text-[11px] text-slate-500 dark:text-slate-400">Выберите способ запуска и частоту репликации данных</p>
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-3 gap-2">
              <!-- Режим 1: Ручной -->
              <label class="flex flex-col p-2.5 rounded-xl border cursor-pointer transition select-none {newJobSyncMode === 'MANUAL'
                ? 'bg-sky-50/70 dark:bg-sky-950/50 border-sky-400 dark:border-sky-600 ring-1 ring-sky-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300'}">
                <div class="flex items-center gap-2 mb-1">
                  <input
                    type="radio"
                    name="sync_mode"
                    value="MANUAL"
                    bind:group={newJobSyncMode}
                    class="text-sky-600 focus:ring-sky-500"
                  />
                  <span class="text-xs font-bold text-slate-800 dark:text-slate-200">Ручной запуск</span>
                </div>
                <span class="text-[10px] text-slate-500">Однократная репликация по кнопке Play</span>
              </label>

              <!-- Режим 2: По расписанию (Cron) -->
              <label class="flex flex-col p-2.5 rounded-xl border cursor-pointer transition select-none {newJobSyncMode === 'SCHEDULED'
                ? 'bg-indigo-50/70 dark:bg-indigo-950/50 border-indigo-400 dark:border-indigo-600 ring-1 ring-indigo-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300'}">
                <div class="flex items-center gap-2 mb-1">
                  <input
                    type="radio"
                    name="sync_mode"
                    value="SCHEDULED"
                    bind:group={newJobSyncMode}
                    class="text-indigo-600 focus:ring-indigo-500"
                  />
                  <span class="text-xs font-bold text-slate-800 dark:text-slate-200">По расписанию</span>
                </div>
                <span class="text-[10px] text-slate-500">Cron-шедулер оркестратора</span>
              </label>

              <!-- Режим 3: Потоковый Inotify -->
              <label class="flex flex-col p-2.5 rounded-xl border transition select-none {isStreamingAvailable ? 'cursor-pointer' : 'cursor-not-allowed opacity-60'} {newJobSyncMode === 'STREAMING_INOTIFY'
                ? 'bg-purple-50/70 dark:bg-purple-950/50 border-purple-400 dark:border-purple-600 ring-1 ring-purple-500/30'
                : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300'}">
                <div class="flex items-center gap-2 mb-1">
                  <input
                    type="radio"
                    name="sync_mode"
                    value="STREAMING_INOTIFY"
                    disabled={!isStreamingAvailable}
                    bind:group={newJobSyncMode}
                    class="text-purple-600 focus:ring-purple-500 disabled:opacity-50"
                  />
                  <span class="text-xs font-bold text-slate-800 dark:text-slate-200 flex items-center gap-1">
                    <span>Live Inotify</span>
                    <span class="text-[9px] px-1 py-0.2 rounded bg-purple-100 dark:bg-purple-900/60 text-purple-700 dark:text-purple-300 font-mono">Near-0 RPO</span>
                  </span>
                </div>
                <span class="text-[10px] text-slate-500">
                  {isStreamingAvailable ? 'Потоковое чтение событий NameNode' : 'Отключено (streaming.enabled=false)'}
                </span>
              </label>
            </div>

            {#if newJobSyncMode === 'SCHEDULED'}
              <div class="space-y-1.5 pt-2 border-t border-slate-200/60 dark:border-slate-800">
                <label class="block text-[11px] font-medium text-slate-500">Интервал повторения:</label>
                <select
                  bind:value={newJobCronPreset}
                  class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono"
                >
                  <option value="@every_5m">Каждые 5 минут (@every_5m)</option>
                  <option value="@every_15m">Каждые 15 минут (@every_15m)</option>
                  <option value="@hourly">Каждый час (@hourly / 0 * * * *)</option>
                  <option value="@daily">Раз в сутки в полночь (@daily / 0 0 * * *)</option>
                </select>
              </div>
            {:else if newJobSyncMode === 'STREAMING_INOTIFY'}
              <div class="p-2.5 rounded-lg bg-purple-50/50 dark:bg-purple-950/20 border border-purple-200/60 dark:border-purple-800/40 text-[11px] text-purple-800 dark:text-purple-300 space-y-1">
                <div class="font-semibold flex items-center gap-1.5">
                  <span class="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
                  <span>Режим непрерывного потока HDFS EditLog (Near-Zero RPO)</span>
                </div>
                <p class="text-[10px] text-slate-500 dark:text-slate-400">
                  Стример перехватывает операции Create/Close/Rename в реальном времени. Временные staging-каталоги игнорируются до финального перемещения.
                </p>
              </div>
            {/if}
          </div>

          <!-- Глубина истории запусков -->
          <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-1.5">
            <div class="flex items-center justify-between">
              <label for="new-job-retention" class="text-xs font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                <History class="w-3.5 h-3.5 text-indigo-500" />
                <span>Глубина истории запусков:</span>
              </label>
              <span class="text-xs font-mono font-bold text-indigo-600 dark:text-indigo-400">
                {newJobHistoryRetention} запусков
              </span>
            </div>
            <div class="flex items-center gap-2">
              <input
                id="new-job-retention"
                type="number"
                min="1"
                max="500"
                bind:value={newJobHistoryRetention}
                class="w-32 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
              <span class="text-[11px] text-slate-500">лимит хранения (1–500, по умолчанию 20)</span>
            </div>
            <p class="text-[10px] text-slate-400">
              Старые запуски сверх указанного лимита автоматически очищаются для экономии места.
            </p>
          </div>

          <!-- Кнопка отправки -->
          <div class="flex items-center justify-end gap-3 pt-2">
            <button
              type="button"
              onclick={() => (isCreateModalOpen = false)}
              class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-800 text-xs font-semibold hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
            >
              Отмена
            </button>
            <button
              type="submit"
              disabled={isSubmittingJob}
              class="px-5 py-2 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-md shadow-sky-600/20 transition cursor-pointer disabled:opacity-50"
            >
              {isSubmittingJob ? 'Создание...' : 'Создать задачу'}
            </button>
          </div>
        </form>
      </div>
    </div>
  {/if}

  <!-- МОДАЛЬНОЕ ОКНО РЕДАКТИРОВАНИЯ ЗАДАЧИ РЕПЛИКАЦИИ -->
  {#if isEditModalOpen}
    <div
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
      onclick={(e) => { if (e.target === e.currentTarget) isEditModalOpen = false; }}
    >
      <div
        class="w-full max-w-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[90vh] overflow-y-auto"
      >
        <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
          <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-xl bg-indigo-50 dark:bg-indigo-950/60 border border-indigo-200 dark:border-indigo-800 flex items-center justify-center text-indigo-600 dark:text-indigo-400">
              <Pencil class="w-5 h-5" />
            </div>
            <div>
              <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">Редактирование задачи</h2>
              <p class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate max-w-xs" title={editingJobId || ''}>
                ID: {editingJobId?.substring(0, 8)}...
              </p>
            </div>
          </div>
          <button
            onclick={() => (isEditModalOpen = false)}
            class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded cursor-pointer"
          >
            <X class="w-5 h-5" />
          </button>
        </div>

        {#if editJobError}
          <div class="p-3 bg-rose-50 dark:bg-rose-950/50 border border-rose-200 dark:border-rose-900 rounded-xl flex items-center gap-2.5 text-xs text-rose-700 dark:text-rose-300 font-medium">
            <AlertCircle class="w-4 h-4 shrink-0" />
            <span>{editJobError}</span>
          </div>
        {/if}

        <form onsubmit={handleSaveEditJob} class="flex flex-col gap-4">
          <!-- Кластеры источника и назначения -->
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
                Исходный кластер (Source)
              </label>
              <select
                bind:value={editJobSourceCluster}
                class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
              >
                {#if topology && topology.clusters}
                  {#each topology.clusters as c}
                    <option value={c.id}>{c.name} [{c.dc_id.toUpperCase()}]</option>
                  {/each}
                {:else}
                  <option value="demo-cluster">HDFS Primary [DC1]</option>
                {/if}
              </select>
            </div>

            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
                Целевой кластер (Target)
              </label>
              <select
                bind:value={editJobTargetCluster}
                class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
              >
                {#if topology && topology.clusters}
                  {#each topology.clusters as c}
                    <option value={c.id}>{c.name} [{c.dc_id.toUpperCase()}]</option>
                  {/each}
                {:else}
                  <option value="backup-cluster">HDFS DR [DC2]</option>
                {/if}
              </select>
            </div>
          </div>

          <!-- Исходный путь в HDFS -->
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Исходный путь в HDFS (Source Path)
            </label>
            <input
              type="text"
              required
              bind:value={editJobSourcePath}
              placeholder="/data/production/events/2026-10"
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>

          <!-- Целевой путь в HDFS -->
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Целевой путь назначения (Target Path)
            </label>
            <input
              type="text"
              required
              bind:value={editJobTargetPath}
              placeholder="/backup/mirror/events/2026-10"
              class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>

          <!-- Пользователь для имперсонации (doAs) -->
          <div>
            <label for="edit-impersonation-user" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Пользователь имперсонации HDFS (doAs username)
            </label>
            <div class="relative">
              {#if isAdmin}
                <input
                  id="edit-impersonation-user"
                  type="text"
                  bind:value={editJobImpersonationUser}
                  placeholder="hdfs"
                  class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
                />
                <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-sky-100 dark:bg-sky-900/60 text-sky-700 dark:text-sky-300 font-semibold">
                  admin
                </span>
              {:else}
                <input
                  id="edit-impersonation-user"
                  type="text"
                  value={editJobImpersonationUser}
                  disabled
                  class="w-full px-3 py-2 bg-slate-100 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-mono text-slate-500 dark:text-slate-400 cursor-not-allowed"
                />
                <span class="absolute right-2.5 top-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-300 font-semibold">
                  doAs
                </span>
              {/if}
            </div>
            <p class="text-[10px] text-slate-400 mt-1">
              {#if isAdmin}
                Администратор может переопределить учетную запись Kerberos doAs имперсонации.
              {:else}
                Имперсонация зафиксирована за создателем задачи.
              {/if}
            </p>
          </div>

          <!-- Планировщик периодических задач (Cron Scheduler) -->
          <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-2.5">
            <label class="flex items-center gap-2 text-xs font-semibold text-slate-700 dark:text-slate-300 cursor-pointer select-none">
              <input
                type="checkbox"
                bind:checked={editJobIsScheduled}
                class="rounded border-slate-300 text-sky-600 focus:ring-sky-500"
              />
              <span class="flex items-center gap-1.5">
                <Calendar class="w-3.5 h-3.5 text-indigo-500" />
                Запуск по расписанию (Шедулер)
              </span>
            </label>

            {#if editJobIsScheduled}
              <div class="space-y-1.5 pt-1">
                <label class="block text-[11px] font-medium text-slate-500">Интервал повторения:</label>
                <select
                  bind:value={editJobCronPreset}
                  class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono"
                >
                  <option value="@every_5m">Каждые 5 минут (@every_5m)</option>
                  <option value="@every_15m">Каждые 15 минут (@every_15m)</option>
                  <option value="@hourly">Каждый час (@hourly / 0 * * * *)</option>
                  <option value="@daily">Раз в сутки в полночь (@daily / 0 0 * * *)</option>
                </select>
              </div>
            {/if}
          </div>

          <!-- Глубина истории запусков -->
          <div class="p-3 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-1.5">
            <div class="flex items-center justify-between">
              <label for="edit-job-retention" class="text-xs font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                <History class="w-3.5 h-3.5 text-indigo-500" />
                <span>Глубина истории запусков:</span>
              </label>
              <span class="text-xs font-mono font-bold text-indigo-600 dark:text-indigo-400">
                {editJobHistoryRetention} запусков
              </span>
            </div>
            <div class="flex items-center gap-2">
              <input
                id="edit-job-retention"
                type="number"
                min="1"
                max="500"
                bind:value={editJobHistoryRetention}
                class="w-32 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
              <span class="text-[11px] text-slate-500">лимит хранения (1–500)</span>
            </div>
            <p class="text-[10px] text-slate-400">
              При сохранении старые запуски сверх лимита будут автоматически очищены.
            </p>
          </div>

          <!-- Кнопка отправки -->
          <div class="flex items-center justify-end gap-3 pt-2">
            <button
              type="button"
              onclick={() => (isEditModalOpen = false)}
              class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-800 text-xs font-semibold hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
            >
              Отмена
            </button>
            <button
              type="submit"
              disabled={isSubmittingEditJob}
              class="px-5 py-2 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-md shadow-sky-600/20 transition cursor-pointer disabled:opacity-50"
            >
              {isSubmittingEditJob ? 'Сохранение...' : 'Сохранить изменения'}
            </button>
          </div>
        </form>
      </div>
    </div>
  {/if}

  <!-- МОДАЛЬНОЕ ОКНО ИСТОРИИ ЗАПУСКОВ ЗАДАЧИ СО СТАТИСТИКОЙ -->
  {#if isHistoryModalOpen && selectedJobForHistory}
    <div
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
      onclick={(e) => { if (e.target === e.currentTarget) isHistoryModalOpen = false; }}
    >
      <div
        class="w-full max-w-4xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 sm:p-7 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[92vh] overflow-hidden"
      >
        <!-- Шапка окна -->
        <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
          <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-xl bg-indigo-50 dark:bg-indigo-950/60 border border-indigo-200 dark:border-indigo-800 flex items-center justify-center text-indigo-600 dark:text-indigo-400">
              <History class="w-5 h-5" />
            </div>
            <div>
              <div class="flex items-center gap-2">
                <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">История запусков задачи</h2>
                <StatusBadge status={selectedJobForHistory.status} />
              </div>
              <p class="text-xs text-slate-500 dark:text-slate-400 font-mono flex items-center gap-2 mt-0.5">
                <span>ID: {selectedJobForHistory.id.substring(0, 13)}...</span>
                <span>•</span>
                <span class="text-slate-700 dark:text-slate-300 font-medium">
                  {selectedJobForHistory.source_cluster_id} → {selectedJobForHistory.target_cluster_id}
                </span>
                {#if selectedJobForHistory.is_scheduled}
                  <span>•</span>
                  <span class="inline-flex items-center gap-1 text-indigo-600 dark:text-indigo-400">
                    <Calendar class="w-3 h-3" />
                    {selectedJobForHistory.cron_expression}
                  </span>
                {/if}
              </p>
            </div>
          </div>
          <div class="flex items-center gap-2">
            <button
              onclick={() => loadJobRuns(selectedJobForHistory!.id)}
              class="p-2 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-300 transition cursor-pointer shadow-2xs"
              title="Обновить историю"
            >
              <RefreshCw class="w-4 h-4 {runsLoading ? 'animate-spin' : ''}" />
            </button>
            <button
              onclick={() => (isHistoryModalOpen = false)}
              class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded cursor-pointer"
            >
              <X class="w-5 h-5" />
            </button>
          </div>
        </div>

        <!-- Панель управления глубиной хранения истории (Retention) -->
        <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <div class="space-y-0.5">
            <div class="text-xs font-semibold text-slate-800 dark:text-slate-200 flex items-center gap-1.5">
              <span>Настройка глубины истории:</span>
              <span class="font-mono text-indigo-600 dark:text-indigo-400">
                {selectedJobForHistory.history_retention_runs ?? 20} запусков
              </span>
            </div>
            <p class="text-[11px] text-slate-500">
              Старые запуски автоматически удаляются при превышении лимита (1–500).
            </p>
          </div>

          <div class="flex items-center gap-2 self-start sm:self-auto">
            <input
              type="number"
              min="1"
              max="500"
              bind:value={runsRetentionInput}
              class="w-24 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-750 rounded-lg text-xs font-mono focus:outline-none focus:ring-1 focus:ring-indigo-500"
            />
            <button
              onclick={() => handleSaveRetention(selectedJobForHistory!.id)}
              disabled={savingRetention}
              class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold transition cursor-pointer disabled:opacity-50 shadow-2xs"
            >
              {#if savingRetention}
                <span>Сохранение...</span>
              {:else if retentionSaveSuccess}
                <Check class="w-3.5 h-3.5" />
                <span>Сохранено!</span>
              {:else}
                <span>Применить лимит</span>
              {/if}
            </button>
          </div>
        </div>

        <!-- Сводная статистика по истории запусков -->
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
            <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Запусков в истории</span>
            <span class="text-lg font-bold text-slate-900 dark:text-slate-100">{jobRuns.length}</span>
          </div>
          <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
            <span class="text-[11px] font-medium text-emerald-600 dark:text-emerald-400 block">Успешных</span>
            <span class="text-lg font-bold text-emerald-600 dark:text-emerald-400">
              {jobRuns.filter((r) => r.status === 'COMPLETED').length}
            </span>
          </div>
          <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
            <span class="text-[11px] font-medium text-rose-600 dark:text-rose-400 block">С ошибками</span>
            <span class="text-lg font-bold text-rose-600 dark:text-rose-400">
              {jobRuns.filter((r) => r.status === 'FAILED').length}
            </span>
          </div>
          <div class="p-3 bg-white dark:bg-slate-850 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs">
            <span class="text-[11px] font-medium text-sky-600 dark:text-sky-400 block">Всего передано</span>
            <span class="text-lg font-bold text-sky-600 dark:text-sky-400 font-mono">
              {formatBytes(jobRuns.reduce((acc, r) => acc + (r.copied_bytes || 0), 0))}
            </span>
          </div>
        </div>

        <!-- Таблица истории запусков -->
        <div class="flex-1 overflow-y-auto border border-slate-200 dark:border-slate-800 rounded-xl bg-white dark:bg-slate-950/60 shadow-inner">
          <table class="w-full text-left border-collapse text-xs min-w-[700px]">
            <thead class="sticky top-0 bg-slate-50 dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 text-[10px] uppercase font-semibold text-slate-500 tracking-wider">
              <tr>
                <th scope="col" class="py-2.5 px-3 w-16">№</th>
                <th scope="col" class="py-2.5 px-3 w-32">Триггер</th>
                <th scope="col" class="py-2.5 px-3 w-28">Статус</th>
                <th scope="col" class="py-2.5 px-3 w-40">Старт / Финиш</th>
                <th scope="col" class="py-2.5 px-3 w-28">Длительность</th>
                <th scope="col" class="py-2.5 px-3 w-36">Объем</th>
                <th scope="col" class="py-2.5 px-3 w-28">Ср. скорость</th>
                <th scope="col" class="py-2.5 px-3">Детали / Ошибка</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-100 dark:divide-slate-850">
              {#if runsLoading}
                <tr>
                  <td colspan="8" class="py-12 text-center text-slate-400">
                    <div class="flex flex-col items-center gap-2">
                      <div class="w-6 h-6 border-2 border-indigo-600 border-t-transparent rounded-full animate-spin"></div>
                      <span>Загрузка истории запусков...</span>
                    </div>
                  </td>
                </tr>
              {:else if jobRuns.length === 0}
                <tr>
                  <td colspan="8" class="py-12 text-center text-slate-400">
                    <div class="flex flex-col items-center gap-2">
                      <History class="w-8 h-8 stroke-1 text-slate-300 dark:text-slate-700" />
                      <span class="font-medium text-slate-700 dark:text-slate-300">История запусков пуста</span>
                      <span class="text-[11px] text-slate-400">Запустите задачу вручную или дождитесь срабатывания шедулера</span>
                    </div>
                  </td>
                </tr>
              {:else}
                {#each jobRuns as run (run.id)}
                  <tr class="hover:bg-slate-50/70 dark:hover:bg-slate-850/50 transition-colors">
                    <!-- Номер запуска -->
                    <td class="py-2.5 px-3 font-mono font-bold text-slate-700 dark:text-slate-300">
                      #{run.run_number}
                    </td>

                    <!-- Триггер -->
                    <td class="py-2.5 px-3">
                      {#if run.trigger_type === 'SCHEDULED'}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-indigo-50 dark:bg-indigo-950/50 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800">
                          <Calendar class="w-3 h-3" />
                          Шедулер
                        </span>
                      {:else}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700">
                          <Play class="w-2.5 h-2.5" />
                          Вручную
                        </span>
                      {/if}
                    </td>

                    <!-- Статус -->
                    <td class="py-2.5 px-3">
                      <StatusBadge status={run.status} />
                    </td>

                    <!-- Тайминги -->
                    <td class="py-2.5 px-3 font-mono text-[11px] text-slate-600 dark:text-slate-300">
                      <div>{formatTime(run.started_at || run.created_at)}</div>
                      {#if run.completed_at}
                        <div class="text-[10px] text-slate-400">до {formatTime(run.completed_at)}</div>
                      {/if}
                    </td>

                    <!-- Длительность -->
                    <td class="py-2.5 px-3 font-mono text-slate-700 dark:text-slate-300">
                      {formatDuration(run.duration_seconds)}
                    </td>

                    <!-- Объем переданных данных -->
                    <td class="py-2.5 px-3 font-mono text-[11px] text-slate-700 dark:text-slate-300">
                      <div>{formatBytes(run.copied_bytes)}</div>
                      <div class="text-[10px] text-slate-400">из {formatBytes(run.total_bytes)}</div>
                      {#if (run.total_objects ?? 0) > 0}
                        <div class="text-[9px] text-teal-600 dark:text-teal-400 font-medium">
                          {(run.transferred_objects ?? 0) + (run.skipped_objects ?? 0)}/{run.total_objects} объектов
                        </div>
                      {/if}
                    </td>

                    <!-- Средняя скорость -->
                    <td class="py-2.5 px-3 font-mono text-slate-700 dark:text-slate-300">
                      {#if run.average_speed_mb_s !== undefined && run.average_speed_mb_s > 0}
                        <span class="font-semibold text-sky-600 dark:text-sky-400">
                          {run.average_speed_mb_s.toFixed(1)} МБ/с
                        </span>
                      {:else}
                        <span class="text-slate-400">—</span>
                      {/if}
                    </td>

                    <!-- Детали и ошибки -->
                    <td class="py-2.5 px-3 max-w-xs truncate">
                      {#if run.error_message}
                        <span class="text-rose-600 dark:text-rose-400 font-medium truncate block" title={run.error_message}>
                          {run.error_message}
                        </span>
                      {:else if run.message}
                        <span class="text-slate-500 truncate block" title={run.message}>
                          {run.message}
                        </span>
                      {:else if run.triggered_by}
                        <span class="text-slate-400 text-[10px]">автор: {run.triggered_by}</span>
                      {:else}
                        <span class="text-slate-400 text-[10px]">ОК</span>
                      {/if}
                    </td>
                  </tr>
                {/each}
              {/if}
            </tbody>
          </table>
        </div>

        <!-- Кнопка закрытия -->
        <div class="flex items-center justify-between pt-2 border-t border-slate-100 dark:border-slate-800">
          <span class="text-[11px] text-slate-400">
            Отображаются последние {jobRuns.length} запусков (лимит хранения: {selectedJobForHistory.history_retention_runs ?? 20})
          </span>
          <button
            type="button"
            onclick={() => (isHistoryModalOpen = false)}
            class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-800 text-xs font-semibold hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
          >
            Закрыть
          </button>
        </div>
      </div>
    </div>
  {/if}
{/if}
