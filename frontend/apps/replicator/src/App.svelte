<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import type { UserSession } from '@hadoop-explorer/common';
  import { Header, LoginModal } from '@hadoop-explorer/common';
  import { api } from './api/client';
  import type {
    Job,
    JobRun,
    TopologyData,
    AgentInfo,
    StreamingLeaseStatus,
  } from './types';
  import { ArrowLeftRight } from 'lucide-svelte';
  import HmsReplicationView from './components/HmsReplicationView.svelte';
  import DisasterRecoveryView from './components/DisasterRecoveryView.svelte';
  import KafkaReplicationView from './components/KafkaReplicationView.svelte';
  import JobsTableView from './components/JobsTableView.svelte';
  import TopologyView from './components/TopologyView.svelte';
  import CreateJobModal from './components/CreateJobModal.svelte';
  import EditJobModal from './components/EditJobModal.svelte';
  import JobHistoryModal from './components/JobHistoryModal.svelte';

  // Состояние аутентификации
  let authLoading = $state(true);
  let user = $state<UserSession | null>(null);
  let authErrorMessage = $state<string | null>(null);
  let isLoginModalOpen = $state(false);

  function getInitialTab(): 'hms' | 'jobs' | 'topology' | 'dr' | 'kafka' {
    if (typeof window !== 'undefined') {
      const hash = window.location.hash.replace('#', '');
      if (hash === 'hms' || hash === 'jobs' || hash === 'topology' || hash === 'dr' || hash === 'kafka') {
        return hash;
      }
      const saved = localStorage.getItem('replicator_active_tab');
      if (saved === 'hms' || saved === 'jobs' || saved === 'topology' || saved === 'dr' || saved === 'kafka') {
        return saved as 'hms' | 'jobs' | 'topology' | 'dr' | 'kafka';
      }
    }
    return 'hms';
  }

  // Табы приложения с сохранением состояния при перезагрузке
  let activeTab = $state<'hms' | 'jobs' | 'topology' | 'dr' | 'kafka'>(getInitialTab());

  function setActiveTab(tab: 'hms' | 'jobs' | 'topology' | 'dr' | 'kafka') {
    activeTab = tab;
    if (typeof window !== 'undefined') {
      localStorage.setItem('replicator_active_tab', tab);
      window.location.hash = tab;
    }
  }

  let jobs = $state<Job[]>([]);
  let jobsLoading = $state(false);
  let pollTimer: any = null;

  let registeredAgents = $state<AgentInfo[]>([]);
  let agentsLoading = $state(false);

  let streamingLease = $state<StreamingLeaseStatus | null>(null);
  let streamingLeases = $state<Record<string, StreamingLeaseStatus>>({});
  let streamingLeaseLoading = $state(false);

  let topology = $state<TopologyData | null>(null);
  let topologyLoading = $state(false);

  const isStreamingAvailable = $derived<boolean>(Boolean(topology?.streaming_enabled));

  // Модалка создания задачи
  let isCreateModalOpen = $state(false);

  // Модалка редактирования задачи
  let isEditModalOpen = $state(false);
  let selectedJobForEdit = $state<Job | null>(null);

  // Модалка истории запусков задачи
  let isHistoryModalOpen = $state(false);
  let selectedJobForHistory = $state<Job | null>(null);
  let jobRuns = $state<JobRun[]>([]);
  let runsLoading = $state(false);

  const isAdmin = $derived<boolean>(
    Boolean(user && (user.system_role === 'admin' || user.is_admin))
  );
  const isReader = $derived<boolean>(
    Boolean(user && user.system_role === 'reader' && !user.is_admin)
  );
  const canCreate = $derived<boolean>(
    Boolean(user && (user.system_role !== 'reader' || user.is_admin))
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
    const isEngineer = curUser === 'writer_user' || curUser === 'de_user' || curUser.includes('engineer') || curUser.includes('writer');
    if (isEngineer && (author.includes('writer') || author.includes('engineer') || author.includes('de_'))) {
      return true;
    }
    return false;
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
      topology = await api.getTopology();
    } catch (e) {
      console.error('Ошибка загрузки топологии:', e);
    } finally {
      topologyLoading = false;
    }
  }

  // Загрузка зарегистрированных агентов
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

  // Загрузка статуса стриминговой аренды по всем кластерам
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

  // Создание задачи
  async function handleCreateJob(payload: any) {
    await api.createJob(payload);
    isCreateModalOpen = false;
    await loadJobs();
  }

  // Запуск задачи
  async function handleStartJob(id: string) {
    try {
      await api.startJob(id);
      await loadJobs();
    } catch (e: any) {
      alert(e.message || 'Не удалось запустить задачу');
    }
  }

  // Остановка задачи
  async function handleStopJob(id: string) {
    if (!confirm(`Остановить задачу ${id.substring(0, 8)}...?`)) return;
    try {
      await api.stopJob(id);
      await loadJobs();
    } catch (e: any) {
      alert(e.message || 'Не удалось остановить задачу');
    }
  }

  // Открытие модалки редактирования
  function openEditModal(job: Job) {
    selectedJobForEdit = job;
    isEditModalOpen = true;
  }

  // Сохранение изменений задачи
  async function handleSaveEditJob(jobId: string, payload: any) {
    await api.updateJob(jobId, payload);
    isEditModalOpen = false;
    await loadJobs();
  }

  // Удаление задачи
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

  async function handleSaveRetention(jobId: string, retention: number) {
    await api.updateJobRetention(jobId, retention);
    if (selectedJobForHistory) {
      selectedJobForHistory.history_retention_runs = retention;
    }
    await loadJobs();
    await loadJobRuns(jobId);
  }

  // Сохранение лимитов полосы пропускания
  async function saveGlobalLimit(limitBytesPerSec: number) {
    await api.saveGlobalLimit(limitBytesPerSec);
    await loadTopology();
  }

  async function saveDcLimit(srcDc: string, dstDc: string, mbPerSec: number) {
    await api.saveDcLimit(srcDc, dstDc, mbPerSec);
    await loadTopology();
  }

  async function saveHdfsLimit(srcCluster: string, dstCluster: string, mbPerSec: number) {
    await api.saveHdfsLimit(srcCluster, dstCluster, mbPerSec);
    await loadTopology();
  }

  let onHashChangeHandler: (() => void) | null = null;

  onMount(async () => {
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

    const onHashChange = () => {
      const hash = window.location.hash.replace('#', '');
      if (hash === 'jobs' || hash === 'hms' || hash === 'topology' || hash === 'dr' || hash === 'kafka') {
        activeTab = hash;
      }
    };
    onHashChangeHandler = onHashChange;
    window.addEventListener('hashchange', onHashChange);

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
    if (onHashChangeHandler) {
      window.removeEventListener('hashchange', onHashChangeHandler);
    }
  });
</script>

{#snippet headerCenterNav()}
  <nav class="flex items-center gap-1 bg-slate-100 dark:bg-slate-950/80 p-1 rounded-lg border border-slate-200 dark:border-slate-800">
    <button
      onclick={() => setActiveTab('hms')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer {activeTab === 'hms' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      HMS Replication
    </button>
    <button
      onclick={() => setActiveTab('jobs')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer {activeTab === 'jobs' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      HDFS Replication
    </button>
    <button
      onclick={() => setActiveTab('kafka')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer {activeTab === 'kafka' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      Kafka Replication
    </button>
    <button
      onclick={() => setActiveTab('topology')}
      class="px-2.5 sm:px-3 py-1 rounded text-xs font-semibold transition shadow-2xs cursor-pointer {activeTab === 'topology' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100'}"
    >
      Топология ЦОД и Полоса
    </button>
    <button
      onclick={() => setActiveTab('dr')}
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
  <div class="min-h-screen flex flex-col bg-slate-50 dark:bg-slate-950 text-slate-900 dark:text-slate-100 antialiased">
    <Header
      title="Hadoop gRPC Replicator"
      subtitle="WAN Replication"
      icon={ArrowLeftRight}
      {user}
      centerContent={headerCenterNav}
      onLogout={handleLogout}
      onLoginClick={() => (isLoginModalOpen = true)}
    />

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

    <!-- ВКЛАДКА 1: HMS REPLICATION -->
    {#if activeTab === 'hms'}
      <main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
        <HmsReplicationView {topology} {user} />
      </main>

    <!-- ВКЛАДКА 2: HDFS ЗАДАЧИ -->
    {:else if activeTab === 'jobs'}
      <JobsTableView
        {jobs}
        canCreate={Boolean(canCreate)}
        {canManageJob}
        onRefresh={loadJobs}
        onCreateJob={() => (isCreateModalOpen = true)}
        onStartJob={handleStartJob}
        onStopJob={handleStopJob}
        onEditJob={openEditModal}
        onDeleteJob={handleDeleteJob}
        onOpenHistory={openHistoryModal}
      />

    <!-- ВКЛАДКА 3: ТОПОЛОГИЯ ЦОД И ПОЛОСА -->
    {:else if activeTab === 'topology'}
      <TopologyView
        {topology}
        {topologyLoading}
        {registeredAgents}
        {agentsLoading}
        {streamingLease}
        {streamingLeases}
        isAdmin={Boolean(isAdmin)}
        onRefreshTopology={loadTopology}
        onRefreshAgents={loadAgents}
        onSaveGlobalLimit={saveGlobalLimit}
        onSaveDcLimit={saveDcLimit}
        onSaveHdfsLimit={saveHdfsLimit}
      />

    <!-- ВКЛАДКА 4: DISASTER RECOVERY -->
    {:else if activeTab === 'dr'}
      <main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
        <DisasterRecoveryView {user} />
      </main>

    <!-- ВКЛАДКА 5: KAFKA REPLICATION -->
    {:else if activeTab === 'kafka'}
      <main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
        <KafkaReplicationView />
      </main>
    {/if}
  </div>

  <!-- МОДАЛКА СОЗДАНИЯ ЗАДАЧИ -->
  <CreateJobModal
    isOpen={isCreateModalOpen}
    {topology}
    {user}
    isAdmin={Boolean(isAdmin)}
    isStreamingAvailable={Boolean(isStreamingAvailable)}
    onClose={() => (isCreateModalOpen = false)}
    onCreate={handleCreateJob}
  />

  <!-- МОДАЛКА РЕДАКТИРОВАНИЯ ЗАДАЧИ -->
  <EditJobModal
    isOpen={isEditModalOpen}
    job={selectedJobForEdit}
    {topology}
    isAdmin={Boolean(isAdmin)}
    onClose={() => (isEditModalOpen = false)}
    onSave={handleSaveEditJob}
  />

  <!-- МОДАЛКА ИСТОРИИ ЗАПУСКОВ ЗАДАЧИ -->
  <JobHistoryModal
    isOpen={isHistoryModalOpen}
    job={selectedJobForHistory}
    {jobRuns}
    {runsLoading}
    onClose={() => (isHistoryModalOpen = false)}
    onRefreshRuns={loadJobRuns}
    onSaveRetention={handleSaveRetention}
  />
{/if}
