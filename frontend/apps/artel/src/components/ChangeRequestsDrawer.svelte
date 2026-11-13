<script lang="ts">
  import type { ChangeRequestSummary, ChangeRequestResponse, DraftQueueItem } from '../types';
  import { api } from '../api/client';
  import { 
    X, CheckCircle, XCircle, Clock, Ban, User, Calendar, 
    FileCode, RefreshCw, GitPullRequest, ArrowRight, Eye, Check, Trash2, ArrowUpRight,
    Rocket, Terminal
  } from 'lucide-svelte';
  import { formatMemory, formatVcores, formatMemoryDelta, formatVcoresDelta } from '../utils/resourceUtils';

  let {
    clusterId,
    canAdmin,
    currentUsername,
    isOpen = $bindable(),
    onApplyToDraft,
    onViewXml,
    onStatusChange,
  }: {
    clusterId: string;
    canAdmin: boolean;
    currentUsername: string;
    isOpen: boolean;
    onApplyToDraft: (changes: DraftQueueItem[], targetQueuePath?: string) => void;
    onViewXml: (xml: string, title: string) => void;
    onStatusChange?: () => void;
  } = $props();

  let requests = $state<ChangeRequestSummary[]>([]);
  let selectedId = $state<number | null>(null);
  let selectedDetail = $state<ChangeRequestResponse | null>(null);
  let filterStatus = $state<string>('ALL');
  let isLoading = $state(false);
  let isDetailLoading = $state(false);
  let reviewComment = $state('');
  let actionLoading = $state(false);
  let isDeploying = $state(false);
  let deployStdout = $state<string | null>(null);
  let showLogs = $state(false);
  let deploySuccessMessage = $state('');
  let errorMessage = $state('');

  const displayedRequests = $derived(
    requests.filter((r) => {
      if (filterStatus === 'MY') return r.author === currentUsername;
      if (filterStatus === 'ALL') return true;
      return r.status === filterStatus;
    })
  );

  // Загрузка списка заявок при открытии
  $effect(() => {
    if (isOpen) {
      loadRequests();
    } else {
      selectedId = null;
      selectedDetail = null;
      reviewComment = '';
      errorMessage = '';
    }
  });

  async function loadRequests() {
    isLoading = true;
    errorMessage = '';
    try {
      const statusParam = (filterStatus === 'ALL' || filterStatus === 'MY') ? undefined : filterStatus;
      requests = await api.listChangeRequests(clusterId, statusParam);
      if (selectedId) {
        await selectRequest(selectedId);
      } else if (requests.length > 0) {
        await selectRequest(requests[0].id);
      }
      onStatusChange?.();
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка загрузки заявок';
    } finally {
      isLoading = false;
    }
  }

  async function selectRequest(id: number) {
    selectedId = id;
    isDetailLoading = true;
    errorMessage = '';
    reviewComment = '';
    try {
      selectedDetail = await api.getChangeRequest(id);
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка загрузки деталей заявки';
    } finally {
      isDetailLoading = false;
    }
  }

  async function handleApprove() {
    if (!selectedId) return;
    actionLoading = true;
    errorMessage = '';
    try {
      const updated = await api.approveChangeRequest(selectedId, reviewComment);
      selectedDetail = updated;
      await loadRequests();
      onStatusChange?.();
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка при одобрении заявки';
    } finally {
      actionLoading = false;
    }
  }

  async function handleReject() {
    if (!selectedId) return;
    actionLoading = true;
    errorMessage = '';
    try {
      const updated = await api.rejectChangeRequest(selectedId, reviewComment);
      selectedDetail = updated;
      await loadRequests();
      onStatusChange?.();
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка при отклонении заявки';
    } finally {
      actionLoading = false;
    }
  }

  async function handleCancel() {
    if (!selectedId) return;
    actionLoading = true;
    errorMessage = '';
    try {
      const updated = await api.cancelChangeRequest(selectedId);
      selectedDetail = updated;
      await loadRequests();
      onStatusChange?.();
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка при отзыве заявки';
    } finally {
      actionLoading = false;
    }
  }

  async function handlePreviewXml() {
    if (!selectedId) return;
    actionLoading = true;
    errorMessage = '';
    try {
      const resp = await api.previewChangeRequestXml(selectedId);
      onViewXml(resp.xml_content, resp.title);
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка предпросмотра XML';
    } finally {
      actionLoading = false;
    }
  }

  async function handleDeploy() {
    if (!selectedId) return;
    isDeploying = true;
    errorMessage = '';
    deploySuccessMessage = '';
    try {
      const resp = await api.deployChangeRequest(selectedId, true);
      if (selectedDetail) {
        selectedDetail.deployment_status = resp.status;
        selectedDetail.awx_job_id = resp.awx_job_id;
        selectedDetail.deployed_at = resp.deployed_at;
        if (resp.stdout) {
          deployStdout = resp.stdout;
          showLogs = true;
        }
      }
      deploySuccessMessage = resp.message || 'Конфигурация успешно применена на кластере через AWX';
      await loadRequests();
      onStatusChange?.();
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка при развертывании через AWX';
    } finally {
      isDeploying = false;
    }
  }

  async function handleFetchLogs() {
    if (!selectedId) return;
    try {
      const resp = await api.getDeployStatus(selectedId);
      deployStdout = resp.stdout || 'Лог AWX пуст или формируется...';
      showLogs = true;
    } catch (err: any) {
      errorMessage = err.message || 'Ошибка получения логов AWX';
    }
  }

  function handleLoadDraft(specificPath?: string) {
    if (!selectedDetail) return;
    const path = specificPath || (selectedDetail.changes.length > 0 ? selectedDetail.changes[0].path : undefined);
    onApplyToDraft(selectedDetail.changes, path);
    isOpen = false;
  }

  function formatDate(iso: string) {
    try {
      const d = new Date(iso);
      return d.toLocaleString('ru-RU', {
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      });
    } catch {
      return iso;
    }
  }

  function statusBadge(status: string) {
    switch (status) {
      case 'SUBMITTED':
        return { text: 'На рассмотрении', bg: 'bg-amber-100 text-amber-800 border-amber-200', icon: Clock };
      case 'APPROVED':
        return { text: 'Одобрено', bg: 'bg-emerald-100 text-emerald-800 border-emerald-200', icon: CheckCircle };
      case 'REJECTED':
        return { text: 'Отклонено', bg: 'bg-red-100 text-red-800 border-red-200', icon: XCircle };
      case 'CANCELLED':
        return { text: 'Отозвано', bg: 'bg-slate-100 text-slate-700 border-slate-200', icon: Ban };
      default:
        return { text: status, bg: 'bg-slate-100 text-slate-700 border-slate-200', icon: Clock };
    }
  }

</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isOpen) isOpen = false; }} />

{#if isOpen}
  <!-- Backdrop -->
  <div
    class="fixed inset-0 bg-black/40 z-40 backdrop-blur-xs"
    onclick={() => isOpen = false}
    role="button"
    tabindex="-1"
    onkeydown={() => {}}
  ></div>

  <!-- Main Drawer Panel -->
  <div class="fixed right-0 top-0 h-full w-[900px] max-w-[95vw] bg-white dark:bg-slate-900 border-l border-slate-200 dark:border-slate-800 shadow-2xl z-50 flex flex-col">
    <!-- Header -->
    <div class="flex items-center justify-between px-6 py-4 border-b border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/50">
      <div class="flex items-center gap-2.5">
        <div class="w-8 h-8 rounded-lg bg-indigo-100 dark:bg-indigo-950/60 flex items-center justify-center text-indigo-600 dark:text-indigo-400">
          <GitPullRequest class="w-4 h-4" />
        </div>
        <div>
          <h2 class="text-sm font-bold text-slate-900 dark:text-slate-100">Заявки на согласование изменений (Change Requests)</h2>
          <p class="text-[11px] text-slate-500 dark:text-slate-400">Кластер: <span class="font-mono font-semibold">{clusterId}</span></p>
        </div>
      </div>
      <div class="flex items-center gap-2">
        <button
          onclick={loadRequests}
          title="Обновить список"
          class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-200 dark:hover:bg-slate-800 text-slate-500 dark:text-slate-400 transition cursor-pointer"
        >
          <RefreshCw class="w-3.5 h-3.5 {isLoading ? 'animate-spin' : ''}" />
        </button>
        <button
          onclick={() => isOpen = false}
          class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-200 dark:hover:bg-slate-800 text-slate-500 dark:text-slate-400 transition cursor-pointer"
        >
          <X class="w-4 h-4" />
        </button>
      </div>
    </div>

    <!-- Error Banner -->
    {#if errorMessage}
      <div class="px-6 py-2.5 bg-red-50 dark:bg-red-950/50 border-b border-red-200 dark:border-red-800 text-red-700 dark:text-red-300 text-xs flex justify-between items-center">
        <span>{errorMessage}</span>
        <button onclick={() => errorMessage = ''} class="text-red-500 hover:text-red-800 dark:hover:text-red-200">✕</button>
      </div>
    {/if}

    <!-- Content: Left List & Right Detail -->
    <div class="flex-1 flex overflow-hidden">
      <!-- Left List (35%) -->
      <div class="w-[330px] border-r border-slate-200 dark:border-slate-800 flex flex-col bg-slate-50/50 dark:bg-slate-950/30">
        <!-- Filter Tabs -->
        <div class="p-2 border-b border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex flex-wrap gap-1 text-[11px] font-medium">
          {#each [
            { id: 'ALL', label: 'Все' },
            { id: 'SUBMITTED', label: 'Ожидают' },
            { id: 'APPROVED', label: 'Одобрены' },
            { id: 'REJECTED', label: 'Отклонены' },
            { id: 'CANCELLED', label: 'Отозваны' },
            { id: 'MY', label: 'Мои' }
          ] as tab}
            <button
              onclick={() => { filterStatus = tab.id; }}
              class="px-2 py-0.5 rounded-md text-center transition cursor-pointer {
                filterStatus === tab.id
                  ? 'bg-sky-600 text-white font-semibold shadow-xs'
                  : 'text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800'
              }"
            >
              {tab.label}
            </button>
          {/each}
        </div>

        <!-- Requests Scroll Area -->
        <div class="flex-1 overflow-y-auto divide-y divide-slate-100 dark:divide-slate-800/60">
          {#if displayedRequests.length === 0}
            <div class="p-8 text-center text-xs text-slate-400 dark:text-slate-500">
              Заявок не найдено
            </div>
          {:else}
            {#each displayedRequests as r}
              {@const badge = statusBadge(r.status)}
              {@const BadgeIcon = badge.icon}
              <button
                onclick={() => selectRequest(r.id)}
                class="w-full text-left p-3 transition border-l-3 cursor-pointer {
                  selectedId === r.id
                    ? 'bg-white dark:bg-slate-900 border-l-sky-600 shadow-xs'
                    : 'border-l-transparent hover:bg-white/80 dark:hover:bg-slate-800/40'
                }"
              >
                <div class="flex items-center justify-between gap-1 mb-1">
                  <span class="text-[10px] font-mono font-bold text-slate-500 dark:text-slate-400">#CR-{r.id}</span>
                  <div class="flex items-center gap-1">
                    {#if r.deployment_status === 'SUCCESS'}
                      <span class="text-[9px] font-bold px-1.5 py-0.2 rounded bg-emerald-100 dark:bg-emerald-950/60 text-emerald-800 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800">AWX ✓</span>
                    {:else if r.deployment_status === 'DEPLOYING'}
                      <span class="text-[9px] font-bold px-1.5 py-0.2 rounded bg-sky-100 dark:bg-sky-950/60 text-sky-800 dark:text-sky-300 border border-sky-200 dark:border-sky-800 animate-pulse">AWX...</span>
                    {:else if r.deployment_status === 'FAILED'}
                      <span class="text-[9px] font-bold px-1.5 py-0.2 rounded bg-red-100 dark:bg-red-950/60 text-red-800 dark:text-red-300 border border-red-200 dark:border-red-800">AWX ✕</span>
                    {/if}
                    <span class="flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.2 rounded border {badge.bg}">
                      <BadgeIcon class="w-2.5 h-2.5" />
                      <span>{badge.text}</span>
                    </span>
                  </div>
                </div>
                <div class="text-xs font-semibold text-slate-900 dark:text-slate-100 truncate mb-1">{r.title}</div>
                <div class="flex items-center justify-between text-[11px] text-slate-400 dark:text-slate-500">
                  <span class="flex items-center gap-1">
                    <User class="w-3 h-3" />
                    <span class={r.author === currentUsername ? 'font-semibold text-indigo-600 dark:text-indigo-400' : ''}>{r.author}</span>
                  </span>
                  <span>{formatDate(r.created_at)}</span>
                </div>
              </button>
            {/each}
          {/if}
        </div>
      </div>

      <!-- Right Detail View (65%) -->
      <div class="flex-1 flex flex-col overflow-y-auto bg-white dark:bg-slate-900">
        {#if !selectedDetail}
          <div class="flex-1 flex items-center justify-center text-xs text-slate-400 dark:text-slate-500">
            Выберите заявку из списка слева
          </div>
        {:else}
          {@const badge = statusBadge(selectedDetail.status)}
          {@const BadgeIcon = badge.icon}
          
          <div class="p-6 space-y-5 flex-1 overflow-y-auto">
            <!-- Title & Meta Header -->
            <div class="border-b border-slate-100 dark:border-slate-800 pb-4">
              <div class="flex items-center justify-between gap-2 mb-1.5">
                <span class="text-xs font-mono font-bold text-slate-400 dark:text-slate-500">Заявка #CR-{selectedDetail.id}</span>
                <span class="flex items-center gap-1 text-xs font-bold px-2 py-0.5 rounded border {badge.bg}">
                  <BadgeIcon class="w-3 h-3" />
                  <span>{badge.text}</span>
                </span>
              </div>
              <h1 class="text-base font-bold text-slate-900 dark:text-slate-100 mb-2">{selectedDetail.title}</h1>
              
              <div class="grid grid-cols-2 gap-2 text-xs text-slate-600 dark:text-slate-400 bg-slate-50 dark:bg-slate-950/60 p-2.5 rounded-lg border border-slate-100 dark:border-slate-800">
                <div class="flex items-center gap-1.5">
                  <User class="w-3.5 h-3.5 text-slate-400 dark:text-slate-500" />
                  <span>Автор: <strong class="text-slate-800 dark:text-slate-200">{selectedDetail.author}</strong></span>
                </div>
                <div class="flex items-center gap-1.5">
                  <Calendar class="w-3.5 h-3.5 text-slate-400 dark:text-slate-500" />
                  <span>Создана: {formatDate(selectedDetail.created_at)}</span>
                </div>
                {#if selectedDetail.reviewer}
                  <div class="flex items-center gap-1.5 col-span-2 border-t border-slate-200/60 dark:border-slate-800 pt-1.5">
                    <span>Рецензент: <strong class="text-slate-800 dark:text-slate-200">{selectedDetail.reviewer}</strong> ({formatDate(selectedDetail.reviewed_at || '')})</span>
                  </div>
                {/if}
              </div>

              {#if selectedDetail.description}
                <div class="mt-3 text-xs text-slate-700 dark:text-slate-300 bg-white dark:bg-slate-950 p-2.5 rounded-lg border border-slate-200 dark:border-slate-800">
                  <span class="text-[10px] uppercase font-bold text-slate-400 dark:text-slate-500 block mb-0.5">Обоснование изменений:</span>
                  <p class="whitespace-pre-wrap leading-relaxed">{selectedDetail.description}</p>
                </div>
              {/if}

              {#if selectedDetail.review_comment}
                <div class="mt-2 text-xs p-2.5 rounded-lg border {
                  selectedDetail.status === 'APPROVED'
                    ? 'bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200 dark:border-emerald-800 text-emerald-900 dark:text-emerald-300'
                    : 'bg-red-50 dark:bg-red-950/40 border-red-200 dark:border-red-800 text-red-900 dark:text-red-300'
                }">
                  <span class="text-[10px] uppercase font-bold block mb-0.5">Комментарий администратора:</span>
                  <p class="whitespace-pre-wrap leading-relaxed">{selectedDetail.review_comment}</p>
                </div>
              {/if}
            </div>

            <!-- Diffs Section -->
            <div>
              <div class="flex items-center justify-between mb-2">
                <h3 class="text-xs font-bold text-slate-800 dark:text-slate-200 uppercase tracking-wide">
                  Изменения конфигурации ({selectedDetail.diffs.length})
                </h3>
                <div class="flex items-center gap-2">
                  <button
                    onclick={handlePreviewXml}
                    disabled={actionLoading}
                    class="flex items-center gap-1 text-xs font-semibold text-indigo-600 dark:text-indigo-400 hover:text-indigo-800 dark:hover:text-indigo-300 bg-indigo-50 dark:bg-indigo-950/60 hover:bg-indigo-100 dark:hover:bg-indigo-900/60 px-2.5 py-1 rounded-lg border border-indigo-200 dark:border-indigo-800 transition cursor-pointer"
                  >
                    <Eye class="w-3.5 h-3.5" />
                    <span>Предпросмотр XML</span>
                  </button>
                  <button
                    onclick={() => handleLoadDraft()}
                    class="flex items-center gap-1 text-xs font-semibold text-sky-600 dark:text-sky-400 hover:text-sky-800 dark:hover:text-sky-300 bg-sky-50 dark:bg-sky-950/60 hover:bg-sky-100 dark:hover:bg-sky-900/60 px-2.5 py-1 rounded-lg border border-sky-200 dark:border-sky-800 transition cursor-pointer"
                  >
                    <ArrowUpRight class="w-3.5 h-3.5" />
                    <span>Открыть в редакторе</span>
                  </button>
                </div>
              </div>

              <div class="border border-slate-200 dark:border-slate-800 rounded-xl overflow-hidden shadow-xs divide-y divide-slate-100 dark:divide-slate-800">
                {#each selectedDetail.diffs as diff}
                  <div class="p-3 bg-white dark:bg-slate-900 text-xs space-y-1.5">
                    <div class="flex items-center justify-between">
                      <div class="flex items-center gap-2">
                        <span class="font-mono font-semibold text-slate-900 dark:text-slate-100">{diff.path}</span>
                        <span class="text-[10px] font-bold px-1.5 py-0.2 rounded {
                          diff.action === 'created' ? 'bg-emerald-100 dark:bg-emerald-950/60 text-emerald-700 dark:text-emerald-300' :
                          diff.action === 'deleted' ? 'bg-red-100 dark:bg-red-950/60 text-red-700 dark:text-red-300' :
                          'bg-sky-100 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300'
                        }">
                          {diff.action.toUpperCase()}
                        </span>
                      </div>
                      <button
                        onclick={() => handleLoadDraft(diff.path)}
                        class="text-[11px] font-medium text-sky-600 dark:text-sky-400 hover:text-sky-800 dark:hover:text-sky-300 hover:underline flex items-center gap-1 cursor-pointer bg-sky-50 dark:bg-sky-950/60 hover:bg-sky-100 dark:hover:bg-sky-900/60 px-2 py-0.5 rounded border border-sky-200 dark:border-sky-800 transition"
                        title="Загрузить и открыть данную очередь в редакторе"
                      >
                        <span>Редактировать</span>
                        <ArrowRight class="w-3 h-3" />
                      </button>
                    </div>

                    <!-- Comparison Grid -->
                    <div class="grid grid-cols-2 gap-3 bg-slate-50 dark:bg-slate-950/60 p-2.5 rounded-lg text-[11px] font-mono border border-slate-100 dark:border-slate-800/60">
                      <div>
                        <span class="text-slate-400 dark:text-slate-500 block text-[10px] uppercase font-sans font-semibold">Capacity (%):</span>
                        {#if diff.live_capacity !== undefined && diff.live_capacity !== null}
                          <span class="text-slate-700 dark:text-slate-300">{diff.live_capacity.toFixed(1)}%</span>
                          {#if diff.draft_capacity !== undefined && diff.draft_capacity !== null && Math.abs(diff.draft_capacity - diff.live_capacity) > 0.01}
                            <span class="text-slate-400 dark:text-slate-500 mx-1">→</span>
                            <span class="font-bold text-indigo-700 dark:text-indigo-400">{diff.draft_capacity.toFixed(1)}%</span>
                            {#if diff.delta_capacity !== undefined && diff.delta_capacity !== null}
                              <span class="text-[10px] ml-1 {diff.delta_capacity > 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                                ({diff.delta_capacity > 0 ? '+' : ''}{diff.delta_capacity.toFixed(1)}%)
                              </span>
                            {/if}
                          {/if}
                        {:else if diff.draft_capacity !== undefined && diff.draft_capacity !== null}
                          <span class="font-bold text-emerald-700 dark:text-emerald-400">+{diff.draft_capacity.toFixed(1)}%</span>
                        {/if}
                      </div>

                      <div>
                        <span class="text-slate-400 dark:text-slate-500 block text-[10px] uppercase font-sans font-semibold">Max Capacity (%):</span>
                        {#if diff.live_max_capacity !== undefined && diff.live_max_capacity !== null}
                          <span class="text-slate-700 dark:text-slate-300">{diff.live_max_capacity.toFixed(1)}%</span>
                          {#if diff.draft_max_capacity !== undefined && diff.draft_max_capacity !== null && Math.abs(diff.draft_max_capacity - diff.live_max_capacity) > 0.01}
                            <span class="text-slate-400 dark:text-slate-500 mx-1">→</span>
                            <span class="font-bold text-indigo-700 dark:text-indigo-400">{diff.draft_max_capacity.toFixed(1)}%</span>
                            {#if diff.delta_max_capacity !== undefined && diff.delta_max_capacity !== null}
                              <span class="text-[10px] ml-1 {diff.delta_max_capacity > 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                                ({diff.delta_max_capacity > 0 ? '+' : ''}{diff.delta_max_capacity.toFixed(1)}%)
                              </span>
                            {/if}
                          {/if}
                        {:else if diff.draft_max_capacity !== undefined && diff.draft_max_capacity !== null}
                          <span class="font-bold text-emerald-700 dark:text-emerald-400">+{diff.draft_max_capacity.toFixed(1)}%</span>
                        {/if}
                      </div>

                      {#if diff.live_memory_mb != null || diff.draft_memory_mb != null}
                        <div>
                          <span class="text-slate-400 dark:text-slate-500 block text-[10px] uppercase font-sans font-semibold">Память RAM:</span>
                          <span class="text-slate-700 dark:text-slate-300">{formatMemory(diff.live_memory_mb)}</span>
                          {#if diff.draft_memory_mb != null && diff.draft_memory_mb !== diff.live_memory_mb}
                            <span class="text-slate-400 dark:text-slate-500 mx-1">→</span>
                            <span class="font-bold text-indigo-700 dark:text-indigo-400">{formatMemory(diff.draft_memory_mb)}</span>
                            {#if diff.live_memory_mb != null && formatMemoryDelta(diff.live_memory_mb, diff.draft_memory_mb)}
                              <span class="text-[10px] ml-1 {diff.draft_memory_mb > diff.live_memory_mb ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                                ({formatMemoryDelta(diff.live_memory_mb, diff.draft_memory_mb)})
                              </span>
                            {/if}
                          {/if}
                        </div>
                      {/if}

                      {#if diff.live_vcores != null || diff.draft_vcores != null}
                        <div>
                          <span class="text-slate-400 dark:text-slate-500 block text-[10px] uppercase font-sans font-semibold">Ядра vCPU:</span>
                          <span class="text-slate-700 dark:text-slate-300">{formatVcores(diff.live_vcores)}</span>
                          {#if diff.draft_vcores != null && diff.draft_vcores !== diff.live_vcores}
                            <span class="text-slate-400 dark:text-slate-500 mx-1">→</span>
                            <span class="font-bold text-blue-700 dark:text-blue-400">{formatVcores(diff.draft_vcores)}</span>
                            {#if diff.live_vcores != null && formatVcoresDelta(diff.live_vcores, diff.draft_vcores)}
                              <span class="text-[10px] ml-1 {diff.draft_vcores > diff.live_vcores ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                                ({formatVcoresDelta(diff.live_vcores, diff.draft_vcores)})
                              </span>
                            {/if}
                          {/if}
                        </div>
                      {/if}
                    </div>

                    <!-- Additional Queue Attributes diff -->
                    {#if diff.draft_ordering_policy || diff.draft_user_limit_factor != null || diff.draft_max_applications != null}
                      <div class="flex flex-wrap gap-2 text-[10px] text-slate-600 dark:text-slate-400 font-sans pt-1">
                        {#if diff.draft_ordering_policy}
                          <span class="bg-purple-50 dark:bg-purple-950/60 text-purple-700 dark:text-purple-300 px-1.5 py-0.5 rounded border border-purple-200 dark:border-purple-800">
                            Policy: <strong>{diff.draft_ordering_policy.toUpperCase()}</strong>
                          </span>
                        {/if}
                        {#if diff.draft_user_limit_factor != null}
                          <span class="bg-sky-50 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300 px-1.5 py-0.5 rounded border border-sky-200 dark:border-sky-800">
                            ULF: <strong>{diff.draft_user_limit_factor}x</strong>
                          </span>
                        {/if}
                        {#if diff.draft_max_applications != null}
                          <span class="bg-indigo-50 dark:bg-indigo-950/60 text-indigo-700 dark:text-indigo-300 px-1.5 py-0.5 rounded border border-indigo-200 dark:border-indigo-800">
                            Max Apps: <strong>{diff.draft_max_applications}</strong>
                          </span>
                        {/if}
                      </div>
                    {/if}
                  </div>
                {/each}
              </div>
            </div>

            <!-- Approved XML preview banner -->
            {#if selectedDetail.status === 'APPROVED' && selectedDetail.xml_content}
              <div class="p-3.5 bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800/80 rounded-xl flex items-center justify-between">
                <div>
                  <span class="text-xs font-bold text-emerald-900 dark:text-emerald-200 block">Конфигурация XML сгенерирована и одобрена</span>
                  <span class="text-[11px] text-emerald-700 dark:text-emerald-400">Готова для применения на кластере YARN</span>
                </div>
                <button
                  onclick={() => onViewXml(selectedDetail?.xml_content || '', selectedDetail?.title || '')}
                  class="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-emerald-600 text-white text-xs font-semibold hover:bg-emerald-700 transition cursor-pointer shadow-xs"
                >
                  <FileCode class="w-3.5 h-3.5" />
                  <span>Просмотреть XML</span>
                </button>
              </div>

              <!-- AWX Deployment Card -->
              <div class="p-4 rounded-xl border space-y-3 {
                selectedDetail.deployment_status === 'SUCCESS'
                  ? 'bg-emerald-50/70 dark:bg-emerald-950/30 border-emerald-200 dark:border-emerald-800'
                  : selectedDetail.deployment_status === 'FAILED'
                  ? 'bg-red-50/70 dark:bg-red-950/30 border-red-200 dark:border-red-800'
                  : 'bg-indigo-50/70 dark:bg-indigo-950/30 border-indigo-200 dark:border-indigo-800'
              }">
                <div class="flex items-center justify-between gap-3">
                  <div class="flex items-center gap-2.5">
                    <div class="p-2 rounded-lg {
                      selectedDetail.deployment_status === 'SUCCESS' ? 'bg-emerald-600 text-white' : 'bg-indigo-600 text-white'
                    }">
                      <Rocket class="w-4 h-4" />
                    </div>
                    <div>
                      <div class="flex items-center gap-2">
                        <span class="text-xs font-bold {
                          selectedDetail.deployment_status === 'SUCCESS' ? 'text-emerald-950 dark:text-emerald-200' : 'text-indigo-950 dark:text-indigo-200'
                        }">
                          {#if selectedDetail.deployment_status === 'SUCCESS'}
                            Конфигурация успешно применена на кластере через AWX
                          {:else if selectedDetail.deployment_status === 'DEPLOYING'}
                            Выполняется доставка и применение через AWX...
                          {:else if selectedDetail.deployment_status === 'FAILED'}
                            Сбой применения конфигурации через AWX
                          {:else}
                            Автоматическая доставка через Ansible AWX
                          {/if}
                        </span>
                        {#if selectedDetail.awx_job_id}
                          <span class="text-[10px] font-mono px-1.5 py-0.2 rounded bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 text-slate-700 dark:text-slate-300 font-semibold">
                            Job #{selectedDetail.awx_job_id}
                          </span>
                        {/if}
                      </div>
                      <p class="text-[11px] text-slate-600 dark:text-slate-400 mt-0.5">
                        {#if selectedDetail.deployment_status === 'SUCCESS'}
                          Очереди обновлены в Active RM без перезапуска. {selectedDetail.deployed_at ? `Время: ${formatDate(selectedDetail.deployed_at)}` : ''}
                        {:else if selectedDetail.deployment_status === 'FAILED'}
                          {selectedDetail.deployment_error || 'Ошибка при вызове rmadmin -refreshQueues. Сработал автоматический откат.'}
                        {:else}
                          Раскладка capacity-scheduler.xml на все ноды RM с горячим обновлением очередей (refreshQueues).
                        {/if}
                      </p>
                    </div>
                  </div>

                  <div class="flex items-center gap-2">
                    {#if selectedDetail.awx_job_id}
                      <button
                        onclick={handleFetchLogs}
                        class="flex items-center gap-1 px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 text-xs font-semibold text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800 transition cursor-pointer"
                        title="Просмотреть консольный лог выполнения задачи"
                      >
                        <Terminal class="w-3.5 h-3.5" />
                        <span>Лог AWX</span>
                      </button>
                    {/if}

                    {#if canAdmin}
                      <button
                        onclick={handleDeploy}
                        disabled={isDeploying}
                        class="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-gradient-to-r from-indigo-600 to-sky-600 text-white text-xs font-semibold shadow-xs hover:shadow-md disabled:opacity-50 transition cursor-pointer"
                      >
                        {#if isDeploying}
                          <RefreshCw class="w-3.5 h-3.5 animate-spin" />
                          <span>Применение...</span>
                        {:else}
                          <Rocket class="w-3.5 h-3.5" />
                          <span>{selectedDetail.deployment_status === 'SUCCESS' ? 'Применить повторно' : 'Применить на кластере'}</span>
                        {/if}
                      </button>
                    {/if}
                  </div>
                </div>

                {#if showLogs && deployStdout}
                  <div class="mt-3 border-t border-slate-200/80 dark:border-slate-800 pt-3">
                    <div class="flex items-center justify-between mb-1.5">
                      <span class="text-[11px] font-mono font-bold text-slate-700 dark:text-slate-300 flex items-center gap-1">
                        <Terminal class="w-3 h-3 text-slate-500 dark:text-slate-400" />
                        Вывод консоли Ansible (stdout):
                      </span>
                      <button
                        onclick={() => showLogs = false}
                        class="text-[10px] text-slate-400 dark:text-slate-500 hover:text-slate-600 dark:hover:text-slate-300 cursor-pointer"
                      >
                        Скрыть лог
                      </button>
                    </div>
                    <pre class="p-3 bg-slate-900 dark:bg-slate-950 text-emerald-400 text-[11px] font-mono rounded-lg overflow-x-auto max-h-48 whitespace-pre-wrap leading-relaxed border border-slate-800">{deployStdout}</pre>
                  </div>
                {/if}
              </div>
            {/if}
          </div>

          <!-- Bottom Action Bar -->
          {#if selectedDetail.status === 'SUBMITTED'}
            <div class="border-t border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-950/50 p-4 space-y-3">
              {#if canAdmin}
                <div>
                  <label for="review-comment" class="block text-[11px] font-semibold text-slate-700 dark:text-slate-300 mb-1">
                    Комментарий администратора (опционально для одобрения, рекомендуется при отклонении)
                  </label>
                  <input
                    id="review-comment"
                    type="text"
                    bind:value={reviewComment}
                    placeholder="Причина решения или комментарий к конфигурации..."
                    class="w-full px-3 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 text-xs text-slate-900 dark:text-slate-100 outline-none focus:border-sky-500"
                  />
                </div>

                <div class="flex items-center gap-2">
                  {#if selectedDetail.author === currentUsername}
                    <button
                      onclick={handleCancel}
                      disabled={actionLoading}
                      title="Отозвать свою заявку"
                      class="px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 text-slate-600 dark:text-slate-400 text-xs font-semibold hover:bg-slate-100 dark:hover:bg-slate-800 disabled:opacity-50 transition cursor-pointer"
                    >
                      <Trash2 class="w-3.5 h-3.5 inline mr-1" />
                      <span>Отозвать</span>
                    </button>
                  {/if}

                  <button
                    onclick={handleReject}
                    disabled={actionLoading}
                    class="flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg border border-red-200 dark:border-red-800 text-red-700 dark:text-red-400 text-xs font-semibold hover:bg-red-50 dark:hover:bg-red-950/40 disabled:opacity-50 transition cursor-pointer"
                  >
                    <XCircle class="w-4 h-4" />
                    <span>Отклонить заявку</span>
                  </button>

                  <button
                    onclick={handleApprove}
                    disabled={actionLoading}
                    class="flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg bg-gradient-to-r from-emerald-600 to-teal-600 text-white text-xs font-semibold shadow-md hover:shadow-lg disabled:opacity-50 transition cursor-pointer"
                  >
                    <Check class="w-4 h-4" />
                    <span>Одобрить и сгенерировать XML</span>
                  </button>
                </div>
              {:else if selectedDetail.author === currentUsername}
                <div class="flex items-center justify-between">
                  <span class="text-xs text-slate-500 dark:text-slate-400">Заявка ожидает проверки администратором</span>
                  <button
                    onclick={handleCancel}
                    disabled={actionLoading}
                    class="flex items-center gap-1 px-3 py-1.5 rounded-lg border border-red-200 dark:border-red-800 text-red-600 dark:text-red-400 text-xs font-semibold hover:bg-red-50 dark:hover:bg-red-950/40 disabled:opacity-50 transition cursor-pointer"
                  >
                    <Trash2 class="w-3.5 h-3.5" />
                    <span>Отозвать заявку</span>
                  </button>
                </div>
              {:else}
                <div class="flex items-center justify-between">
                  <span class="text-xs text-slate-500 dark:text-slate-400">Заявка ожидает согласования администратором</span>
                  <button
                    onclick={() => handleLoadDraft()}
                    class="flex items-center gap-1 px-3 py-1.5 rounded-lg border border-sky-200 dark:border-sky-800 text-sky-700 dark:text-sky-400 text-xs font-semibold hover:bg-sky-50 dark:hover:bg-sky-950/40 transition cursor-pointer"
                  >
                    <ArrowUpRight class="w-3.5 h-3.5" />
                    <span>Открыть в редакторе очередей</span>
                  </button>
                </div>
              {/if}
            </div>
          {/if}
        {/if}
      </div>
    </div>
  </div>
{/if}

