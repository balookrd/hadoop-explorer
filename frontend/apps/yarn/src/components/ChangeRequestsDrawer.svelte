<script lang="ts">
  import type { ChangeRequestSummary, ChangeRequestResponse, DraftQueueItem } from '../types';
  import { api } from '../api/client';
  import { 
    X, CheckCircle, XCircle, Clock, Ban, User, Calendar, 
    FileCode, RefreshCw, GitPullRequest, ArrowRight, Eye, Check, Trash2, ArrowUpRight
  } from 'lucide-svelte';
  import { formatMemory, formatVcores } from '../utils/resourceUtils';

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
    onApplyToDraft: (changes: DraftQueueItem[]) => void;
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
  let errorMessage = $state('');

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
      const statusParam = filterStatus === 'ALL' ? undefined : filterStatus;
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

  function handleLoadDraft() {
    if (!selectedDetail) return;
    onApplyToDraft(selectedDetail.changes);
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

{#if isOpen}
  <!-- Backdrop -->
  <div
    class="fixed inset-0 bg-black/30 z-40 backdrop-blur-xs"
    onclick={() => isOpen = false}
    role="button"
    tabindex="-1"
    onkeydown={() => {}}
  ></div>

  <!-- Main Drawer Panel -->
  <div class="fixed right-0 top-0 h-full w-[900px] max-w-[95vw] bg-white border-l border-slate-200 shadow-2xl z-50 flex flex-col">
    <!-- Header -->
    <div class="flex items-center justify-between px-6 py-4 border-b border-slate-200 bg-slate-50">
      <div class="flex items-center gap-2.5">
        <div class="w-8 h-8 rounded-lg bg-indigo-100 flex items-center justify-center text-indigo-600">
          <GitPullRequest class="w-4 h-4" />
        </div>
        <div>
          <h2 class="text-sm font-bold text-slate-900">Заявки на согласование изменений (Change Requests)</h2>
          <p class="text-[11px] text-slate-500">Кластер: <span class="font-mono font-semibold">{clusterId}</span></p>
        </div>
      </div>
      <div class="flex items-center gap-2">
        <button
          onclick={loadRequests}
          title="Обновить список"
          class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-200 text-slate-500 transition cursor-pointer"
        >
          <RefreshCw class="w-3.5 h-3.5 {isLoading ? 'animate-spin' : ''}" />
        </button>
        <button
          onclick={() => isOpen = false}
          class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-200 text-slate-500 transition cursor-pointer"
        >
          <X class="w-4 h-4" />
        </button>
      </div>
    </div>

    <!-- Error Banner -->
    {#if errorMessage}
      <div class="px-6 py-2.5 bg-red-50 border-b border-red-200 text-red-700 text-xs flex justify-between items-center">
        <span>{errorMessage}</span>
        <button onclick={() => errorMessage = ''} class="text-red-500 hover:text-red-800">✕</button>
      </div>
    {/if}

    <!-- Content: Left List & Right Detail -->
    <div class="flex-1 flex overflow-hidden">
      <!-- Left List (35%) -->
      <div class="w-[320px] border-r border-slate-200 flex flex-col bg-slate-50/50">
        <!-- Filter Tabs -->
        <div class="p-2.5 border-b border-slate-200 bg-white flex gap-1 text-[11px] font-medium">
          {#each [
            { id: 'ALL', label: 'Все' },
            { id: 'SUBMITTED', label: 'Ожидают' },
            { id: 'APPROVED', label: 'Одобрены' },
            { id: 'REJECTED', label: 'Отклонены' }
          ] as tab}
            <button
              onclick={() => { filterStatus = tab.id; loadRequests(); }}
              class="flex-1 py-1 rounded-md text-center transition cursor-pointer {
                filterStatus === tab.id ? 'bg-sky-600 text-white font-semibold' : 'text-slate-600 hover:bg-slate-100'
              }"
            >
              {tab.label}
            </button>
          {/each}
        </div>

        <!-- Requests Scroll Area -->
        <div class="flex-1 overflow-y-auto divide-y divide-slate-100">
          {#if requests.length === 0}
            <div class="p-8 text-center text-xs text-slate-400">
              Заявок не найдено
            </div>
          {:else}
            {#each requests as r}
              {@const badge = statusBadge(r.status)}
              {@const BadgeIcon = badge.icon}
              <button
                onclick={() => selectRequest(r.id)}
                class="w-full text-left p-3 transition border-l-3 cursor-pointer {
                  selectedId === r.id ? 'bg-white border-l-sky-600 shadow-xs' : 'border-l-transparent hover:bg-white/80'
                }"
              >
                <div class="flex items-center justify-between gap-1 mb-1">
                  <span class="text-[10px] font-mono font-bold text-slate-500">#CR-{r.id}</span>
                  <span class="flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.2 rounded border {badge.bg}">
                    <BadgeIcon class="w-2.5 h-2.5" />
                    <span>{badge.text}</span>
                  </span>
                </div>
                <div class="text-xs font-semibold text-slate-900 truncate mb-1">{r.title}</div>
                <div class="flex items-center justify-between text-[11px] text-slate-400">
                  <span class="flex items-center gap-1">
                    <User class="w-3 h-3" />
                    <span>{r.author}</span>
                  </span>
                  <span>{formatDate(r.created_at)}</span>
                </div>
              </button>
            {/each}
          {/if}
        </div>
      </div>

      <!-- Right Detail View (65%) -->
      <div class="flex-1 flex flex-col overflow-y-auto bg-white">
        {#if !selectedDetail}
          <div class="flex-1 flex items-center justify-center text-xs text-slate-400">
            Выберите заявку из списка слева
          </div>
        {:else}
          {@const badge = statusBadge(selectedDetail.status)}
          {@const BadgeIcon = badge.icon}
          
          <div class="p-6 space-y-5 flex-1 overflow-y-auto">
            <!-- Title & Meta Header -->
            <div class="border-b border-slate-100 pb-4">
              <div class="flex items-center justify-between gap-2 mb-1.5">
                <span class="text-xs font-mono font-bold text-slate-400">Заявка #CR-{selectedDetail.id}</span>
                <span class="flex items-center gap-1 text-xs font-bold px-2 py-0.5 rounded border {badge.bg}">
                  <BadgeIcon class="w-3 h-3" />
                  <span>{badge.text}</span>
                </span>
              </div>
              <h1 class="text-base font-bold text-slate-900 mb-2">{selectedDetail.title}</h1>
              
              <div class="grid grid-cols-2 gap-2 text-xs text-slate-600 bg-slate-50 p-2.5 rounded-lg border border-slate-100">
                <div class="flex items-center gap-1.5">
                  <User class="w-3.5 h-3.5 text-slate-400" />
                  <span>Автор: <strong class="text-slate-800">{selectedDetail.author}</strong></span>
                </div>
                <div class="flex items-center gap-1.5">
                  <Calendar class="w-3.5 h-3.5 text-slate-400" />
                  <span>Создана: {formatDate(selectedDetail.created_at)}</span>
                </div>
                {#if selectedDetail.reviewer}
                  <div class="flex items-center gap-1.5 col-span-2 border-t border-slate-200/60 pt-1.5">
                    <span>Рецензент: <strong class="text-slate-800">{selectedDetail.reviewer}</strong> ({formatDate(selectedDetail.reviewed_at || '')})</span>
                  </div>
                {/if}
              </div>

              {#if selectedDetail.description}
                <div class="mt-3 text-xs text-slate-700 bg-white p-2 rounded border border-slate-200">
                  <span class="text-[10px] uppercase font-bold text-slate-400 block mb-0.5">Обоснование:</span>
                  <p class="whitespace-pre-wrap">{selectedDetail.description}</p>
                </div>
              {/if}

              {#if selectedDetail.review_comment}
                <div class="mt-2 text-xs p-2 rounded border {
                  selectedDetail.status === 'APPROVED' ? 'bg-emerald-50 border-emerald-200 text-emerald-900' : 'bg-red-50 border-red-200 text-red-900'
                }">
                  <span class="text-[10px] uppercase font-bold block mb-0.5">Комментарий администратора:</span>
                  <p class="whitespace-pre-wrap">{selectedDetail.review_comment}</p>
                </div>
              {/if}
            </div>

            <!-- Diffs Section -->
            <div>
              <div class="flex items-center justify-between mb-2">
                <h3 class="text-xs font-bold text-slate-800 uppercase tracking-wide">
                  Изменения конфигурации ({selectedDetail.diffs.length})
                </h3>
                <button
                  onclick={handleLoadDraft}
                  class="flex items-center gap-1 text-xs font-semibold text-sky-600 hover:text-sky-800 transition cursor-pointer"
                >
                  <ArrowUpRight class="w-3.5 h-3.5" />
                  <span>Открыть в редакторе очередей</span>
                </button>
              </div>

              <div class="border border-slate-200 rounded-xl overflow-hidden shadow-xs divide-y divide-slate-100">
                {#each selectedDetail.diffs as diff}
                  <div class="p-3 bg-white text-xs space-y-1.5">
                    <div class="flex items-center justify-between">
                      <span class="font-mono font-semibold text-slate-900">{diff.path}</span>
                      <span class="text-[10px] font-bold px-1.5 py-0.2 rounded {
                        diff.action === 'created' ? 'bg-emerald-100 text-emerald-700' :
                        diff.action === 'deleted' ? 'bg-red-100 text-red-700' :
                        'bg-sky-100 text-sky-700'
                      }">
                        {diff.action.toUpperCase()}
                      </span>
                    </div>

                    <!-- Comparison Grid -->
                    <div class="grid grid-cols-2 gap-3 bg-slate-50 p-2 rounded-lg text-[11px] font-mono">
                      <div>
                        <span class="text-slate-400 block text-[10px] uppercase">Capacity:</span>
                        {#if diff.live_capacity !== undefined}
                          <span class="text-slate-700">{diff.live_capacity.toFixed(1)}%</span>
                          {#if diff.draft_capacity !== undefined && diff.draft_capacity !== diff.live_capacity}
                            <span class="text-slate-400 mx-1">→</span>
                            <span class="font-bold text-indigo-700">{diff.draft_capacity.toFixed(1)}%</span>
                          {/if}
                        {:else if diff.draft_capacity !== undefined}
                          <span class="font-bold text-emerald-700">+{diff.draft_capacity.toFixed(1)}%</span>
                        {/if}
                      </div>

                      <div>
                        <span class="text-slate-400 block text-[10px] uppercase">Max Capacity:</span>
                        {#if diff.live_max_capacity !== undefined}
                          <span class="text-slate-700">{diff.live_max_capacity.toFixed(1)}%</span>
                          {#if diff.draft_max_capacity !== undefined && diff.draft_max_capacity !== diff.live_max_capacity}
                            <span class="text-slate-400 mx-1">→</span>
                            <span class="font-bold text-indigo-700">{diff.draft_max_capacity.toFixed(1)}%</span>
                          {/if}
                        {:else if diff.draft_max_capacity !== undefined}
                          <span class="font-bold text-emerald-700">+{diff.draft_max_capacity.toFixed(1)}%</span>
                        {/if}
                      </div>

                      {#if diff.live_memory_mb || diff.draft_memory_mb}
                        <div>
                          <span class="text-slate-400 block text-[10px] uppercase">Память RAM:</span>
                          <span>{formatMemory(diff.live_memory_mb)}</span>
                          {#if diff.draft_memory_mb && diff.draft_memory_mb !== diff.live_memory_mb}
                            <span class="text-slate-400 mx-1">→</span>
                            <span class="font-bold text-indigo-700">{formatMemory(diff.draft_memory_mb)}</span>
                          {/if}
                        </div>
                      {/if}

                      {#if diff.live_vcores || diff.draft_vcores}
                        <div>
                          <span class="text-slate-400 block text-[10px] uppercase">Ядра vCPU:</span>
                          <span>{formatVcores(diff.live_vcores)}</span>
                          {#if diff.draft_vcores && diff.draft_vcores !== diff.live_vcores}
                            <span class="text-slate-400 mx-1">→</span>
                            <span class="font-bold text-blue-700">{formatVcores(diff.draft_vcores)}</span>
                          {/if}
                        </div>
                      {/if}
                    </div>
                  </div>
                {/each}
              </div>
            </div>

            <!-- Approved XML preview button -->
            {#if selectedDetail.status === 'APPROVED' && selectedDetail.xml_content}
              <div class="p-3 bg-emerald-50 border border-emerald-200 rounded-xl flex items-center justify-between">
                <div>
                  <span class="text-xs font-bold text-emerald-900 block">Конфигурация XML сгенерирована</span>
                  <span class="text-[11px] text-emerald-700">Готова для применения на кластере YARN</span>
                </div>
                <button
                  onclick={() => onViewXml(selectedDetail?.xml_content || '', selectedDetail?.title || '')}
                  class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-600 text-white text-xs font-semibold hover:bg-emerald-700 transition cursor-pointer"
                >
                  <FileCode class="w-3.5 h-3.5" />
                  <span>Просмотреть XML</span>
                </button>
              </div>
            {/if}
          </div>

          <!-- Bottom Action Bar -->
          {#if selectedDetail.status === 'SUBMITTED'}
            <div class="border-t border-slate-200 bg-slate-50 p-4 space-y-3">
              {#if canAdmin}
                <div>
                  <label for="review-comment" class="block text-[11px] font-semibold text-slate-700 mb-1">
                    Комментарий рецензента (опционально для одобрения, рекомендуется при отклонении)
                  </label>
                  <input
                    id="review-comment"
                    type="text"
                    bind:value={reviewComment}
                    placeholder="Причина решения или комментарий к конфигурации..."
                    class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs text-slate-900 outline-none focus:border-sky-500"
                  />
                </div>

                <div class="flex items-center gap-2">
                  <button
                    onclick={handleReject}
                    disabled={actionLoading}
                    class="flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg border border-red-200 text-red-700 text-xs font-semibold hover:bg-red-50 disabled:opacity-50 transition cursor-pointer"
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
                  <span class="text-xs text-slate-500">Заявка ожидает проверки администратором</span>
                  <button
                    onclick={handleCancel}
                    disabled={actionLoading}
                    class="flex items-center gap-1 px-3 py-1.5 rounded-lg border border-red-200 text-red-600 text-xs font-semibold hover:bg-red-50 disabled:opacity-50 transition cursor-pointer"
                  >
                    <Trash2 class="w-3.5 h-3.5" />
                    <span>Отозвать заявку</span>
                  </button>
                </div>
              {:else}
                <div class="text-center text-xs text-slate-400 py-1">
                  Только администраторы могут согласовывать заявки
                </div>
              {/if}
            </div>
          {/if}
        {/if}
      </div>
    </div>
  </div>
{/if}
