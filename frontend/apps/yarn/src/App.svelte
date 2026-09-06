<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from './api/client';
  import type {
    UserSession, ClusterSummary, QueueNode, ClusterMetrics,
    BranchBalance, DraftQueueItem, DiffItem, QueueMappingsDiff,
  } from './types';
  import Header from './components/Header.svelte';
  import LoginModal from './components/LoginModal.svelte';
  import ClusterMetricsBar from './components/ClusterMetricsBar.svelte';
  import PartitionSelector from './components/PartitionSelector.svelte';
  import QueueTreeTable from './components/QueueTreeTable.svelte';
  import QueueEditDrawer from './components/QueueEditDrawer.svelte';
  import AddQueueModal from './components/AddQueueModal.svelte';
  import ResourceBalanceCard from './components/ResourceBalanceCard.svelte';
  import DiffPanel from './components/DiffPanel.svelte';
  import XmlExportModal from './components/XmlExportModal.svelte';
  import SubmitChangeRequestModal from './components/SubmitChangeRequestModal.svelte';
  import ChangeRequestsDrawer from './components/ChangeRequestsDrawer.svelte';
  import QueueMappingsModal from './components/QueueMappingsModal.svelte';
  import { GitCompareArrows, RotateCcw, FileDown, RefreshCw, Send, GitPullRequest, ArrowRightLeft } from 'lucide-svelte';

  // Auth state
  let user = $state<UserSession | null>(null);
  let clusters = $state<ClusterSummary[]>([]);
  let selectedClusterId = $state('');

  // Queue tree state
  let rootQueue = $state<QueueNode | null>(null);
  let clusterMetrics = $state<ClusterMetrics | null>(null);
  let balances = $state<BranchBalance[]>([]);
  let selectedPartition = $state('DEFAULT');
  let partitions = $state<string[]>(['DEFAULT']);
  let resourceMode = $state('percentage');
  let displayMode = $state<'percentage' | 'absolute'>('percentage');
  let exportMode = $state<'percentage' | 'absolute'>('percentage');

  // Queue Mappings state
  let liveQueueMappings = $state<string>('');
  let liveQueueMappingsOverride = $state<boolean>(false);
  let draftQueueMappings = $state<string>('');
  let draftQueueMappingsOverride = $state<boolean>(false);
  let queueMappingsDiff = $state<QueueMappingsDiff | null>(null);
  let isMappingsModalOpen = $state(false);

  // Draft state
  let draftChanges = $state(new Map<string, DraftQueueItem>());

  // Change Requests state
  let showSubmitCrModal = $state(false);
  let showCrDrawer = $state(false);
  let pendingCrCount = $state(0);

  // UI state
  let editingQueue = $state<QueueNode | null>(null);
  let isDrawerOpen = $state(false);
  let showAddModal = $state(false);
  let addParentPath = $state('root');
  let showDiffPanel = $state(false);
  let diffs = $state<DiffItem[]>([]);
  let showXmlModal = $state(false);
  let xmlContent = $state('');
  let xmlFilename = $state('');
  let xmlInstructions = $state('');
  let isLoading = $state(false);
  let loadError = $state('');

  const activeCluster = $derived(clusters.find(c => c.id === selectedClusterId));
  const canWrite = $derived(activeCluster?.can_write || false);
  const canAdmin = $derived(activeCluster?.can_admin || false);
  const draftCount = $derived(draftChanges.size);
  const isMappingsModified = $derived(
    draftQueueMappings !== liveQueueMappings || draftQueueMappingsOverride !== liveQueueMappingsOverride
  );
  const totalChangesCount = $derived(draftCount + (isMappingsModified ? 1 : 0));

  onMount(async () => {
    try {
      user = await api.getMe();
      await loadClusters();
    } catch {
      // Не авторизован
    }
  });

  // Загрузка дерева при смене кластера
  $effect(() => {
    if (selectedClusterId && user) {
      loadQueueTree();
    }
  });

  async function loadClusters() {
    clusters = await api.getClusters();
    if (clusters.length > 0 && !selectedClusterId) {
      selectedClusterId = clusters[0].id;
    }
  }

  async function loadQueueTree() {
    if (!selectedClusterId) return;
    isLoading = true;
    loadError = '';
    try {
      const resp = await api.getQueueTree(selectedClusterId);
      rootQueue = resp.root_queue;
      clusterMetrics = resp.cluster_metrics;
      balances = resp.balances;
      partitions = resp.partitions;
      resourceMode = resp.resource_mode;
      selectedPartition = resp.default_partition;
      liveQueueMappings = resp.queue_mappings || '';
      liveQueueMappingsOverride = resp.queue_mappings_override || false;
      draftQueueMappings = liveQueueMappings;
      draftQueueMappingsOverride = liveQueueMappingsOverride;
      queueMappingsDiff = null;
      // Сбрасываем черновик при смене кластера
      draftChanges = new Map();
      await loadPendingCrCount();
    } catch (err: any) {
      loadError = err.message || 'Ошибка загрузки очередей';
    } finally {
      isLoading = false;
    }
  }

  async function handleLogin(username: string, password: string) {
    user = await api.login(username, password);
    await loadClusters();
  }

  async function handleLogout() {
    await api.logout();
    user = null;
    clusters = [];
    rootQueue = null;
    clusterMetrics = null;
  }

  function handleEditQueue(queue: QueueNode) {
    editingQueue = queue;
    isDrawerOpen = true;
  }

  function handleSaveDraft(draft: DraftQueueItem) {
    const newMap = new Map(draftChanges);
    newMap.set(draft.path, draft);
    draftChanges = newMap;
    recalcBalances();
  }

  function handleAddChild(parentPath: string) {
    addParentPath = parentPath;
    showAddModal = true;
  }

  function handleConfirmAdd(draft: DraftQueueItem) {
    const newMap = new Map(draftChanges);
    newMap.set(draft.path, draft);
    draftChanges = newMap;

    // Добавляем в дерево для мгновенного отображения
    if (rootQueue) {
      addNodeToTree(rootQueue, draft);
      rootQueue = { ...rootQueue }; // trigger reactivity
    }
    recalcBalances();
  }

  function addNodeToTree(node: QueueNode, draft: DraftQueueItem) {
    if (node.path === draft.parent_path) {
      const part = draft.partitions[selectedPartition] || Object.values(draft.partitions)[0];
      const newNode: QueueNode = {
        name: draft.name,
        path: draft.path,
        parent_path: draft.parent_path,
        is_leaf: true,
        state: draft.state,
        partitions: draft.partitions,
        current_used_resources: { memory_mb: 0, vcores: 0 },
        allocated_resources: { memory_mb: 0, vcores: 0 },
        current_used_percent: 0,
        num_applications: 0,
        num_active_applications: 0,
        num_pending_applications: 0,
        children: [],
      };
      node.children = [...node.children, newNode];
      node.is_leaf = false;
      return;
    }
    for (const child of node.children) {
      addNodeToTree(child, draft);
    }
  }

  function handleDelete(path: string) {
    if (path === 'root') return;
    const newMap = new Map(draftChanges);

    // Находим все дочерние пути
    function collectPaths(node: QueueNode): string[] {
      const paths = [node.path];
      for (const child of node.children) {
        paths.push(...collectPaths(child));
      }
      return paths;
    }

    // Находим узел в дереве
    function findNode(node: QueueNode, targetPath: string): QueueNode | null {
      if (node.path === targetPath) return node;
      for (const child of node.children) {
        const found = findNode(child, targetPath);
        if (found) return found;
      }
      return null;
    }

    if (rootQueue) {
      const target = findNode(rootQueue, path);
      if (target) {
        const allPaths = collectPaths(target);
        for (const p of allPaths) {
          newMap.set(p, {
            path: p,
            name: p.split('.').pop() || p,
            parent_path: p.includes('.') ? p.substring(0, p.lastIndexOf('.')) : undefined,
            action: 'delete',
            is_leaf: true,
            state: 'STOPPED',
            partitions: {},
          });
        }
      }
    }

    draftChanges = newMap;
    recalcBalances();
  }

  function resetDraft() {
    draftChanges = new Map();
    draftQueueMappings = liveQueueMappings;
    draftQueueMappingsOverride = liveQueueMappingsOverride;
    queueMappingsDiff = null;
    loadQueueTree();
  }

  function handleSaveMappings(mappings: string, override: boolean) {
    draftQueueMappings = mappings;
    draftQueueMappingsOverride = override;
  }

  function recalcBalances() {
    // Локальный расчёт балансов с учётом draft
    if (!rootQueue) return;

    const newBalances: BranchBalance[] = [];
    const totalMem = activeCluster?.total_resources.memory_mb || 2097152;
    const totalCores = activeCluster?.total_resources.vcores || 1024;

    function recurse(node: QueueNode) {
      if (node.children.length > 0) {
        let totalCap = 0;
        let totalMemAlloc = 0;
        let totalCoresAlloc = 0;

        for (const child of node.children) {
          const draft = draftChanges.get(child.path);
          if (draft?.action === 'delete') continue;
          const part = draft
            ? (draft.partitions[selectedPartition] || Object.values(draft.partitions)[0])
            : (child.partitions[selectedPartition] || child.partitions['DEFAULT']);
          if (part) {
            const cap = part.memory_percent ?? part.capacity;
            totalCap += cap;
            totalMemAlloc += part.memory_mb ?? Math.round(totalMem * (cap / 100));
            totalCoresAlloc += part.vcores ?? Math.round(totalCores * (cap / 100));
          }
        }

        const unallocated = 100 - totalCap;
        const isBalanced = Math.abs(unallocated) < 0.01;

        newBalances.push({
          parent_path: node.path,
          partition: selectedPartition,
          total_children_capacity: Math.round(totalCap * 100) / 100,
          unallocated_capacity: Math.round(unallocated * 100) / 100,
          is_balanced: isBalanced,
          status: isBalanced ? 'ok' : unallocated > 0 ? 'underallocated' : 'overallocated',
          message: '',
          total_children_memory_mb: totalMemAlloc,
          total_children_vcores: totalCoresAlloc,
          ram_is_balanced: isBalanced,
          vcpu_is_balanced: isBalanced,
        });

        for (const child of node.children) {
          recurse(child);
        }
      }
    }

    recurse(rootQueue);
    balances = newBalances;
  }

  async function handleShowDiff() {
    if (!selectedClusterId) return;
    try {
      const draftList = Array.from(draftChanges.values());
      const resp = await api.getDiff(
        selectedClusterId,
        draftList,
        selectedPartition,
        draftQueueMappings,
        draftQueueMappingsOverride
      );
      diffs = resp.diffs;
      queueMappingsDiff = resp.queue_mappings_diff || null;
      showDiffPanel = true;
    } catch (err: any) {
      alert(err.message);
    }
  }

  async function handleGenerateXml(mode?: 'percentage' | 'absolute') {
    if (!selectedClusterId || !canAdmin) return;
    try {
      const targetMode = mode || exportMode;
      exportMode = targetMode;
      const allQueues = collectAllQueues();
      const resp = await api.generateXml(
        selectedClusterId,
        allQueues,
        undefined,
        targetMode,
        draftQueueMappings,
        draftQueueMappingsOverride
      );
      xmlContent = resp.xml_content;
      xmlFilename = resp.filename;
      xmlInstructions = resp.instructions;
      showXmlModal = true;
    } catch (err: any) {
      alert(err.message);
    }
  }

  function collectAllQueues(): DraftQueueItem[] {
    if (!rootQueue) return [];
    const result: DraftQueueItem[] = [];

    function recurse(node: QueueNode) {
      const draft = draftChanges.get(node.path);
      result.push({
        path: node.path,
        name: node.name,
        parent_path: node.parent_path || undefined,
        action: draft?.action || 'modify',
        is_leaf: node.is_leaf,
        state: draft?.state || node.state,
        partitions: draft?.partitions || node.partitions,
        user_limit_factor: draft?.user_limit_factor ?? node.user_limit_factor,
        ordering_policy: draft?.ordering_policy ?? node.ordering_policy,
        max_applications: draft?.max_applications ?? node.max_applications,
        max_am_resource_percent: draft?.max_am_resource_percent ?? node.max_am_resource_percent,
        max_parallel_apps: draft?.max_parallel_apps ?? node.max_parallel_apps,
        max_application_lifetime: draft?.max_application_lifetime ?? node.max_application_lifetime,
      });
      for (const child of node.children) {
        recurse(child);
      }
    }

    recurse(rootQueue);
    return result;
  }

  async function loadPendingCrCount() {
    if (!selectedClusterId) return;
    try {
      const resp = await api.getPendingCount(selectedClusterId);
      pendingCrCount = resp.pending_count;
    } catch {
      // ignore
    }
  }

  async function handleSubmitChangeRequest(title: string, description: string) {
    if (!selectedClusterId) return;
    const changes = Array.from(draftChanges.values());
    await api.createChangeRequest({
      cluster_id: selectedClusterId,
      title,
      description,
      changes,
    });
    draftChanges = new Map();
    recalcBalances();
    await loadPendingCrCount();
    alert('Заявка на согласование успешно создана и передана администратору!');
  }

  function handleApplyCrToDraft(changes: DraftQueueItem[]) {
    const newMap = new Map(draftChanges);
    for (const c of changes) {
      newMap.set(c.path, c);
    }
    draftChanges = newMap;
    recalcBalances();
  }

  function handleViewCrXml(xml: string, title: string) {
    xmlContent = xml;
    xmlFilename = `capacity-scheduler-${selectedClusterId}.xml`;
    xmlInstructions = `XML сгенерирован для заявки: ${title}\n1. Скопируйте файл в /etc/hadoop/conf/capacity-scheduler.xml\n2. Выполните: yarn rmadmin -refreshQueues`;
    showXmlModal = true;
  }

  // Периодическое автообновление счетчика заявок для администратора
  $effect(() => {
    if (user && selectedClusterId) {
      loadPendingCrCount();
      const interval = setInterval(loadPendingCrCount, 10000);
      return () => clearInterval(interval);
    }
  });
</script>

<div class="h-screen w-screen flex flex-col overflow-hidden">
  {#if !user}
    <LoginModal onLogin={handleLogin} />
  {:else}
    <Header
      {user}
      {clusters}
      bind:selectedClusterId
      {pendingCrCount}
      onOpenChangeRequests={() => showCrDrawer = true}
      onLogout={handleLogout}
    />

    <ClusterMetricsBar metrics={clusterMetrics} />

    <!-- Toolbar: Partitions, Queue Balances, Queue Mappings & Display Mode -->
    <div class="bg-white border-b border-slate-200 px-4 sm:px-6 py-2 flex flex-wrap items-center justify-between gap-3 shrink-0">
      <div class="flex items-center gap-3 flex-wrap">
        {#if partitions.length > 1}
          <PartitionSelector {partitions} bind:selectedPartition />
        {/if}

        <ResourceBalanceCard {balances} {resourceMode} {displayMode} inline={true} />

        <button
          onclick={() => isMappingsModalOpen = true}
          class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white text-slate-700 text-xs font-medium hover:bg-slate-50 transition cursor-pointer shadow-xs"
        >
          <ArrowRightLeft class="w-3.5 h-3.5 text-indigo-600" />
          <span>Queue Mappings</span>
          {#if isMappingsModified}
            <span class="w-2 h-2 rounded-full bg-amber-500" title="Есть несохраненные изменения в правилах маппинга"></span>
          {/if}
        </button>
      </div>

      <!-- Mode Selector -->
      <div class="flex items-center gap-2">
        <span class="text-xs text-slate-500 font-medium">Режим отображения:</span>
        <div class="flex items-center bg-slate-100 p-0.5 rounded-lg border border-slate-200">
          <button
            onclick={() => displayMode = 'percentage'}
            class="px-2.5 py-1 rounded text-xs font-semibold cursor-pointer transition {
              displayMode === 'percentage' ? 'bg-white text-sky-700 shadow-xs' : 'text-slate-600 hover:text-slate-900'
            }"
          >
            % Проценты
          </button>
          <button
            onclick={() => displayMode = 'absolute'}
            class="px-2.5 py-1 rounded text-xs font-semibold cursor-pointer transition {
              displayMode === 'absolute' ? 'bg-white text-sky-700 shadow-xs' : 'text-slate-600 hover:text-slate-900'
            }"
          >
            Абсолютные (RAM/CPU)
          </button>
        </div>
      </div>
    </div>

    {#if isLoading}
      <div class="flex-1 flex items-center justify-center">
        <div class="text-center">
          <div class="w-8 h-8 border-3 border-sky-200 border-t-sky-600 rounded-full animate-spin mx-auto mb-3"></div>
          <p class="text-sm text-slate-500">Loading queues...</p>
        </div>
      </div>
    {:else if loadError}
      <div class="flex-1 flex items-center justify-center">
        <div class="text-center max-w-md">
          <p class="text-sm text-red-600 mb-3">{loadError}</p>
          <button onclick={loadQueueTree}
            class="px-4 py-2 rounded-lg bg-sky-600 text-white text-xs font-medium cursor-pointer">
            Retry
          </button>
        </div>
      </div>
    {:else}
      <QueueTreeTable
        {rootQueue}
        {resourceMode}
        {displayMode}
        clusterResources={activeCluster?.total_resources}
        {selectedPartition}
        {canWrite}
        {draftChanges}
        onAddChild={handleAddChild}
        onDelete={handleDelete}
        onEditQueue={handleEditQueue}
      />
    {/if}

    <!-- Footer Actions Bar -->
    {#if canWrite}
      <div class="h-12 bg-white border-t border-slate-200 flex items-center justify-between px-4 sm:px-6 shrink-0">
        <div class="flex items-center gap-2">
          <button onclick={loadQueueTree}
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-slate-700 text-xs font-medium hover:bg-slate-50 transition cursor-pointer">
            <RefreshCw class="w-3.5 h-3.5" />
            Обновить
          </button>
          {#if totalChangesCount > 0}
            <button onclick={resetDraft}
              class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-red-200 text-red-700 text-xs font-medium hover:bg-red-50 transition cursor-pointer">
              <RotateCcw class="w-3.5 h-3.5" />
              Сбросить все ({totalChangesCount})
            </button>
          {/if}
        </div>

        <div class="flex items-center gap-2">
          {#if totalChangesCount > 0}
            <span class="text-[11px] text-amber-600 font-medium px-2 py-1 bg-amber-50 border border-amber-200 rounded-lg">
              {totalChangesCount} изм. в черновике
            </span>

            <button onclick={handleShowDiff}
              class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-sky-200 text-sky-700 text-xs font-medium hover:bg-sky-50 transition cursor-pointer">
              <GitCompareArrows class="w-3.5 h-3.5" />
              Просмотр изменений
            </button>

            <button onclick={() => showSubmitCrModal = true}
              class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-indigo-200 text-indigo-700 bg-indigo-50/60 hover:bg-indigo-50 text-xs font-semibold transition cursor-pointer">
              <Send class="w-3.5 h-3.5" />
              Отправить на согласование
            </button>

            {#if canAdmin}
              <button onclick={() => handleGenerateXml()}
                class="flex items-center gap-1.5 px-4 py-1.5 rounded-lg bg-gradient-to-r from-sky-600 to-indigo-600 text-white text-xs font-semibold shadow-md shadow-sky-500/20 hover:shadow-lg transition cursor-pointer">
                <FileDown class="w-3.5 h-3.5" />
                Сгенерировать XML
              </button>
            {/if}
          {/if}
        </div>
      </div>
    {/if}

    <!-- Modals & Drawers -->
    <QueueEditDrawer
      queue={editingQueue}
      draftItem={editingQueue ? draftChanges.get(editingQueue.path) || null : null}
      {resourceMode}
      clusterResources={activeCluster?.total_resources}
      {selectedPartition}
      {partitions}
      bind:isOpen={isDrawerOpen}
      onSave={handleSaveDraft}
    />

    <AddQueueModal
      parentPath={addParentPath}
      bind:isOpen={showAddModal}
      {resourceMode}
      clusterResources={activeCluster?.total_resources}
      {selectedPartition}
      {partitions}
      onConfirm={handleConfirmAdd}
    />

    <DiffPanel
      {diffs}
      {queueMappingsDiff}
      {canAdmin}
      bind:isOpen={showDiffPanel}
      onGenerateXml={() => handleGenerateXml()}
    />

    <QueueMappingsModal
      rootNode={rootQueue}
      currentMappings={draftQueueMappings}
      currentOverride={draftQueueMappingsOverride}
      liveMappings={liveQueueMappings}
      liveOverride={liveQueueMappingsOverride}
      {canWrite}
      bind:isOpen={isMappingsModalOpen}
      onSave={handleSaveMappings}
    />

    <XmlExportModal
      {xmlContent}
      filename={xmlFilename}
      instructions={xmlInstructions}
      currentMode={exportMode}
      bind:isOpen={showXmlModal}
      onModeChange={(newMode) => handleGenerateXml(newMode)}
    />

    <SubmitChangeRequestModal
      clusterId={selectedClusterId}
      changes={Array.from(draftChanges.values())}
      bind:isOpen={showSubmitCrModal}
      onSubmit={handleSubmitChangeRequest}
    />

    <ChangeRequestsDrawer
      clusterId={selectedClusterId}
      {canAdmin}
      currentUsername={user?.username || ''}
      bind:isOpen={showCrDrawer}
      onApplyToDraft={handleApplyCrToDraft}
      onViewXml={handleViewCrXml}
      onStatusChange={loadPendingCrCount}
    />
  {/if}
</div>
