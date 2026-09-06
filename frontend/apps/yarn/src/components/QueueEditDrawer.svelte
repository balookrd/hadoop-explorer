<script lang="ts">
  import type { QueueNode, DraftQueueItem, PartitionResourceConfig } from '../types';
  import { X, RotateCcw, Save, HardDrive, Cpu, Link, Unlink, Percent, Hash, AlertCircle, Users, Layers } from 'lucide-svelte';
  import { formatMemory, formatVcores, mbToGb, gbToMb } from '../utils/resourceUtils';

  let {
    queue,
    draftItem,
    resourceMode,
    clusterResources = { memory_mb: 2097152, vcores: 1024 },
    selectedPartition,
    partitions = [],
    isOpen = $bindable(),
    onSave,
  }: {
    queue: QueueNode | null;
    draftItem: DraftQueueItem | null;
    resourceMode: string;
    clusterResources?: { memory_mb: number; vcores: number };
    selectedPartition: string;
    partitions?: string[];
    isOpen: boolean;
    onSave: (draft: DraftQueueItem) => void;
  } = $props();

  // Режим ввода: 'percentage' или 'absolute'
  let inputMode = $state<'percentage' | 'absolute'>('percentage');
  let isLinked = $state(true); // Связаны ли RAM и vCPU по долям

  // Поля ввода
  let editRamPercent = $state(0);
  let editRamGb = $state(0);
  let editRamMb = $state(0);

  let editVcorePercent = $state(0);
  let editVcores = $state(0);

  let editMaxRamPercent = $state(0);
  let editMaxRamGb = $state(0);
  let editMaxRamMb = $state(0);

  let editMaxVcorePercent = $state(0);
  let editMaxVcores = $state(0);

  let editState = $state<'RUNNING' | 'STOPPED'>('RUNNING');
  let editType = $state<'fixed' | 'elastic'>('elastic');

  let editUserLimitFactor = $state(1.0);
  let editOrderingPolicy = $state<'fifo' | 'fair'>('fifo');

  let editMaxApplications = $state<number | null>(null);
  let editMaxAmPercent = $state<number | null>(null);
  let editMaxParallelApps = $state<number | null>(null);
  let editMaxLifetime = $state<number | null>(null);

  // Node Labels
  let editAccessibleLabels = $state<string[]>([]);
  let editDefaultLabelExpression = $state<string>('');

  const totalMem = $derived(clusterResources?.memory_mb || 2097152);
  const totalCores = $derived(clusterResources?.vcores || 1024);

  let lastOpenedPath = $state<string | null>(null);

  function parseVal(val: any, fallback = 0): number {
    if (val === '' || val === null || val === undefined) return fallback;
    const n = parseFloat(val);
    return isNaN(n) ? fallback : n;
  }

  function initForm() {
    if (!queue) return;
    const draft = draftItem;
    const part = draft
      ? (draft.partitions[selectedPartition] || draft.partitions['DEFAULT'])
      : (queue.partitions[selectedPartition] || queue.partitions['DEFAULT']);

    const activeMode = draft?.resource_mode || queue.resource_mode || resourceMode || 'percentage';
    inputMode = activeMode === 'absolute' ? 'absolute' : 'percentage';

    if (part) {
      const cap = part.memory_percent ?? part.capacity;
      const vcap = part.vcore_percent ?? part.capacity;
      const maxCap = part.max_memory_percent ?? part.max_capacity;
      const maxVcap = part.max_vcore_percent ?? part.max_capacity;

      editRamPercent = cap;
      editRamMb = part.memory_mb ?? Math.round(totalMem * (cap / 100));
      editRamGb = mbToGb(editRamMb);

      editVcorePercent = vcap;
      editVcores = part.vcores ?? Math.round(totalCores * (vcap / 100));

      editMaxRamPercent = maxCap;
      editMaxRamMb = part.max_memory_mb ?? Math.round(totalMem * (maxCap / 100));
      editMaxRamGb = mbToGb(editMaxRamMb);

      editMaxVcorePercent = maxVcap;
      editMaxVcores = part.max_vcores ?? Math.round(totalCores * (maxVcap / 100));

      editType = part.is_elastic ? 'elastic' : 'fixed';
      isLinked = Math.abs(cap - vcap) < 0.01;
    }
    editState = (draft?.state || queue.state) as 'RUNNING' | 'STOPPED';

    editUserLimitFactor = draft?.user_limit_factor ?? queue.user_limit_factor ?? 1.0;
    const policy = (draft?.ordering_policy || queue.ordering_policy || 'fifo').toLowerCase();
    editOrderingPolicy = policy === 'fair' ? 'fair' : 'fifo';

    editMaxApplications = draft?.max_applications ?? queue.max_applications ?? null;
    editMaxAmPercent = draft?.max_am_resource_percent ?? queue.max_am_resource_percent ?? null;
    editMaxParallelApps = draft?.max_parallel_apps ?? queue.max_parallel_apps ?? null;
    editMaxLifetime = draft?.max_application_lifetime ?? queue.max_application_lifetime ?? null;

    // Инициализация Node Labels
    const initialLabels = draft?.accessible_node_labels ?? queue.accessible_node_labels ?? [];
    editAccessibleLabels = [...initialLabels];
    editDefaultLabelExpression = draft?.default_node_label_expression ?? queue.default_node_label_expression ?? '';
  }

  // Заполняем поля только при открытии Drawer или смене очереди
  $effect(() => {
    if (isOpen && queue) {
      if (lastOpenedPath !== queue.path) {
        lastOpenedPath = queue.path;
        initForm();
      }
    } else if (!isOpen) {
      lastOpenedPath = null;
    }
  });

  // Обновление RAM %
  function updateRamPercent(val: number) {
    editRamPercent = val;
    editRamMb = Math.round(totalMem * (val / 100));
    editRamGb = mbToGb(editRamMb);
    if (isLinked) {
      editVcorePercent = val;
      editVcores = Math.round(totalCores * (val / 100));
    }
    if (editType === 'fixed') {
      editMaxRamPercent = editRamPercent;
      editMaxRamMb = editRamMb;
      editMaxRamGb = editRamGb;
      if (isLinked) {
        editMaxVcorePercent = editVcorePercent;
        editMaxVcores = editVcores;
      }
    }
  }

  // Обновление RAM GB
  function updateRamGb(gb: number) {
    editRamGb = gb;
    editRamMb = gbToMb(gb);
    editRamPercent = totalMem > 0 ? Math.round((editRamMb / totalMem) * 1000) / 10 : 0;
    if (isLinked) {
      editVcorePercent = editRamPercent;
      editVcores = Math.round(totalCores * (editVcorePercent / 100));
    }
    if (editType === 'fixed') {
      editMaxRamGb = editRamGb;
      editMaxRamMb = editRamMb;
      editMaxRamPercent = editRamPercent;
      if (isLinked) {
        editMaxVcorePercent = editVcorePercent;
        editMaxVcores = editVcores;
      }
    }
  }

  // Обновление vCPU %
  function updateVcorePercent(val: number) {
    editVcorePercent = val;
    editVcores = Math.round(totalCores * (val / 100));
    if (isLinked) {
      editRamPercent = val;
      editRamMb = Math.round(totalMem * (val / 100));
      editRamGb = mbToGb(editRamMb);
    }
    if (editType === 'fixed') {
      editMaxVcorePercent = editVcorePercent;
      editMaxVcores = editVcores;
      if (isLinked) {
        editMaxRamPercent = editRamPercent;
        editMaxRamMb = editRamMb;
        editMaxRamGb = editRamGb;
      }
    }
  }

  // Обновление vCPU Cores
  function updateVcores(cores: number) {
    editVcores = cores;
    editVcorePercent = totalCores > 0 ? Math.round((cores / totalCores) * 1000) / 10 : 0;
    if (isLinked) {
      editRamPercent = editVcorePercent;
      editRamMb = Math.round(totalMem * (editRamPercent / 100));
      editRamGb = mbToGb(editRamMb);
    }
    if (editType === 'fixed') {
      editMaxVcorePercent = editVcorePercent;
      editMaxVcores = editVcores;
      if (isLinked) {
        editMaxRamPercent = editRamPercent;
        editMaxRamMb = editRamMb;
        editMaxRamGb = editRamGb;
      }
    }
  }

  // Обновление Max RAM %
  function updateMaxRamPercent(val: number) {
    editMaxRamPercent = val;
    editMaxRamMb = Math.round(totalMem * (val / 100));
    editMaxRamGb = mbToGb(editMaxRamMb);
    if (isLinked) {
      editMaxVcorePercent = val;
      editMaxVcores = Math.round(totalCores * (val / 100));
    }
  }

  // Обновление Max RAM GB
  function updateMaxRamGb(gb: number) {
    editMaxRamGb = gb;
    editMaxRamMb = gbToMb(gb);
    editMaxRamPercent = totalMem > 0 ? Math.round((editMaxRamMb / totalMem) * 1000) / 10 : 0;
    if (isLinked) {
      editMaxVcorePercent = editMaxRamPercent;
      editMaxVcores = Math.round(totalCores * (editMaxVcorePercent / 100));
    }
  }

  // Обновление Max vCPU %
  function updateMaxVcorePercent(val: number) {
    editMaxVcorePercent = val;
    editMaxVcores = Math.round(totalCores * (val / 100));
    if (isLinked) {
      editMaxRamPercent = val;
      editMaxRamMb = Math.round(totalMem * (val / 100));
      editMaxRamGb = mbToGb(editMaxRamMb);
    }
  }

  // Обновление Max vCPU Cores
  function updateMaxVcores(cores: number) {
    editMaxVcores = cores;
    editMaxVcorePercent = totalCores > 0 ? Math.round((cores / totalCores) * 1000) / 10 : 0;
    if (isLinked) {
      editMaxRamPercent = editMaxVcorePercent;
      editMaxRamMb = Math.round(totalMem * (editMaxRamPercent / 100));
      editMaxRamGb = mbToGb(editMaxRamMb);
    }
  }

  function handleTypeChange(e: Event) {
    const val = (e.currentTarget as HTMLSelectElement).value as 'fixed' | 'elastic';
    editType = val;
    if (val === 'fixed') {
      editMaxRamPercent = editRamPercent;
      editMaxRamMb = editRamMb;
      editMaxRamGb = editRamGb;
      editMaxVcorePercent = editVcorePercent;
      editMaxVcores = editVcores;
    }
  }

  function handleSave() {
    if (!queue) return;

    const newPart: PartitionResourceConfig = {
      partition_name: selectedPartition,
      capacity: editRamPercent,
      max_capacity: editMaxRamPercent,
      is_elastic: editMaxRamPercent > editRamPercent || editMaxVcorePercent > editVcorePercent,
      elasticity_ratio: editRamPercent > 0 ? Math.round((editMaxRamPercent / editRamPercent) * 100) / 100 : 1,
      memory_mb: editRamMb,
      vcores: editVcores,
      max_memory_mb: editMaxRamMb,
      max_vcores: editMaxVcores,
      memory_percent: editRamPercent,
      vcore_percent: editVcorePercent,
      max_memory_percent: editMaxRamPercent,
      max_vcore_percent: editMaxVcorePercent,
      absolute_resources: {
        memory_mb: editRamMb,
        vcores: editVcores,
      },
      absolute_max_resources: {
        memory_mb: editMaxRamMb,
        vcores: editMaxVcores,
      }
    };

    const existingPartitions = draftItem?.partitions || { ...queue.partitions };
    const updatedPartitions = { ...existingPartitions, [selectedPartition]: newPart };

    const draft: DraftQueueItem = {
      path: queue.path,
      name: queue.name,
      parent_path: queue.parent_path || undefined,
      action: draftItem?.action || 'modify',
      is_leaf: queue.is_leaf,
      state: editState,
      resource_mode: inputMode,
      user_limit_factor: queue.is_leaf ? editUserLimitFactor : undefined,
      ordering_policy: queue.is_leaf ? editOrderingPolicy : undefined,
      max_applications: editMaxApplications !== null && !isNaN(editMaxApplications) ? editMaxApplications : undefined,
      max_am_resource_percent: editMaxAmPercent !== null && !isNaN(editMaxAmPercent) ? editMaxAmPercent : undefined,
      max_parallel_apps: editMaxParallelApps !== null && !isNaN(editMaxParallelApps) ? editMaxParallelApps : undefined,
      max_application_lifetime: editMaxLifetime !== null && !isNaN(editMaxLifetime) ? editMaxLifetime : undefined,
      accessible_node_labels: editAccessibleLabels.length > 0 ? editAccessibleLabels : undefined,
      default_node_label_expression: editDefaultLabelExpression ? editDefaultLabelExpression : undefined,
      partitions: updatedPartitions,
    };

    onSave(draft);
    isOpen = false;
  }

  function handleReset() {
    if (!queue) return;
    const activeMode = queue.resource_mode || resourceMode || 'percentage';
    inputMode = activeMode === 'absolute' ? 'absolute' : 'percentage';

    editUserLimitFactor = queue.user_limit_factor ?? 1.0;
    const policy = (queue.ordering_policy || 'fifo').toLowerCase();
    editOrderingPolicy = policy === 'fair' ? 'fair' : 'fifo';

    editMaxApplications = queue.max_applications ?? null;
    editMaxAmPercent = queue.max_am_resource_percent ?? null;
    editMaxParallelApps = queue.max_parallel_apps ?? null;
    editMaxLifetime = queue.max_application_lifetime ?? null;

    const part = queue.partitions[selectedPartition] || queue.partitions['DEFAULT'];
    if (part) {
      const cap = part.memory_percent ?? part.capacity;
      const vcap = part.vcore_percent ?? part.capacity;
      const maxCap = part.max_memory_percent ?? part.max_capacity;
      const maxVcap = part.max_vcore_percent ?? part.max_capacity;

      editRamPercent = cap;
      editRamMb = part.memory_mb ?? Math.round(totalMem * (cap / 100));
      editRamGb = mbToGb(editRamMb);

      editVcorePercent = vcap;
      editVcores = part.vcores ?? Math.round(totalCores * (vcap / 100));

      editMaxRamPercent = maxCap;
      editMaxRamMb = part.max_memory_mb ?? Math.round(totalMem * (maxCap / 100));
      editMaxRamGb = mbToGb(editMaxRamMb);

      editMaxVcorePercent = maxVcap;
      editMaxVcores = part.max_vcores ?? Math.round(totalCores * (maxVcap / 100));

      editType = part.is_elastic ? 'elastic' : 'fixed';
      isLinked = Math.abs(cap - vcap) < 0.01;
    }
    editState = queue.state as 'RUNNING' | 'STOPPED';

    const initialLabels = queue.accessible_node_labels ?? [];
    editAccessibleLabels = [...initialLabels];
    editDefaultLabelExpression = queue.default_node_label_expression ?? '';
  }
  const origMode = $derived(queue?.resource_mode || resourceMode || 'percentage');
  const isModeChanged = $derived(inputMode !== origMode);
</script>

{#if isOpen && queue}
  <!-- Backdrop -->
  <div
    class="fixed inset-0 bg-black/20 z-40"
    onclick={() => isOpen = false}
    role="button"
    tabindex="-1"
    onkeydown={() => {}}
  ></div>

  <!-- Drawer -->
  <div class="fixed right-0 top-0 h-full w-[470px] bg-white border-l border-slate-200 shadow-2xl z-50 flex flex-col">
    <!-- Header -->
    <div class="flex items-center justify-between px-5 py-4 border-b border-slate-200 bg-slate-50/50">
      <div>
        <div class="flex items-center gap-2">
          <h2 class="text-sm font-bold text-slate-900">Настройки ресурсов очереди</h2>
          {#if inputMode === 'absolute'}
            <span class="inline-flex items-center text-[10px] px-1.5 py-0.5 rounded font-mono font-bold bg-purple-100 text-purple-700 border border-purple-200">
              ABS
            </span>
          {:else}
            <span class="inline-flex items-center text-[10px] px-1.5 py-0.5 rounded font-mono font-bold bg-sky-100 text-sky-700 border border-sky-200">
              %
            </span>
          {/if}
        </div>
        <p class="text-[11px] text-slate-500 font-mono mt-0.5">{queue.path}</p>
      </div>
      <button
        onclick={() => isOpen = false}
        class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-200 text-slate-500 transition cursor-pointer"
      >
        <X class="w-4 h-4" />
      </button>
    </div>

    {#if queue.path === 'root'}
      <div class="px-5 py-2.5 bg-amber-50 border-b border-amber-200 flex items-center gap-2 text-xs text-amber-800">
        <AlertCircle class="w-4 h-4 text-amber-600 shrink-0" />
        <span>Корневая очередь <strong>root</strong> всегда имеет 100% ресурсов кластера. Дочерние очереди делят её ресурсы.</span>
      </div>
    {/if}

    <!-- Mode Switcher & Tools -->
    <div class="px-5 py-2.5 bg-slate-100/60 border-b border-slate-200 flex items-center justify-between">
      <div class="flex items-center gap-1 bg-white p-0.5 rounded-lg border border-slate-200 shadow-xs">
        <button
          onclick={() => inputMode = 'percentage'}
          class="flex items-center gap-1 px-2.5 py-1 rounded text-xs font-semibold transition cursor-pointer {
            inputMode === 'percentage' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 hover:text-slate-900'
          }"
        >
          <Percent class="w-3 h-3" />
          <span>Проценты (%)</span>
        </button>
        <button
          onclick={() => inputMode = 'absolute'}
          class="flex items-center gap-1 px-2.5 py-1 rounded text-xs font-semibold transition cursor-pointer {
            inputMode === 'absolute' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 hover:text-slate-900'
          }"
        >
          <Hash class="w-3 h-3" />
          <span>Абсолютные (GB / CPU)</span>
        </button>
      </div>

      <button
        onclick={() => isLinked = !isLinked}
        title={isLinked ? 'RAM и vCPU синхронизированы (нажмите для раздельного ввода)' : 'RAM и vCPU настраиваются независимо (нажмите для связывания)'}
        class="flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-medium border transition cursor-pointer {
          isLinked ? 'bg-indigo-50 border-indigo-200 text-indigo-700' : 'bg-slate-100 border-slate-300 text-slate-600'
        }"
      >
        {#if isLinked}
          <Link class="w-3.5 h-3.5 text-indigo-600" />
          <span>Связаны (RAM = vCPU)</span>
        {:else}
          <Unlink class="w-3.5 h-3.5 text-slate-500" />
          <span>Раздельно</span>
        {/if}
      </button>
    </div>

    {#if isModeChanged}
      <div class="px-5 py-2 bg-amber-50 border-b border-amber-200 flex items-center justify-between text-xs text-amber-900">
        <span>Режим изменен: <strong class="font-semibold">{origMode === 'absolute' ? 'Абсолютный' : 'Процентный'}</strong> → <strong class="font-semibold">{inputMode === 'absolute' ? 'Абсолютный (GB/CPU)' : 'Процентный (%)'}</strong></span>
        <span class="text-[10px] font-bold bg-amber-200/80 text-amber-900 px-1.5 py-0.5 rounded">В черновике</span>
      </div>
    {/if}

    <!-- Form Body -->
    <div class="flex-1 overflow-auto px-5 py-4 space-y-5">
      
      <!-- Guaranteed Capacity Section -->
      <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-xs space-y-3.5">
        <div class="flex items-center justify-between border-b border-slate-100 pb-2">
          <span class="text-xs font-bold text-slate-800 uppercase tracking-wide">Гарантированная емкость (Capacity)</span>
          <span class="text-[10px] text-slate-400">Мин. гарантированная доля</span>
        </div>

        <!-- RAM Guaranteed -->
        <div>
          <div class="flex items-center justify-between text-xs font-semibold text-slate-700 mb-1">
            <span class="flex items-center gap-1 text-indigo-700">
              <HardDrive class="w-3.5 h-3.5" />
              <span>Память (RAM)</span>
            </span>
            <span class="text-[11px] font-mono text-slate-500">
              {#if inputMode === 'percentage'}
                ≈ {editRamGb.toFixed(1)} GB ({editRamMb} MB)
              {:else}
                = {editRamPercent.toFixed(1)}%
              {/if}
            </span>
          </div>

          {#if inputMode === 'percentage'}
            <div class="relative">
              <input
                type="number"
                value={editRamPercent}
                oninput={(e) => updateRamPercent(parseVal(e.currentTarget.value, 0))}
                min="0"
                max="100"
                step="0.1"
                class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/20"
              />
              <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">%</span>
            </div>
          {:else}
            <div class="relative">
              <input
                type="number"
                value={editRamGb}
                oninput={(e) => updateRamGb(parseVal(e.currentTarget.value, 0))}
                min="0"
                max={mbToGb(totalMem)}
                step="1"
                class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/20"
              />
              <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">GB</span>
            </div>
          {/if}
        </div>

        <!-- vCPU Guaranteed -->
        <div>
          <div class="flex items-center justify-between text-xs font-semibold text-slate-700 mb-1">
            <span class="flex items-center gap-1 text-blue-700">
              <Cpu class="w-3.5 h-3.5" />
              <span>Процессор (vCPU)</span>
              {#if isLinked}
                <span class="text-[10px] text-indigo-500 font-normal">(синхронизируется с RAM)</span>
              {/if}
            </span>
            <span class="text-[11px] font-mono text-slate-500">
              {#if inputMode === 'percentage'}
                ≈ {editVcores} Cores
              {:else}
                = {editVcorePercent.toFixed(1)}%
              {/if}
            </span>
          </div>

          {#if inputMode === 'percentage'}
            <div class="relative">
              <input
                type="number"
                value={editVcorePercent}
                oninput={(e) => updateVcorePercent(parseVal(e.currentTarget.value, 0))}
                min="0"
                max="100"
                step="0.1"
                class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20"
              />
              <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">%</span>
            </div>
          {:else}
            <div class="relative">
              <input
                type="number"
                value={editVcores}
                oninput={(e) => updateVcores(parseVal(e.currentTarget.value, 0))}
                min="0"
                max={totalCores}
                step="1"
                class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20"
              />
              <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">Cores</span>
            </div>
          {/if}
        </div>
      </div>

      <!-- Max Capacity (Burst Limit) -->
      <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-xs space-y-3.5">
        <div class="flex items-center justify-between border-b border-slate-100 pb-2">
          <div class="flex items-center gap-2">
            <span class="text-xs font-bold text-slate-800 uppercase tracking-wide">Макс. лимит (Max Capacity)</span>
            <select
              value={editType}
              onchange={handleTypeChange}
              class="px-2 py-0.5 rounded text-[11px] font-semibold border border-slate-300 bg-slate-50 cursor-pointer"
            >
              <option value="elastic">Elastic (разрешить burst)</option>
              <option value="fixed">Fixed (строго = capacity)</option>
            </select>
          </div>
        </div>

        {#if editType === 'elastic'}
          <!-- Max RAM -->
          <div>
            <div class="flex items-center justify-between text-xs font-semibold text-slate-700 mb-1">
              <span class="flex items-center gap-1 text-indigo-700">
                <HardDrive class="w-3.5 h-3.5" />
                <span>Max RAM</span>
              </span>
              <span class="text-[11px] font-mono text-slate-500">
                {#if inputMode === 'percentage'}
                  ≈ {editMaxRamGb.toFixed(1)} GB
                {:else}
                  = {editMaxRamPercent.toFixed(1)}%
                {/if}
              </span>
            </div>

            {#if inputMode === 'percentage'}
              <div class="relative">
                <input
                  type="number"
                  value={editMaxRamPercent}
                  oninput={(e) => updateMaxRamPercent(parseVal(e.currentTarget.value, 0))}
                  min={editRamPercent}
                  max="100"
                  step="0.1"
                  class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-indigo-500"
                />
                <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">%</span>
              </div>
            {:else}
              <div class="relative">
                <input
                  type="number"
                  value={editMaxRamGb}
                  oninput={(e) => updateMaxRamGb(parseVal(e.currentTarget.value, 0))}
                  min={editRamGb}
                  max={mbToGb(totalMem)}
                  step="1"
                  class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-indigo-500"
                />
                <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">GB</span>
              </div>
            {/if}
          </div>

          <!-- Max vCPU -->
          <div>
            <div class="flex items-center justify-between text-xs font-semibold text-slate-700 mb-1">
              <span class="flex items-center gap-1 text-blue-700">
                <Cpu class="w-3.5 h-3.5" />
                <span>Max vCPU</span>
                {#if isLinked}
                  <span class="text-[10px] text-indigo-500 font-normal">(синхронизируется с RAM)</span>
                {/if}
              </span>
              <span class="text-[11px] font-mono text-slate-500">
                {#if inputMode === 'percentage'}
                  ≈ {editMaxVcores} Cores
                {:else}
                  = {editMaxVcorePercent.toFixed(1)}%
                {/if}
              </span>
            </div>

            {#if inputMode === 'percentage'}
              <div class="relative">
                <input
                  type="number"
                  value={editMaxVcorePercent}
                  oninput={(e) => updateMaxVcorePercent(parseVal(e.currentTarget.value, 0))}
                  min={editVcorePercent}
                  max="100"
                  step="0.1"
                  class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-blue-500"
                />
                <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">%</span>
              </div>
            {:else}
              <div class="relative">
                <input
                  type="number"
                  value={editMaxVcores}
                  oninput={(e) => updateMaxVcores(parseVal(e.currentTarget.value, 0))}
                  min={editVcores}
                  max={totalCores}
                  step="1"
                  class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono text-slate-900 outline-none focus:border-blue-500"
                />
                <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">Cores</span>
              </div>
            {/if}
          </div>

          {#if editRamPercent > 0}
            <div class="bg-sky-50 border border-sky-100 rounded-lg p-2 text-[11px] text-sky-800 flex justify-between">
              <span>Коэффициент эластичности RAM:</span>
              <span class="font-bold">{(editMaxRamPercent / editRamPercent).toFixed(2)}x</span>
            </div>
          {/if}
        {:else}
          <div class="text-xs text-slate-500 italic p-2 bg-slate-50 rounded border border-slate-100">
            Очередь работает в фиксированном режиме. Потребление жестко ограничено гарантированной емкостью.
          </div>
        {/if}
      </div>

      <!-- User Limits & Ordering Policy (Leaf Queues Only) -->
      {#if queue.is_leaf}
        <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-xs space-y-3">
          <div class="flex items-center justify-between border-b border-slate-100 pb-2">
            <div class="flex items-center gap-1.5">
              <Users class="w-4 h-4 text-sky-600" />
              <span class="text-xs font-bold text-slate-800">Политика планирования и лимиты пользователей</span>
            </div>
            <span class="text-[10px] px-1.5 py-0.5 rounded font-mono font-bold bg-slate-100 text-slate-600">Leaf Queue</span>
          </div>

          <!-- Ordering Policy -->
          <div>
            <div class="text-xs font-semibold text-slate-700 mb-1.5">
              Ordering Policy (политика внутри очереди)
            </div>
            <div class="grid grid-cols-2 gap-2">
              <button
                type="button"
                onclick={() => editOrderingPolicy = 'fifo'}
                class="flex flex-col items-start p-2 rounded-lg border text-left transition cursor-pointer {
                  editOrderingPolicy === 'fifo' ? 'bg-sky-50 border-sky-300 text-sky-900 ring-1 ring-sky-400' : 'bg-slate-50 border-slate-200 text-slate-600 hover:bg-white'
                }"
              >
                <span class="text-xs font-bold font-mono">FIFO</span>
                <span class="text-[10px] text-slate-500 mt-0.5">В порядке поступления</span>
              </button>
              <button
                type="button"
                onclick={() => editOrderingPolicy = 'fair'}
                class="flex flex-col items-start p-2 rounded-lg border text-left transition cursor-pointer {
                  editOrderingPolicy === 'fair' ? 'bg-indigo-50 border-indigo-300 text-indigo-900 ring-1 ring-indigo-400' : 'bg-slate-50 border-slate-200 text-slate-600 hover:bg-white'
                }"
              >
                <span class="text-xs font-bold font-mono">FAIR</span>
                <span class="text-[10px] text-slate-500 mt-0.5">Справедливое разделение</span>
              </button>
            </div>
          </div>

          <!-- User Limit Factor -->
          <div>
            <div class="flex items-center justify-between text-xs font-semibold text-slate-700 mb-1">
              <span>User Limit Factor (ULF)</span>
              <span class="font-mono text-sky-700 font-bold">{editUserLimitFactor.toFixed(1)}x</span>
            </div>
            <div class="flex items-center gap-3">
              <input
                type="range"
                min="0.1"
                max="5.0"
                step="0.1"
                bind:value={editUserLimitFactor}
                class="flex-1 accent-sky-600 cursor-pointer"
              />
              <input
                type="number"
                min="0.1"
                max="10.0"
                step="0.1"
                bind:value={editUserLimitFactor}
                class="w-20 px-2 py-1 text-xs font-mono font-bold text-slate-800 rounded border border-slate-300 text-center outline-none focus:border-sky-500"
              />
            </div>
            <p class="text-[10px] text-slate-500 mt-1">
              {#if editUserLimitFactor <= 1.0}
                Один пользователь может занять не более <strong>{(editUserLimitFactor * 100).toFixed(0)}%</strong> от гарантированной емкости очереди.
              {:else}
                Пользователь может превышать гарантированную емкость очереди до <strong>{editUserLimitFactor.toFixed(1)}x</strong> при наличии свободных ресурсов кластера.
              {/if}
            </p>
          </div>
        </div>
      {/if}

      <!-- Application Limits -->
      <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-xs space-y-3">
        <div class="flex items-center justify-between border-b border-slate-100 pb-2">
          <div class="flex items-center gap-1.5">
            <Layers class="w-4 h-4 text-purple-600" />
            <span class="text-xs font-bold text-slate-800">Лимиты приложений (Application Limits)</span>
          </div>
        </div>

        <div class="grid grid-cols-2 gap-3">
          <!-- Maximum Applications -->
          <div>
            <label for="edit-max-applications" class="block text-[11px] font-semibold text-slate-700 mb-1">
              Max Applications
            </label>
            <input
              id="edit-max-applications"
              type="number"
              placeholder="Не задано"
              value={editMaxApplications ?? ''}
              oninput={(e) => {
                const v = e.currentTarget.value;
                editMaxApplications = v === '' ? null : parseInt(v, 10);
              }}
              min="0"
              step="1"
              class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-900 outline-none focus:border-purple-500"
            />
            <p class="text-[10px] text-slate-400 mt-0.5">Лимит всех приложений (running + pending)</p>
          </div>

          <!-- Max Parallel Apps -->
          <div>
            <label for="edit-max-parallel-apps" class="block text-[11px] font-semibold text-slate-700 mb-1">
              Max Parallel Apps
            </label>
            <input
              id="edit-max-parallel-apps"
              type="number"
              placeholder="Не задано"
              value={editMaxParallelApps ?? ''}
              oninput={(e) => {
                const v = e.currentTarget.value;
                editMaxParallelApps = v === '' ? null : parseInt(v, 10);
              }}
              min="0"
              step="1"
              class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-900 outline-none focus:border-purple-500"
            />
            <p class="text-[10px] text-slate-400 mt-0.5">Лимит параллельно запущенных (running)</p>
          </div>

          <!-- Max AM Resource Percent -->
          <div>
            <label for="edit-max-am-percent" class="block text-[11px] font-semibold text-slate-700 mb-1">
              Max AM Resource %
            </label>
            <div class="relative">
              <input
                id="edit-max-am-percent"
                type="number"
                placeholder="20"
                value={editMaxAmPercent !== null ? (editMaxAmPercent <= 1 ? +(editMaxAmPercent * 100).toFixed(1) : editMaxAmPercent) : ''}
                oninput={(e) => {
                  const v = e.currentTarget.value;
                  editMaxAmPercent = v === '' ? null : (parseFloat(v) / 100);
                }}
                min="0"
                max="100"
                step="1"
                class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-900 outline-none focus:border-purple-500"
              />
              <span class="absolute right-3 top-1.5 text-xs font-bold text-slate-400">%</span>
            </div>
            <p class="text-[10px] text-slate-400 mt-0.5">Макс. доля ресурсов на Application Masters</p>
          </div>

          <!-- Max Application Lifetime -->
          <div>
            <label for="edit-max-lifetime" class="block text-[11px] font-semibold text-slate-700 mb-1">
              Max App Lifetime (сек)
            </label>
            <input
              id="edit-max-lifetime"
              type="number"
              placeholder="Не задано"
              value={editMaxLifetime ?? ''}
              oninput={(e) => {
                const v = e.currentTarget.value;
                editMaxLifetime = v === '' ? null : parseInt(v, 10);
              }}
              min="-1"
              step="1"
              class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-900 outline-none focus:border-purple-500"
            />
            <p class="text-[10px] text-slate-400 mt-0.5">Время жизни (-1 = бессрочно, 86400 = 24ч)</p>
          </div>
        </div>
      </div>

      <!-- Node Labels / Partitioning -->
      <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-xs space-y-3">
        <div class="flex items-center justify-between border-b border-slate-100 pb-2">
          <div class="flex items-center gap-1.5">
            <Layers class="w-4 h-4 text-emerald-600" />
            <span class="text-xs font-bold text-slate-800">Node Labels / Разделы кластера</span>
          </div>
          <span class="text-[10px] px-1.5 py-0.5 rounded font-mono font-bold bg-emerald-50 text-emerald-700 border border-emerald-200">Partitioning</span>
        </div>

        <!-- Accessible Node Labels -->
        <div>
          <span class="block text-xs font-semibold text-slate-700 mb-1.5">
            Доступные метки узлов (Accessible Labels)
          </span>
          {#if partitions.length > 1}
            <div class="flex flex-wrap gap-1.5">
              {#each partitions.filter(p => p !== 'DEFAULT') as partName}
                {@const isChecked = editAccessibleLabels.includes(partName) || editAccessibleLabels.includes('*')}
                <button
                  type="button"
                  onclick={() => {
                    if (editAccessibleLabels.includes(partName)) {
                      editAccessibleLabels = editAccessibleLabels.filter(l => l !== partName);
                    } else {
                      editAccessibleLabels = [...editAccessibleLabels.filter(l => l !== '*'), partName];
                    }
                  }}
                  class="flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-mono font-medium transition cursor-pointer border {
                    isChecked
                      ? 'bg-emerald-50 text-emerald-800 border-emerald-300 ring-1 ring-emerald-400'
                      : 'bg-slate-50 text-slate-600 border-slate-200 hover:bg-slate-100'
                  }"
                >
                  <span class="w-1.5 h-1.5 rounded-full {isChecked ? 'bg-emerald-500' : 'bg-slate-300'}"></span>
                  <span>{partName}</span>
                </button>
              {/each}
              <button
                type="button"
                onclick={() => {
                  if (editAccessibleLabels.includes('*')) {
                    editAccessibleLabels = [];
                  } else {
                    editAccessibleLabels = ['*'];
                  }
                }}
                class="flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-mono font-bold transition cursor-pointer border {
                  editAccessibleLabels.includes('*')
                    ? 'bg-purple-50 text-purple-800 border-purple-300 ring-1 ring-purple-400'
                    : 'bg-slate-50 text-slate-600 border-slate-200 hover:bg-slate-100'
                }"
              >
                <span>* (Все метки)</span>
              </button>
            </div>
          {:else}
            <p class="text-[11px] text-slate-500 italic">В кластере настроен только раздел DEFAULT.</p>
          {/if}
        </div>

        <!-- Default Node Label Expression -->
        <div>
          <label for="edit-default-node-label" class="block text-[11px] font-semibold text-slate-700 mb-1">
            Дефолтная метка задач (Default Label Expression)
          </label>
          <input
            id="edit-default-node-label"
            type="text"
            placeholder="например: gpu или оставьте пустым"
            bind:value={editDefaultLabelExpression}
            class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-900 outline-none focus:border-emerald-500"
          />
          <p class="text-[10px] text-slate-400 mt-0.5">Метка нод, назначаемая приложениям по умолчанию при отправке в эту очередь</p>
        </div>
      </div>

      <!-- State -->
      <div>
        <label for="edit-queue-state" class="block text-xs font-semibold text-slate-700 mb-1.5">
          Состояние очереди (State)
        </label>
        <select
          id="edit-queue-state"
          bind:value={editState}
          class="w-full px-3 py-2 rounded-lg border border-slate-200 bg-slate-50 text-sm text-slate-900 outline-none focus:border-sky-500 cursor-pointer"
        >
          <option value="RUNNING">RUNNING (активна)</option>
          <option value="STOPPED">STOPPED (остановлена)</option>
        </select>
      </div>

      <!-- Summary -->
      <div class="bg-slate-50 border border-slate-200 rounded-lg p-3">
        <div class="text-[11px] font-semibold text-slate-500 mb-2 uppercase">Кластерные ресурсы</div>
        <div class="space-y-1 text-xs">
          <div class="flex justify-between"><span class="text-slate-500">Всего RAM в кластере:</span><span class="font-mono font-semibold">{formatMemory(totalMem)}</span></div>
          <div class="flex justify-between"><span class="text-slate-500">Всего vCPU в кластере:</span><span class="font-mono font-semibold">{formatVcores(totalCores)}</span></div>
          <div class="flex justify-between"><span class="text-slate-500">Текущие приложения:</span><span class="font-semibold">{queue.num_active_applications} акт. / {queue.num_pending_applications} в оч.</span></div>
        </div>
      </div>
    </div>

    <!-- Footer -->
    <div class="flex items-center gap-2 px-5 py-3 border-t border-slate-200 bg-slate-50/50">
      <button
        onclick={handleReset}
        class="flex items-center gap-1.5 px-3 py-2 rounded-lg border border-slate-200 text-slate-700 text-xs font-medium hover:bg-white transition cursor-pointer"
      >
        <RotateCcw class="w-3.5 h-3.5" />
        Сбросить к live
      </button>
      <div class="flex-1"></div>
      <button
        onclick={() => isOpen = false}
        class="px-3 py-2 rounded-lg border border-slate-200 text-slate-700 text-xs font-medium hover:bg-white transition cursor-pointer"
      >
        Отмена
      </button>
      <button
        onclick={handleSave}
        class="flex items-center gap-1.5 px-4 py-2 rounded-lg bg-gradient-to-r from-sky-600 to-indigo-600 text-white text-xs font-semibold shadow-md shadow-sky-500/20 hover:shadow-lg transition cursor-pointer"
      >
        <Save class="w-3.5 h-3.5" />
        Сохранить в черновик
      </button>
    </div>
  </div>
{/if}
