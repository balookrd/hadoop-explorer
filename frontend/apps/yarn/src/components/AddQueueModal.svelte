<script lang="ts">
  import type { DraftQueueItem, PartitionResourceConfig } from '../types';
  import { X, Plus, HardDrive, Cpu, Percent, Hash, Layers } from 'lucide-svelte';
  import { formatMemory, formatVcores, mbToGb, gbToMb } from '../utils/resourceUtils';

  let {
    parentPath,
    isOpen = $bindable(),
    resourceMode,
    clusterResources = { memory_mb: 2097152, vcores: 1024 },
    selectedPartition,
    partitions = [],
    onConfirm,
  }: {
    parentPath: string;
    isOpen: boolean;
    resourceMode: string;
    clusterResources?: { memory_mb: number; vcores: number };
    selectedPartition: string;
    partitions?: string[];
    onConfirm: (draft: DraftQueueItem) => void;
  } = $props();

  let queueName = $state('');
  let inputMode = $state<'percentage' | 'absolute'>('percentage');

  let accessibleNodeLabels = $state<string[]>([]);
  let defaultLabelExpression = $state<string>('');

  let capacity = $state(10);
  let ramGb = $state(0);
  let vcores = $state(0);

  let maxCapacity = $state(20);
  let maxRamGb = $state(0);
  let maxVcores = $state(0);

  let queueType = $state<'elastic' | 'fixed'>('elastic');
  let userLimitFactor = $state(1.0);
  let orderingPolicy = $state<'fifo' | 'fair'>('fifo');

  let maxApplications = $state<number | null>(null);
  let maxParallelApps = $state<number | null>(null);
  let maxAmPercent = $state<number | null>(null);
  let maxLifetime = $state<number | null>(null);

  let error = $state('');

  const totalMem = $derived(clusterResources?.memory_mb || 2097152);
  const totalCores = $derived(clusterResources?.vcores || 1024);

  let wasOpen = $state(false);

  function parseVal(val: any, fallback = 0): number {
    if (val === '' || val === null || val === undefined) return fallback;
    const n = parseFloat(val);
    return isNaN(n) ? fallback : n;
  }

  $effect(() => {
    if (isOpen && !wasOpen) {
      wasOpen = true;
      inputMode = resourceMode === 'absolute' ? 'absolute' : 'percentage';
      ramGb = mbToGb(Math.round(totalMem * (capacity / 100)));
      vcores = Math.round(totalCores * (capacity / 100));
      maxRamGb = mbToGb(Math.round(totalMem * (maxCapacity / 100)));
      maxVcores = Math.round(totalCores * (maxCapacity / 100));
    } else if (!isOpen) {
      wasOpen = false;
    }
  });

  function updateCapacity(val: number) {
    capacity = val;
    ramGb = mbToGb(Math.round(totalMem * (val / 100)));
    vcores = Math.round(totalCores * (val / 100));
    if (queueType === 'fixed') {
      maxCapacity = val;
      maxRamGb = ramGb;
      maxVcores = vcores;
    }
  }

  function updateRamGb(gb: number) {
    ramGb = gb;
    const memMb = gbToMb(gb);
    capacity = totalMem > 0 ? Math.round((memMb / totalMem) * 1000) / 10 : 0;
    vcores = Math.round(totalCores * (capacity / 100));
    if (queueType === 'fixed') {
      maxCapacity = capacity;
      maxRamGb = ramGb;
      maxVcores = vcores;
    }
  }

  function updateVcores(cores: number) {
    vcores = cores;
    capacity = totalCores > 0 ? Math.round((cores / totalCores) * 1000) / 10 : 0;
    ramGb = mbToGb(Math.round(totalMem * (capacity / 100)));
    if (queueType === 'fixed') {
      maxCapacity = capacity;
      maxRamGb = ramGb;
      maxVcores = vcores;
    }
  }

  function updateMaxCapacity(val: number) {
    maxCapacity = val;
    maxRamGb = mbToGb(Math.round(totalMem * (val / 100)));
    maxVcores = Math.round(totalCores * (val / 100));
  }

  function updateMaxRamGb(gb: number) {
    maxRamGb = gb;
    const memMb = gbToMb(gb);
    maxCapacity = totalMem > 0 ? Math.round((memMb / totalMem) * 1000) / 10 : 0;
    maxVcores = Math.round(totalCores * (maxCapacity / 100));
  }

  function updateMaxVcores(cores: number) {
    maxVcores = cores;
    maxCapacity = totalCores > 0 ? Math.round((cores / totalCores) * 1000) / 10 : 0;
    maxRamGb = mbToGb(Math.round(totalMem * (maxCapacity / 100)));
  }

  function validate(): boolean {
    if (!queueName.trim()) {
      error = 'Имя очереди обязательно';
      return false;
    }
    if (!/^[a-z][a-z0-9_]*$/.test(queueName)) {
      error = 'Только строчные латинские буквы, цифры и символ подчеркивания.';
      return false;
    }
    if (capacity <= 0 || capacity > 100) {
      error = 'Емкость должна быть от 0 до 100%';
      return false;
    }
    error = '';
    return true;
  }

  function handleTypeChange(e: Event) {
    const val = (e.currentTarget as HTMLSelectElement).value as 'elastic' | 'fixed';
    queueType = val;
    if (val === 'fixed') {
      maxCapacity = capacity;
      maxRamGb = ramGb;
      maxVcores = vcores;
    }
  }

  function handleSubmit() {
    if (!validate()) return;

    const actualMaxCap = queueType === 'fixed' ? capacity : maxCapacity;
    const path = `${parentPath}.${queueName}`;
    const memMb = gbToMb(ramGb);
    const maxMemMb = gbToMb(maxRamGb);

    const part: PartitionResourceConfig = {
      partition_name: selectedPartition,
      capacity,
      max_capacity: actualMaxCap,
      is_elastic: actualMaxCap > capacity,
      elasticity_ratio: capacity > 0 ? Math.round((actualMaxCap / capacity) * 100) / 100 : 1,
      memory_mb: memMb,
      vcores: vcores,
      max_memory_mb: maxMemMb,
      max_vcores: maxVcores,
      memory_percent: capacity,
      vcore_percent: capacity,
      max_memory_percent: actualMaxCap,
      max_vcore_percent: actualMaxCap,
      absolute_resources: {
        memory_mb: memMb,
        vcores: vcores,
      },
      absolute_max_resources: {
        memory_mb: maxMemMb,
        vcores: maxVcores,
      }
    };

    const draft: DraftQueueItem = {
      path,
      name: queueName,
      parent_path: parentPath,
      action: 'create',
      is_leaf: true,
      state: 'RUNNING',
      resource_mode: inputMode,
      user_limit_factor: userLimitFactor,
      ordering_policy: orderingPolicy,
      max_applications: maxApplications !== null && !isNaN(maxApplications) ? maxApplications : undefined,
      max_am_resource_percent: maxAmPercent !== null && !isNaN(maxAmPercent) ? maxAmPercent : undefined,
      max_parallel_apps: maxParallelApps !== null && !isNaN(maxParallelApps) ? maxParallelApps : undefined,
      max_application_lifetime: maxLifetime !== null && !isNaN(maxLifetime) ? maxLifetime : undefined,
      accessible_node_labels: accessibleNodeLabels.length > 0 ? accessibleNodeLabels : undefined,
      default_node_label_expression: defaultLabelExpression ? defaultLabelExpression : undefined,
      partitions: { [selectedPartition]: part },
    };

    onConfirm(draft);
    // Reset
    queueName = '';
    accessibleNodeLabels = [];
    defaultLabelExpression = '';
    capacity = 10;
    maxCapacity = 20;
    queueType = 'elastic';
    maxApplications = null;
    maxParallelApps = null;
    maxAmPercent = null;
    maxLifetime = null;
    error = '';
    isOpen = false;
  }
</script>

{#if isOpen}
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-sm p-4">
    <div class="bg-white rounded-2xl shadow-2xl p-6 w-full max-w-lg max-h-[90vh] flex flex-col">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-sm font-bold text-slate-900">Создание новой очереди</h2>
        <button onclick={() => isOpen = false} class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-100 cursor-pointer">
          <X class="w-4 h-4 text-slate-500" />
        </button>
      </div>

      <div class="mb-4 px-3 py-2 bg-slate-50 border border-slate-200 rounded-lg flex justify-between items-center">
        <div>
          <span class="text-[11px] text-slate-500">Родительская очередь:</span>
          <span class="text-xs font-mono font-semibold text-slate-800 ml-1">{parentPath}</span>
        </div>
        <div class="flex items-center gap-1 bg-white p-0.5 rounded border border-slate-200">
          <button
            onclick={() => inputMode = 'percentage'}
            class="px-2.5 py-1 rounded text-xs font-semibold cursor-pointer {
              inputMode === 'percentage' ? 'bg-sky-600 text-white' : 'text-slate-600 hover:text-slate-900'
            }"
          >
            %
          </button>
          <button
            onclick={() => inputMode = 'absolute'}
            class="px-2.5 py-1 rounded text-xs font-semibold cursor-pointer {
              inputMode === 'absolute' ? 'bg-sky-600 text-white' : 'text-slate-600 hover:text-slate-900'
            }"
          >
            GB / Cores
          </button>
        </div>
      </div>

      {#if error}
        <div class="mb-4 p-2.5 rounded-lg bg-red-50 border border-red-200 text-red-700 text-xs">{error}</div>
      {/if}

      <div class="space-y-4 flex-1 overflow-y-auto pr-1">
        <div>
          <label for="new-queue-name" class="block text-xs font-semibold text-slate-700 mb-1">Имя очереди</label>
          <input
            id="new-queue-name"
            type="text"
            bind:value={queueName}
            placeholder="analytics_batch"
            class="w-full px-3 py-2 rounded-lg border border-slate-300 bg-white text-sm text-slate-900 outline-none focus:border-sky-500 font-mono"
          />
        </div>

        <!-- Guaranteed capacity -->
        <div class="p-3 bg-slate-50 border border-slate-200 rounded-xl space-y-2">
          <div class="flex justify-between items-center text-xs font-semibold text-slate-800">
            <span>Гарантированная емкость (Capacity)</span>
            <span class="text-[11px] font-mono text-slate-500">
              {ramGb.toFixed(1)} GB / {vcores} Cores ({capacity.toFixed(1)}%)
            </span>
          </div>

          {#if inputMode === 'percentage'}
            <div class="relative">
              <input
                type="number"
                value={capacity}
                oninput={(e) => updateCapacity(parseVal(e.currentTarget.value, 0))}
                min="0.1"
                max="100"
                step="0.1"
                class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono outline-none focus:border-sky-500"
              />
              <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">%</span>
            </div>
          {:else}
            <div class="grid grid-cols-2 gap-2">
              <div>
                <label for="new-ram-gb" class="block text-[11px] text-slate-500 mb-0.5">RAM (GB)</label>
                <input
                  id="new-ram-gb"
                  type="number"
                  value={ramGb}
                  oninput={(e) => updateRamGb(parseVal(e.currentTarget.value, 0))}
                  min="1"
                  step="1"
                  class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono outline-none focus:border-sky-500"
                />
              </div>
              <div>
                <label for="new-vcores" class="block text-[11px] text-slate-500 mb-0.5">vCPU (Cores)</label>
                <input
                  id="new-vcores"
                  type="number"
                  value={vcores}
                  oninput={(e) => updateVcores(parseVal(e.currentTarget.value, 0))}
                  min="1"
                  step="1"
                  class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono outline-none focus:border-sky-500"
                />
              </div>
            </div>
          {/if}
        </div>

        <div>
          <label for="new-queue-type" class="block text-xs font-semibold text-slate-700 mb-1">Режим эластичности</label>
          <select
            id="new-queue-type"
            value={queueType}
            onchange={handleTypeChange}
            class="w-full px-3 py-2 rounded-lg border border-slate-300 bg-white text-sm outline-none cursor-pointer"
          >
            <option value="elastic">Elastic (разрешить овербукинг)</option>
            <option value="fixed">Fixed (строго по capacity)</option>
          </select>
        </div>

        {#if queueType === 'elastic'}
          <div class="p-3 bg-slate-50 border border-slate-200 rounded-xl space-y-2">
            <div class="flex justify-between items-center text-xs font-semibold text-slate-800">
              <span>Максимальный лимит (Max Capacity)</span>
              <span class="text-[11px] font-mono text-slate-500">
                {maxRamGb.toFixed(1)} GB / {maxVcores} Cores ({maxCapacity.toFixed(1)}%)
              </span>
            </div>

            {#if inputMode === 'percentage'}
              <div class="relative">
                <input
                  type="number"
                  value={maxCapacity}
                  oninput={(e) => updateMaxCapacity(parseVal(e.currentTarget.value, 0))}
                  min={capacity}
                  max="100"
                  step="0.1"
                  class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono outline-none focus:border-sky-500"
                />
                <span class="absolute right-3 top-2 text-xs font-bold text-slate-400">%</span>
              </div>
            {:else}
              <div class="grid grid-cols-2 gap-2">
                <div>
                  <label for="new-max-ram-gb" class="block text-[11px] text-slate-500 mb-0.5">Max RAM (GB)</label>
                  <input
                    id="new-max-ram-gb"
                    type="number"
                    value={maxRamGb}
                    oninput={(e) => updateMaxRamGb(parseVal(e.currentTarget.value, 0))}
                    min={ramGb}
                    step="1"
                    class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono outline-none focus:border-sky-500"
                  />
                </div>
                <div>
                  <label for="new-max-vcores" class="block text-[11px] text-slate-500 mb-0.5">Max vCPU (Cores)</label>
                  <input
                    id="new-max-vcores"
                    type="number"
                    value={maxVcores}
                    oninput={(e) => updateMaxVcores(parseVal(e.currentTarget.value, 0))}
                    min={vcores}
                    step="1"
                    class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-sm font-mono outline-none focus:border-sky-500"
                  />
                </div>
              </div>
            {/if}
          </div>
        {/if}

        <!-- Ordering Policy & User Limit Factor -->
        <div class="grid grid-cols-2 gap-3 pt-2 border-t border-slate-100">
          <div>
            <label for="add-queue-ordering-policy" class="block text-xs font-semibold text-slate-700 mb-1">Ordering Policy</label>
            <select
              id="add-queue-ordering-policy"
              bind:value={orderingPolicy}
              class="w-full px-2.5 py-1.5 rounded-lg border border-slate-300 bg-white text-xs text-slate-800 outline-none focus:border-sky-500 cursor-pointer"
            >
              <option value="fifo">FIFO (в порядке очереди)</option>
              <option value="fair">FAIR (справедливое)</option>
            </select>
          </div>
          <div>
            <label for="add-queue-user-limit-factor" class="block text-xs font-semibold text-slate-700 mb-1">
              User Limit Factor: <span class="font-mono text-sky-700">{userLimitFactor.toFixed(1)}x</span>
            </label>
            <input
              id="add-queue-user-limit-factor"
              type="number"
              min="0.1"
              max="10.0"
              step="0.1"
              bind:value={userLimitFactor}
              class="w-full px-2.5 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-800 outline-none focus:border-sky-500"
            />
          </div>
        </div>

        <!-- Application Limits (Optional) -->
        <div class="pt-2 border-t border-slate-100">
          <div class="flex items-center gap-1.5 mb-2">
            <Layers class="w-3.5 h-3.5 text-purple-600" />
            <span class="text-xs font-semibold text-slate-800">Лимиты приложений (опционально)</span>
          </div>

          <div class="grid grid-cols-2 gap-2">
            <div>
              <label for="add-max-applications" class="block text-[10px] text-slate-500 mb-0.5">Max Applications</label>
              <input
                id="add-max-applications"
                type="number"
                placeholder="10000"
                value={maxApplications ?? ''}
                oninput={(e) => {
                  const v = e.currentTarget.value;
                  maxApplications = v === '' ? null : parseInt(v, 10);
                }}
                min="0"
                step="1"
                class="w-full px-2 py-1 rounded border border-slate-300 bg-white text-xs font-mono text-slate-800 outline-none focus:border-purple-500"
              />
            </div>

            <div>
              <label for="add-max-parallel-apps" class="block text-[10px] text-slate-500 mb-0.5">Max Parallel Apps</label>
              <input
                id="add-max-parallel-apps"
                type="number"
                placeholder="50"
                value={maxParallelApps ?? ''}
                oninput={(e) => {
                  const v = e.currentTarget.value;
                  maxParallelApps = v === '' ? null : parseInt(v, 10);
                }}
                min="0"
                step="1"
                class="w-full px-2 py-1 rounded border border-slate-300 bg-white text-xs font-mono text-slate-800 outline-none focus:border-purple-500"
              />
            </div>

            <div>
              <label for="add-max-am-percent" class="block text-[10px] text-slate-500 mb-0.5">Max AM Resource %</label>
              <div class="relative">
                <input
                  id="add-max-am-percent"
                  type="number"
                  placeholder="20"
                  value={maxAmPercent !== null ? (maxAmPercent <= 1 ? +(maxAmPercent * 100).toFixed(1) : maxAmPercent) : ''}
                  oninput={(e) => {
                    const v = e.currentTarget.value;
                    maxAmPercent = v === '' ? null : (parseFloat(v) / 100);
                  }}
                  min="0"
                  max="100"
                  step="1"
                  class="w-full px-2 py-1 rounded border border-slate-300 bg-white text-xs font-mono text-slate-800 outline-none focus:border-purple-500"
                />
                <span class="absolute right-2 top-1 text-[10px] font-bold text-slate-400">%</span>
              </div>
            </div>

            <div>
              <label for="add-max-lifetime" class="block text-[10px] text-slate-500 mb-0.5">Max App Lifetime (сек)</label>
              <input
                id="add-max-lifetime"
                type="number"
                placeholder="86400"
                value={maxLifetime ?? ''}
                oninput={(e) => {
                  const v = e.currentTarget.value;
                  maxLifetime = v === '' ? null : parseInt(v, 10);
                }}
                min="-1"
                step="1"
                class="w-full px-2 py-1 rounded border border-slate-300 bg-white text-xs font-mono text-slate-800 outline-none focus:border-purple-500"
              />
            </div>
          </div>
        </div>

        <!-- Node Labels / Partitioning -->
        {#if partitions.length > 1}
          <div class="bg-emerald-50/40 border border-emerald-100 rounded-xl p-3 space-y-2.5">
            <div class="flex items-center gap-1.5 text-xs font-bold text-slate-800">
              <Layers class="w-3.5 h-3.5 text-emerald-600" />
              <span>Node Labels (Метки узлов)</span>
            </div>

            <div>
              <span class="block text-[10px] font-semibold text-slate-600 mb-1">Доступные метки</span>
              <div class="flex flex-wrap gap-1">
                {#each partitions.filter(p => p !== 'DEFAULT') as partName}
                  {@const isChecked = accessibleNodeLabels.includes(partName) || accessibleNodeLabels.includes('*')}
                  <button
                    type="button"
                    onclick={() => {
                      if (accessibleNodeLabels.includes(partName)) {
                        accessibleNodeLabels = accessibleNodeLabels.filter(l => l !== partName);
                      } else {
                        accessibleNodeLabels = [...accessibleNodeLabels.filter(l => l !== '*'), partName];
                      }
                    }}
                    class="px-2 py-0.5 rounded text-[10px] font-mono font-medium transition cursor-pointer border {
                      isChecked
                        ? 'bg-emerald-100 text-emerald-800 border-emerald-300 font-bold'
                        : 'bg-white text-slate-600 border-slate-200 hover:bg-slate-50'
                    }"
                  >
                    {partName}
                  </button>
                {/each}
                <button
                  type="button"
                  onclick={() => {
                    accessibleNodeLabels = accessibleNodeLabels.includes('*') ? [] : ['*'];
                  }}
                  class="px-2 py-0.5 rounded text-[10px] font-mono font-bold transition cursor-pointer border {
                    accessibleNodeLabels.includes('*')
                      ? 'bg-purple-100 text-purple-800 border-purple-300'
                      : 'bg-white text-slate-600 border-slate-200 hover:bg-slate-50'
                  }"
                >
                  * (Все)
                </button>
              </div>
            </div>

            <div>
              <label for="add-default-label" class="block text-[10px] font-semibold text-slate-600 mb-0.5">
                Дефолтная метка (Default Label Expression)
              </label>
              <input
                id="add-default-label"
                type="text"
                placeholder="например: gpu"
                bind:value={defaultLabelExpression}
                class="w-full px-2 py-1 rounded border border-slate-300 bg-white text-xs font-mono text-slate-800 outline-none focus:border-emerald-500"
              />
            </div>
          </div>
        {/if}
      </div>

      <div class="flex items-center gap-2 mt-6">
        <button onclick={() => isOpen = false}
          class="flex-1 py-2 rounded-lg border border-slate-200 text-slate-700 text-xs font-medium hover:bg-slate-50 transition cursor-pointer">
          Отмена
        </button>
        <button onclick={handleSubmit}
          class="flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg bg-gradient-to-r from-sky-600 to-indigo-600 text-white text-xs font-semibold shadow-md cursor-pointer">
          <Plus class="w-3.5 h-3.5" />
          Создать очередь
        </button>
      </div>
    </div>
  </div>
{/if}
