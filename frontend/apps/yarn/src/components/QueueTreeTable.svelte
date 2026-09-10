<script lang="ts">
  import type { QueueNode, DraftQueueItem, PartitionResourceConfig } from '../types';
  import { ChevronRight, ChevronDown, Folder, FileText, Plus, Trash2, Pencil, Cpu, HardDrive, Hash, Percent } from 'lucide-svelte';
  import { formatMemory, formatVcores, formatMemoryDelta, formatVcoresDelta } from '../utils/resourceUtils';

  let {
    rootQueue,
    resourceMode,
    displayMode = 'percentage',
    clusterResources,
    selectedPartition,
    canWrite,
    draftChanges,
    onAddChild,
    onDelete,
    onEditQueue,
  }: {
    rootQueue: QueueNode | null;
    resourceMode: string;
    displayMode: 'percentage' | 'absolute';
    clusterResources?: { memory_mb: number; vcores: number };
    selectedPartition: string;
    canWrite: boolean;
    draftChanges: Map<string, DraftQueueItem>;
    onAddChild: (parentPath: string) => void;
    onDelete: (path: string) => void;
    onEditQueue: (queue: QueueNode) => void;
  } = $props();

  let expandedPaths = $state<Set<string>>(new Set(['root', 'root.production', 'root.analytics', 'root.batch', 'root.prod', 'root.dev']));

  interface FlatRow {
    node: QueueNode;
    level: number;
    hasChildren: boolean;
    isExpanded: boolean;
  }

  function flattenTree(node: QueueNode, level: number = 0): FlatRow[] {
    const rows: FlatRow[] = [];
    const hasChildren = node.children.length > 0;
    const isExpanded = expandedPaths.has(node.path);

    rows.push({ node, level, hasChildren, isExpanded });

    if (hasChildren && isExpanded) {
      for (const child of node.children) {
        rows.push(...flattenTree(child, level + 1));
      }
    }
    return rows;
  }

  const flatRows = $derived(rootQueue ? flattenTree(rootQueue) : []);

  function toggleExpand(path: string) {
    const newSet = new Set(expandedPaths);
    if (newSet.has(path)) {
      newSet.delete(path);
    } else {
      newSet.add(path);
    }
    expandedPaths = newSet;
  }

  function getPartition(node: QueueNode): PartitionResourceConfig | undefined {
    return node.partitions[selectedPartition] || node.partitions['DEFAULT'];
  }

  function getDraftPartition(path: string): PartitionResourceConfig | undefined {
    const draft = draftChanges.get(path);
    if (!draft) return undefined;
    return draft.partitions[selectedPartition] || draft.partitions['DEFAULT'];
  }

  function hasDraftChange(path: string): boolean {
    return draftChanges.has(path);
  }

  function formatDelta(live: number, draft: number, suffix: string = '%'): string {
    const delta = draft - live;
    if (Math.abs(delta) < 0.01) return '';
    return delta > 0 ? `+${delta.toFixed(1)}${suffix}` : `${delta.toFixed(1)}${suffix}`;
  }

  function deltaClass(live: number, draft: number): string {
    const delta = draft - live;
    if (Math.abs(delta) < 0.01) return '';
    return delta > 0 ? 'text-emerald-600 font-bold' : 'text-red-600 font-bold';
  }
</script>

<div class="overflow-auto flex-1">
  <table class="w-full text-xs">
    <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800">
      <tr class="text-[11px] text-slate-500 dark:text-slate-400 font-semibold uppercase tracking-wider">
        <th class="text-left px-4 py-2.5 w-full min-w-[260px]">Queue</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Status</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Mode</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Policy</th>
        
        <!-- RAM Capacity -->
        <th class="text-right px-3 py-2.5 min-w-[125px] whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <HardDrive class="w-3.5 h-3.5 text-indigo-500 dark:text-indigo-400" />
            <span>RAM Cap</span>
          </div>
        </th>

        <!-- vCPU Capacity -->
        <th class="text-right px-3 py-2.5 min-w-[125px] whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <Cpu class="w-3.5 h-3.5 text-blue-500 dark:text-blue-400" />
            <span>vCPU Cap</span>
          </div>
        </th>

        <!-- RAM Max -->
        <th class="text-right px-3 py-2.5 min-w-[125px] whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <HardDrive class="w-3.5 h-3.5 text-indigo-400 dark:text-indigo-300" />
            <span>RAM Max</span>
          </div>
        </th>

        <!-- vCPU Max -->
        <th class="text-right px-3 py-2.5 min-w-[125px] whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <Cpu class="w-3.5 h-3.5 text-blue-400 dark:text-blue-300" />
            <span>vCPU Max</span>
          </div>
        </th>

        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Elasticity</th>
        <th class="text-left px-3 py-2.5 w-28 whitespace-nowrap">Used</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Apps</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Actions</th>
      </tr>
    </thead>
    <tbody>
      {#each flatRows as row}
        {@const draftItem = draftChanges.get(row.node.path)}
        {@const isNew = draftItem?.action === 'create'}
        {@const isDelete = draftItem?.action === 'delete'}
        {@const isDraft = hasDraftChange(row.node.path)}

        {@const part = isNew
          ? (draftItem?.partitions[selectedPartition] || Object.values(draftItem?.partitions || {})[0])
          : getPartition(row.node)}
        {@const draftPart = isNew ? part : getDraftPartition(row.node.path)}

        {@const totalClusterMem = clusterResources?.memory_mb || 2097152}
        {@const totalClusterCores = clusterResources?.vcores || 1024}

        {@const liveCap = !isNew && part ? (part.memory_percent ?? part.capacity) : 0}
        {@const draftCap = draftPart ? (draftPart.memory_percent ?? draftPart.capacity) : (isDelete ? 0 : liveCap)}

        {@const liveVcore = !isNew && part ? (part.vcore_percent ?? part.capacity) : 0}
        {@const draftVcore = draftPart ? (draftPart.vcore_percent ?? draftPart.capacity) : (isDelete ? 0 : liveVcore)}

        {@const liveMaxCap = !isNew && part ? (part.max_memory_percent ?? part.max_capacity) : 0}
        {@const draftMaxCap = draftPart ? (draftPart.max_memory_percent ?? draftPart.max_capacity) : (isDelete ? 0 : liveMaxCap)}

        {@const liveMaxVcore = !isNew && part ? (part.max_vcore_percent ?? part.max_capacity) : 0}
        {@const draftMaxVcore = draftPart ? (draftPart.max_vcore_percent ?? draftPart.max_capacity) : (isDelete ? 0 : liveMaxVcore)}

        {@const liveMemMb = !isNew && part ? (part.memory_mb ?? Math.round(totalClusterMem * (liveCap / 100))) : 0}
        {@const draftMemMb = draftPart ? (draftPart.memory_mb ?? Math.round(totalClusterMem * (draftCap / 100))) : (isDelete ? 0 : liveMemMb)}

        {@const liveVcoresVal = !isNew && part ? (part.vcores ?? Math.round(totalClusterCores * (liveVcore / 100))) : 0}
        {@const draftVcoresVal = draftPart ? (draftPart.vcores ?? Math.round(totalClusterCores * (draftVcore / 100))) : (isDelete ? 0 : liveVcoresVal)}

        {@const liveMaxMemMb = !isNew && part ? (part.max_memory_mb ?? Math.round(totalClusterMem * (liveMaxCap / 100))) : 0}
        {@const draftMaxMemMb = draftPart ? (draftPart.max_memory_mb ?? Math.round(totalClusterMem * (draftMaxCap / 100))) : (isDelete ? 0 : liveMaxMemMb)}

        {@const liveMaxVcoresVal = !isNew && part ? (part.max_vcores ?? Math.round(totalClusterCores * (liveMaxVcore / 100))) : 0}
        {@const draftMaxVcoresVal = draftPart ? (draftPart.max_vcores ?? Math.round(totalClusterCores * (draftMaxVcore / 100))) : (isDelete ? 0 : liveMaxVcoresVal)}

        {@const liveMode = row.node.resource_mode || resourceMode || 'percentage'}
        {@const draftMode = draftItem?.resource_mode || liveMode}
        {@const isModeChanged = Boolean(draftItem && draftItem.resource_mode && draftItem.resource_mode !== liveMode)}

        <tr
          class="border-b border-slate-100 dark:border-slate-800/60 hover:bg-sky-50/40 dark:hover:bg-sky-950/20 transition {isDraft ? 'bg-amber-50/30 dark:bg-amber-950/20' : ''} {draftItem?.action === 'create' ? 'bg-emerald-50/40 dark:bg-emerald-950/20' : ''} {draftItem?.action === 'delete' ? 'bg-red-50/40 dark:bg-red-950/20 opacity-60' : ''}"
        >
          <!-- Queue Name -->
          <td class="px-4 py-2" style="padding-left: {16 + row.level * 20}px">
            <div class="flex items-center gap-1.5">
              {#if row.hasChildren}
                <button
                  onclick={() => toggleExpand(row.node.path)}
                  class="w-5 h-5 flex items-center justify-center rounded hover:bg-slate-200 dark:hover:bg-slate-800 transition cursor-pointer"
                >
                  {#if row.isExpanded}
                    <ChevronDown class="w-3.5 h-3.5 text-slate-500 dark:text-slate-400" />
                  {:else}
                    <ChevronRight class="w-3.5 h-3.5 text-slate-500 dark:text-slate-400" />
                  {/if}
                </button>
              {:else}
                <span class="w-5 h-5"></span>
              {/if}

              {#if row.node.is_leaf}
                <FileText class="w-4 h-4 text-slate-400 dark:text-slate-500 shrink-0" />
              {:else}
                <Folder class="w-4 h-4 text-sky-500 dark:text-sky-400 shrink-0" />
              {/if}

              <span class="font-semibold text-slate-800 dark:text-slate-200">{row.node.name}</span>
              <span class="text-[10px] text-slate-400 dark:text-slate-500 font-mono">{row.node.path}</span>

              {#if draftItem?.action === 'create'}
                <span class="text-[9px] px-1.5 py-0.2 rounded bg-emerald-100 dark:bg-emerald-950 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800 font-bold">NEW</span>
              {/if}
              {#if draftItem?.action === 'delete'}
                <span class="text-[9px] px-1.5 py-0.2 rounded bg-red-100 dark:bg-red-950 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800 font-bold">DEL</span>
              {/if}

              <!-- Node Labels Badge -->
              {#if (draftItem?.accessible_node_labels ?? row.node.accessible_node_labels)?.length}
                {@const effectiveLabels = draftItem?.accessible_node_labels ?? row.node.accessible_node_labels ?? []}
                <div class="flex items-center gap-1" title="Accessible Node Labels: {effectiveLabels.join(', ')}">
                  {#each effectiveLabels.slice(0, 2) as lbl}
                    <span class="text-[9px] px-1 py-0.2 rounded bg-emerald-50 dark:bg-emerald-950/60 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800 font-mono font-bold">
                      {lbl}
                    </span>
                  {/each}
                  {#if effectiveLabels.length > 2}
                    <span class="text-[9px] text-slate-400 dark:text-slate-500 font-mono font-bold">+{effectiveLabels.length - 2}</span>
                  {/if}
                </div>
              {/if}
            </div>
          </td>

          <!-- Status -->
          <td class="text-center px-3 py-2 whitespace-nowrap">
            <span class="text-[10px] px-2 py-0.5 rounded-full font-bold {
              row.node.state === 'RUNNING' ? 'bg-emerald-100 dark:bg-emerald-950/60 text-emerald-700 dark:text-emerald-300' : 'bg-red-100 dark:bg-red-950/60 text-red-700 dark:text-red-300'
            }">
              {row.node.state}
            </span>
          </td>

          <!-- Mode (Percentage / Absolute) -->
          <td class="text-center px-3 py-2 whitespace-nowrap">
            {#if isModeChanged}
              <button
                onclick={() => onEditQueue(row.node)}
                title="Режим изменен в черновике: {liveMode === 'absolute' ? 'Абсолютный' : 'Процентный'} → {draftMode === 'absolute' ? 'Абсолютный' : 'Процентный'}. Нажмите для редактирования"
                class="inline-flex items-center gap-1 text-[10px] px-2 py-0.5 rounded-md font-mono font-bold bg-amber-100 dark:bg-amber-950/60 text-amber-800 dark:text-amber-300 border border-amber-300 dark:border-amber-800 hover:bg-amber-200 dark:hover:bg-amber-900/60 transition cursor-pointer shadow-2xs"
              >
                <span>{liveMode === 'absolute' ? 'ABS' : '%'}</span>
                <span>→</span>
                <span>{draftMode === 'absolute' ? 'ABS' : '%'}</span>
              </button>
            {:else if draftMode === 'absolute'}
              <button
                onclick={() => onEditQueue(row.node)}
                title="Режим конфигурации: Абсолютные величины (MB / Cores). Нажмите для редактирования"
                class="inline-flex items-center justify-center min-w-[32px] text-[10px] px-2 py-0.5 rounded-md font-mono font-bold bg-purple-50 dark:bg-purple-950/60 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-800 hover:bg-purple-100 dark:hover:bg-purple-900/60 transition cursor-pointer shadow-2xs"
              >
                <span>ABS</span>
              </button>
            {:else}
              <button
                onclick={() => onEditQueue(row.node)}
                title="Режим конфигурации: Проценты (%). Нажмите для редактирования"
                class="inline-flex items-center justify-center min-w-[32px] text-[10px] px-2 py-0.5 rounded-md font-mono font-bold bg-sky-50 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300 border border-sky-200 dark:border-sky-800 hover:bg-sky-100 dark:hover:bg-sky-900/60 transition cursor-pointer shadow-2xs"
              >
                <span>%</span>
              </button>
            {/if}
          </td>

          <!-- Policy & User Limit Factor (Leaf Queues) -->
          <td class="text-center px-3 py-2 whitespace-nowrap">
            {#if row.node.is_leaf}
              {@const activePolicy = (draftItem?.ordering_policy || row.node.ordering_policy || 'fifo').toUpperCase()}
              {@const activeUlf = draftItem?.user_limit_factor ?? row.node.user_limit_factor ?? 1.0}
              <button
                onclick={() => onEditQueue(row.node)}
                class="inline-flex items-center justify-center whitespace-nowrap text-[10px] px-2 py-0.5 rounded-md font-mono font-bold transition cursor-pointer shadow-2xs {
                  activePolicy === 'FAIR' ? 'bg-indigo-50 dark:bg-indigo-950/60 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800 hover:bg-indigo-100 dark:hover:bg-indigo-900/60' : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 border border-slate-200 dark:border-slate-700 hover:bg-slate-200 dark:hover:bg-slate-700'
                }"
                title="Ordering Policy: {activePolicy} | User Limit Factor: {activeUlf.toFixed(1)}x. Нажмите для редактирования"
              >
                {activePolicy}&nbsp;·&nbsp;{activeUlf.toFixed(1)}x
              </button>
            {:else}
              <span class="text-slate-300 dark:text-slate-600 text-xs font-mono">—</span>
            {/if}
          </td>

          <!-- RAM Capacity -->
          <td class="text-right px-3 py-1.5 font-mono whitespace-nowrap">
            {#if part}
              {@const isMemChanged = isDraft && (Math.abs(draftMemMb - liveMemMb) >= 1 || Math.abs(draftCap - liveCap) > 0.01)}
              {#if displayMode === 'percentage'}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftCap.toFixed(1)}%</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatDelta(0, draftCap)})</span>
                  {:else if isDraft && Math.abs(draftCap - liveCap) > 0.01}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{liveCap.toFixed(1)}%</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftCap.toFixed(1)}%</span>
                    <span class="text-[10px] {deltaClass(liveCap, draftCap)} font-mono">({formatDelta(liveCap, draftCap)})</span>
                  {:else}
                    <span class="text-slate-900 dark:text-slate-100 font-semibold">{liveCap.toFixed(1)}%</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{formatMemory(draftMemMb)}</span>
                  {#if isMemChanged && formatMemoryDelta(liveMemMb, draftMemMb)}
                    <span class="font-mono text-[9px] {liveMemMb < draftMemMb ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                      ({formatMemoryDelta(liveMemMb, draftMemMb)})
                    </span>
                  {/if}
                </div>
              {:else}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatMemory(draftMemMb)}</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatMemoryDelta(0, draftMemMb)})</span>
                  {:else if isMemChanged}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{formatMemory(liveMemMb)}</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatMemory(draftMemMb)}</span>
                    {#if formatMemoryDelta(liveMemMb, draftMemMb)}
                      <span class="text-[10px] {liveMemMb < draftMemMb ? 'text-emerald-600 dark:text-emerald-400 font-bold' : 'text-red-600 dark:text-red-400 font-bold'} font-mono">
                        ({formatMemoryDelta(liveMemMb, draftMemMb)})
                      </span>
                    {/if}
                  {:else}
                    <span class="text-slate-900 dark:text-slate-100 font-semibold">{formatMemory(liveMemMb)}</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{draftCap.toFixed(1)}%</span>
                  {#if isDraft && Math.abs(draftCap - liveCap) > 0.01}
                    <span class="font-mono text-[9px] {deltaClass(liveCap, draftCap)}">
                      ({formatDelta(liveCap, draftCap)})
                    </span>
                  {/if}
                </div>
              {/if}
            {/if}
          </td>

          <!-- vCPU Capacity -->
          <td class="text-right px-3 py-1.5 font-mono whitespace-nowrap">
            {#if part}
              {@const isVcoreChanged = isDraft && (Math.abs(draftVcoresVal - liveVcoresVal) >= 0.01 || Math.abs(draftVcore - liveVcore) > 0.01)}
              {#if displayMode === 'percentage'}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftVcore.toFixed(1)}%</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatDelta(0, draftVcore)})</span>
                  {:else if isDraft && Math.abs(draftVcore - liveVcore) > 0.01}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{liveVcore.toFixed(1)}%</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftVcore.toFixed(1)}%</span>
                    <span class="text-[10px] {deltaClass(liveVcore, draftVcore)} font-mono">({formatDelta(liveVcore, draftVcore)})</span>
                  {:else}
                    <span class="text-slate-900 dark:text-slate-100 font-semibold">{liveVcore.toFixed(1)}%</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{formatVcores(draftVcoresVal)}</span>
                  {#if isVcoreChanged && formatVcoresDelta(liveVcoresVal, draftVcoresVal)}
                    <span class="font-mono text-[9px] {liveVcoresVal < draftVcoresVal ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                      ({formatVcoresDelta(liveVcoresVal, draftVcoresVal)})
                    </span>
                  {/if}
                </div>
              {:else}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatVcores(draftVcoresVal)}</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatVcoresDelta(0, draftVcoresVal)})</span>
                  {:else if isVcoreChanged}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{formatVcores(liveVcoresVal)}</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatVcores(draftVcoresVal)}</span>
                    {#if formatVcoresDelta(liveVcoresVal, draftVcoresVal)}
                      <span class="text-[10px] {liveVcoresVal < draftVcoresVal ? 'text-emerald-600 dark:text-emerald-400 font-bold' : 'text-red-600 dark:text-red-400 font-bold'} font-mono">
                        ({formatVcoresDelta(liveVcoresVal, draftVcoresVal)})
                      </span>
                    {/if}
                  {:else}
                    <span class="text-slate-900 dark:text-slate-100 font-semibold">{formatVcores(liveVcoresVal)}</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{draftVcore.toFixed(1)}%</span>
                  {#if isDraft && Math.abs(draftVcore - liveVcore) > 0.01}
                    <span class="font-mono text-[9px] {deltaClass(liveVcore, draftVcore)}">
                      ({formatDelta(liveVcore, draftVcore)})
                    </span>
                  {/if}
                </div>
              {/if}
            {/if}
          </td>

          <!-- RAM Max Capacity -->
          <td class="text-right px-3 py-1.5 font-mono whitespace-nowrap">
            {#if part}
              {@const isMaxMemChanged = isDraft && (Math.abs(draftMaxMemMb - liveMaxMemMb) >= 1 || Math.abs(draftMaxCap - liveMaxCap) > 0.01)}
              {#if displayMode === 'percentage'}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftMaxCap.toFixed(1)}%</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatDelta(0, draftMaxCap)})</span>
                  {:else if isDraft && Math.abs(draftMaxCap - liveMaxCap) > 0.01}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{liveMaxCap.toFixed(1)}%</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftMaxCap.toFixed(1)}%</span>
                    <span class="text-[10px] {deltaClass(liveMaxCap, draftMaxCap)} font-mono">({formatDelta(liveMaxCap, draftMaxCap)})</span>
                  {:else}
                    <span class="text-slate-700 dark:text-slate-300 font-medium">{liveMaxCap.toFixed(1)}%</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{formatMemory(draftMaxMemMb)}</span>
                  {#if isMaxMemChanged && formatMemoryDelta(liveMaxMemMb, draftMaxMemMb)}
                    <span class="font-mono text-[9px] {liveMaxMemMb < draftMaxMemMb ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                      ({formatMemoryDelta(liveMaxMemMb, draftMaxMemMb)})
                    </span>
                  {/if}
                </div>
              {:else}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatMemory(draftMaxMemMb)}</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatMemoryDelta(0, draftMaxMemMb)})</span>
                  {:else if isMaxMemChanged}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{formatMemory(liveMaxMemMb)}</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatMemory(draftMaxMemMb)}</span>
                    {#if formatMemoryDelta(liveMaxMemMb, draftMaxMemMb)}
                      <span class="text-[10px] {liveMaxMemMb < draftMaxMemMb ? 'text-emerald-600 dark:text-emerald-400 font-bold' : 'text-red-600 dark:text-red-400 font-bold'} font-mono">
                        ({formatMemoryDelta(liveMaxMemMb, draftMaxMemMb)})
                      </span>
                    {/if}
                  {:else}
                    <span class="text-slate-700 dark:text-slate-300 font-medium">{formatMemory(liveMaxMemMb)}</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{draftMaxCap.toFixed(1)}%</span>
                  {#if isDraft && Math.abs(draftMaxCap - liveMaxCap) > 0.01}
                    <span class="font-mono text-[9px] {deltaClass(liveMaxCap, draftMaxCap)}">
                      ({formatDelta(liveMaxCap, draftMaxCap)})
                    </span>
                  {/if}
                </div>
              {/if}
            {/if}
          </td>

          <!-- vCPU Max Capacity -->
          <td class="text-right px-3 py-1.5 font-mono whitespace-nowrap">
            {#if part}
              {@const isMaxVcoreChanged = isDraft && (Math.abs(draftMaxVcoresVal - liveMaxVcoresVal) >= 0.01 || Math.abs(draftMaxVcore - liveMaxVcore) > 0.01)}
              {#if displayMode === 'percentage'}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftMaxVcore.toFixed(1)}%</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatDelta(0, draftMaxVcore)})</span>
                  {:else if isDraft && Math.abs(draftMaxVcore - liveMaxVcore) > 0.01}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{liveMaxVcore.toFixed(1)}%</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{draftMaxVcore.toFixed(1)}%</span>
                    <span class="text-[10px] {deltaClass(liveMaxVcore, draftMaxVcore)} font-mono">({formatDelta(liveMaxVcore, draftMaxVcore)})</span>
                  {:else}
                    <span class="text-slate-700 dark:text-slate-300 font-medium">{liveMaxVcore.toFixed(1)}%</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{formatVcores(draftMaxVcoresVal)}</span>
                  {#if isMaxVcoreChanged && formatVcoresDelta(liveMaxVcoresVal, draftMaxVcoresVal)}
                    <span class="font-mono text-[9px] {liveMaxVcoresVal < draftMaxVcoresVal ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}">
                      ({formatVcoresDelta(liveMaxVcoresVal, draftMaxVcoresVal)})
                    </span>
                  {/if}
                </div>
              {:else}
                <div class="flex items-center justify-end gap-1 whitespace-nowrap leading-tight">
                  {#if isNew}
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatVcores(draftMaxVcoresVal)}</span>
                    <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold font-mono">({formatVcoresDelta(0, draftMaxVcoresVal)})</span>
                  {:else if isMaxVcoreChanged}
                    <span class="text-slate-400 dark:text-slate-500 text-[11px] line-through">{formatVcores(liveMaxVcoresVal)}</span>
                    <span class="font-bold text-slate-900 dark:text-slate-100">{formatVcores(draftMaxVcoresVal)}</span>
                    {#if formatVcoresDelta(liveMaxVcoresVal, draftMaxVcoresVal)}
                      <span class="text-[10px] {liveMaxVcoresVal < draftMaxVcoresVal ? 'text-emerald-600 dark:text-emerald-400 font-bold' : 'text-red-600 dark:text-red-400 font-bold'} font-mono">
                        ({formatVcoresDelta(liveMaxVcoresVal, draftMaxVcoresVal)})
                      </span>
                    {/if}
                  {:else}
                    <span class="text-slate-700 dark:text-slate-300 font-medium">{formatVcores(liveMaxVcoresVal)}</span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 dark:text-slate-500 font-sans flex items-center justify-end gap-1 whitespace-nowrap leading-tight mt-0.5">
                  <span>{draftMaxVcore.toFixed(1)}%</span>
                  {#if isDraft && Math.abs(draftMaxVcore - liveMaxVcore) > 0.01}
                    <span class="font-mono text-[9px] {deltaClass(liveMaxVcore, draftMaxVcore)}">
                      ({formatDelta(liveMaxVcore, draftMaxVcore)})
                    </span>
                  {/if}
                </div>
              {/if}
            {/if}
          </td>


          <!-- Elasticity -->
          <td class="text-center px-2 py-2 font-mono text-[11px]">
            {#if part && part.is_elastic}
              <span class="text-sky-700 dark:text-sky-400 font-semibold">{part.elasticity_ratio.toFixed(1)}x</span>
            {:else}
              <span class="text-slate-300 dark:text-slate-600">—</span>
            {/if}
          </td>

          <!-- Used -->
          <td class="px-2 py-2">
            <div class="flex items-center gap-2">
              <div class="flex-1 h-2 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
                <div
                  class="h-full rounded-full transition-all {
                    row.node.current_used_percent > 90 ? 'bg-red-500' :
                    row.node.current_used_percent > 70 ? 'bg-amber-500' : 'bg-sky-500'
                  }"
                  style="width: {Math.min(row.node.current_used_percent, 100)}%"
                ></div>
              </div>
              <span class="text-[11px] font-mono text-slate-600 dark:text-slate-400 w-10 text-right">{row.node.current_used_percent.toFixed(0)}%</span>
            </div>
          </td>

          <!-- Apps -->
          <td class="text-center px-2 py-2">
            <span class="font-semibold text-slate-800 dark:text-slate-200">{row.node.num_active_applications}</span>
            {#if row.node.num_pending_applications > 0}
              <span class="text-[10px] text-amber-600 dark:text-amber-400 ml-0.5">+{row.node.num_pending_applications}</span>
            {/if}
          </td>

          <!-- Actions -->
          <td class="text-center px-2 py-2">
            {#if canWrite}
              <div class="flex items-center justify-center gap-1">
                {#if !row.node.is_leaf || !row.hasChildren}
                  <button
                    onclick={() => onAddChild(row.node.path)}
                    title="Add Child Queue"
                    class="w-6 h-6 flex items-center justify-center rounded hover:bg-emerald-100 dark:hover:bg-emerald-950/50 text-emerald-600 dark:text-emerald-400 transition cursor-pointer"
                  >
                    <Plus class="w-3.5 h-3.5" />
                  </button>
                {/if}
                <button
                  onclick={() => onEditQueue(row.node)}
                  title="Edit Queue"
                  class="w-6 h-6 flex items-center justify-center rounded hover:bg-sky-100 dark:hover:bg-sky-950/50 text-sky-600 dark:text-sky-400 transition cursor-pointer"
                >
                  <Pencil class="w-3.5 h-3.5" />
                </button>
                {#if row.node.path !== 'root'}
                  <button
                    onclick={() => onDelete(row.node.path)}
                    title="Delete Queue"
                    class="w-6 h-6 flex items-center justify-center rounded hover:bg-red-100 dark:hover:bg-red-950/50 text-red-500 dark:text-red-400 transition cursor-pointer"
                  >
                    <Trash2 class="w-3.5 h-3.5" />
                  </button>
                {/if}
              </div>
            {/if}
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>
