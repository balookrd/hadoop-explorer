<script lang="ts">
  import type { QueueNode, DraftQueueItem, PartitionResourceConfig } from '../types';
  import { ChevronRight, ChevronDown, Folder, FileText, Plus, Trash2, Pencil, Cpu, HardDrive, Hash, Percent } from 'lucide-svelte';
  import { formatMemory, formatVcores } from '../utils/resourceUtils';

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
    <thead class="sticky top-0 z-10 bg-slate-50 border-b border-slate-200">
      <tr class="text-[11px] text-slate-500 font-semibold uppercase tracking-wider">
        <th class="text-left px-4 py-2.5 w-full min-w-[260px]">Queue</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Status</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Mode</th>
        <th class="text-center px-2 py-2.5 w-1 whitespace-nowrap">Policy</th>
        
        <!-- RAM Capacity -->
        <th class="text-right px-3 py-2.5 w-1 whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <HardDrive class="w-3.5 h-3.5 text-indigo-500" />
            <span>RAM Cap</span>
          </div>
        </th>

        <!-- vCPU Capacity -->
        <th class="text-right px-3 py-2.5 w-1 whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <Cpu class="w-3.5 h-3.5 text-blue-500" />
            <span>vCPU Cap</span>
          </div>
        </th>

        <!-- RAM Max -->
        <th class="text-right px-3 py-2.5 w-1 whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <HardDrive class="w-3.5 h-3.5 text-indigo-400" />
            <span>RAM Max</span>
          </div>
        </th>

        <!-- vCPU Max -->
        <th class="text-right px-3 py-2.5 w-1 whitespace-nowrap">
          <div class="flex items-center justify-end gap-1">
            <Cpu class="w-3.5 h-3.5 text-blue-400" />
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
        {@const part = getPartition(row.node)}
        {@const draftPart = getDraftPartition(row.node.path)}
        {@const isDraft = hasDraftChange(row.node.path)}
        {@const draftItem = draftChanges.get(row.node.path)}

        {@const liveCap = part ? (part.memory_percent ?? part.capacity) : 0}
        {@const draftCap = draftPart ? (draftPart.memory_percent ?? draftPart.capacity) : liveCap}

        {@const liveVcore = part ? (part.vcore_percent ?? part.capacity) : 0}
        {@const draftVcore = draftPart ? (draftPart.vcore_percent ?? draftPart.capacity) : liveVcore}

        {@const liveMaxCap = part ? (part.max_memory_percent ?? part.max_capacity) : 0}
        {@const draftMaxCap = draftPart ? (draftPart.max_memory_percent ?? draftPart.max_capacity) : liveMaxCap}

        {@const liveMaxVcore = part ? (part.max_vcore_percent ?? part.max_capacity) : 0}
        {@const draftMaxVcore = draftPart ? (draftPart.max_vcore_percent ?? draftPart.max_capacity) : liveMaxVcore}

        {@const liveMode = row.node.resource_mode || resourceMode || 'percentage'}
        {@const draftMode = draftItem?.resource_mode || liveMode}
        {@const isModeChanged = Boolean(draftItem && draftItem.resource_mode && draftItem.resource_mode !== liveMode)}

        <tr
          class="border-b border-slate-100 hover:bg-sky-50/40 transition {isDraft ? 'bg-amber-50/30' : ''} {draftItem?.action === 'create' ? 'bg-emerald-50/40' : ''} {draftItem?.action === 'delete' ? 'bg-red-50/40 opacity-60' : ''}"
        >
          <!-- Queue Name -->
          <td class="px-4 py-2" style="padding-left: {16 + row.level * 20}px">
            <div class="flex items-center gap-1.5">
              {#if row.hasChildren}
                <button
                  onclick={() => toggleExpand(row.node.path)}
                  class="w-5 h-5 flex items-center justify-center rounded hover:bg-slate-200 transition cursor-pointer"
                >
                  {#if row.isExpanded}
                    <ChevronDown class="w-3.5 h-3.5 text-slate-500" />
                  {:else}
                    <ChevronRight class="w-3.5 h-3.5 text-slate-500" />
                  {/if}
                </button>
              {:else}
                <span class="w-5 h-5"></span>
              {/if}

              {#if row.node.is_leaf}
                <FileText class="w-4 h-4 text-slate-400 shrink-0" />
              {:else}
                <Folder class="w-4 h-4 text-sky-500 shrink-0" />
              {/if}

              <span class="font-semibold text-slate-800">{row.node.name}</span>
              <span class="text-[10px] text-slate-400 font-mono">{row.node.path}</span>

              {#if draftItem?.action === 'create'}
                <span class="text-[9px] px-1.5 py-0.2 rounded bg-emerald-100 text-emerald-700 border border-emerald-200 font-bold">NEW</span>
              {/if}
              {#if draftItem?.action === 'delete'}
                <span class="text-[9px] px-1.5 py-0.2 rounded bg-red-100 text-red-700 border border-red-200 font-bold">DEL</span>
              {/if}

              <!-- Node Labels Badge -->
              {#if (draftItem?.accessible_node_labels ?? row.node.accessible_node_labels)?.length}
                {@const effectiveLabels = draftItem?.accessible_node_labels ?? row.node.accessible_node_labels ?? []}
                <div class="flex items-center gap-1" title="Accessible Node Labels: {effectiveLabels.join(', ')}">
                  {#each effectiveLabels.slice(0, 2) as lbl}
                    <span class="text-[9px] px-1 py-0.2 rounded bg-emerald-50 text-emerald-700 border border-emerald-200 font-mono font-bold">
                      {lbl}
                    </span>
                  {/each}
                  {#if effectiveLabels.length > 2}
                    <span class="text-[9px] text-slate-400 font-mono font-bold">+{effectiveLabels.length - 2}</span>
                  {/if}
                </div>
              {/if}
            </div>
          </td>

          <!-- Status -->
          <td class="text-center px-3 py-2 whitespace-nowrap">
            <span class="text-[10px] px-2 py-0.5 rounded-full font-bold {
              row.node.state === 'RUNNING' ? 'bg-emerald-100 text-emerald-700' : 'bg-red-100 text-red-700'
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
                class="inline-flex items-center gap-1 text-[10px] px-2 py-0.5 rounded-md font-mono font-bold bg-amber-100 text-amber-800 border border-amber-300 hover:bg-amber-200 transition cursor-pointer shadow-2xs"
              >
                <span>{liveMode === 'absolute' ? 'ABS' : '%'}</span>
                <span>→</span>
                <span>{draftMode === 'absolute' ? 'ABS' : '%'}</span>
              </button>
            {:else if draftMode === 'absolute'}
              <button
                onclick={() => onEditQueue(row.node)}
                title="Режим конфигурации: Абсолютные величины (MB / Cores). Нажмите для редактирования"
                class="inline-flex items-center justify-center min-w-[32px] text-[10px] px-2 py-0.5 rounded-md font-mono font-bold bg-purple-50 text-purple-700 border border-purple-200 hover:bg-purple-100 transition cursor-pointer shadow-2xs"
              >
                <span>ABS</span>
              </button>
            {:else}
              <button
                onclick={() => onEditQueue(row.node)}
                title="Режим конфигурации: Проценты (%). Нажмите для редактирования"
                class="inline-flex items-center justify-center min-w-[32px] text-[10px] px-2 py-0.5 rounded-md font-mono font-bold bg-sky-50 text-sky-700 border border-sky-200 hover:bg-sky-100 transition cursor-pointer shadow-2xs"
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
                  activePolicy === 'FAIR' ? 'bg-indigo-50 text-indigo-700 border border-indigo-200 hover:bg-indigo-100' : 'bg-slate-100 text-slate-600 border border-slate-200 hover:bg-slate-200'
                }"
                title="Ordering Policy: {activePolicy} | User Limit Factor: {activeUlf.toFixed(1)}x. Нажмите для редактирования"
              >
                {activePolicy}&nbsp;·&nbsp;{activeUlf.toFixed(1)}x
              </button>
            {:else}
              <span class="text-slate-300 text-xs font-mono">—</span>
            {/if}
          </td>

          <!-- RAM Capacity -->
          <td class="text-right px-3 py-2 font-mono">
            {#if part}
              {#if displayMode === 'percentage'}
                <div>
                  <span class="text-slate-900 font-semibold">{liveCap.toFixed(1)}%</span>
                  {#if isDraft && draftPart && Math.abs(draftCap - liveCap) > 0.01}
                    <span class="ml-1 text-[10px] {deltaClass(liveCap, draftCap)}">
                      → {draftCap.toFixed(1)}% ({formatDelta(liveCap, draftCap)})
                    </span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {formatMemory(draftPart?.memory_mb ?? part.memory_mb)}
                </div>
              {:else}
                <div>
                  <span class="text-slate-900 font-semibold">{formatMemory(draftPart?.memory_mb ?? part.memory_mb)}</span>
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {draftCap.toFixed(1)}%
                </div>
              {/if}
            {/if}
          </td>

          <!-- vCPU Capacity -->
          <td class="text-right px-3 py-2 font-mono">
            {#if part}
              {#if displayMode === 'percentage'}
                <div>
                  <span class="text-slate-900 font-semibold">{liveVcore.toFixed(1)}%</span>
                  {#if isDraft && draftPart && Math.abs(draftVcore - liveVcore) > 0.01}
                    <span class="ml-1 text-[10px] {deltaClass(liveVcore, draftVcore)}">
                      → {draftVcore.toFixed(1)}% ({formatDelta(liveVcore, draftVcore)})
                    </span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {formatVcores(draftPart?.vcores ?? part.vcores)}
                </div>
              {:else}
                <div>
                  <span class="text-slate-900 font-semibold">{formatVcores(draftPart?.vcores ?? part.vcores)}</span>
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {draftVcore.toFixed(1)}%
                </div>
              {/if}
            {/if}
          </td>

          <!-- RAM Max Capacity -->
          <td class="text-right px-3 py-2 font-mono">
            {#if part}
              {#if displayMode === 'percentage'}
                <div>
                  <span class="text-slate-700">{liveMaxCap.toFixed(1)}%</span>
                  {#if isDraft && draftPart && Math.abs(draftMaxCap - liveMaxCap) > 0.01}
                    <span class="ml-1 text-[10px] {deltaClass(liveMaxCap, draftMaxCap)}">
                      → {draftMaxCap.toFixed(1)}% ({formatDelta(liveMaxCap, draftMaxCap)})
                    </span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {formatMemory(draftPart?.max_memory_mb ?? part.max_memory_mb)}
                </div>
              {:else}
                <div>
                  <span class="text-slate-700">{formatMemory(draftPart?.max_memory_mb ?? part.max_memory_mb)}</span>
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {draftMaxCap.toFixed(1)}%
                </div>
              {/if}
            {/if}
          </td>

          <!-- vCPU Max Capacity -->
          <td class="text-right px-3 py-2 font-mono">
            {#if part}
              {#if displayMode === 'percentage'}
                <div>
                  <span class="text-slate-700">{liveMaxVcore.toFixed(1)}%</span>
                  {#if isDraft && draftPart && Math.abs(draftMaxVcore - liveMaxVcore) > 0.01}
                    <span class="ml-1 text-[10px] {deltaClass(liveMaxVcore, draftMaxVcore)}">
                      → {draftMaxVcore.toFixed(1)}% ({formatDelta(liveMaxVcore, draftMaxVcore)})
                    </span>
                  {/if}
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {formatVcores(draftPart?.max_vcores ?? part.max_vcores)}
                </div>
              {:else}
                <div>
                  <span class="text-slate-700">{formatVcores(draftPart?.max_vcores ?? part.max_vcores)}</span>
                </div>
                <div class="text-[10px] text-slate-400 font-sans">
                  {draftMaxVcore.toFixed(1)}%
                </div>
              {/if}
            {/if}
          </td>

          <!-- Elasticity -->
          <td class="text-center px-2 py-2 font-mono text-[11px]">
            {#if part && part.is_elastic}
              <span class="text-sky-700 font-semibold">{part.elasticity_ratio.toFixed(1)}x</span>
            {:else}
              <span class="text-slate-300">—</span>
            {/if}
          </td>

          <!-- Used -->
          <td class="px-2 py-2">
            <div class="flex items-center gap-2">
              <div class="flex-1 h-2 bg-slate-100 rounded-full overflow-hidden">
                <div
                  class="h-full rounded-full transition-all {
                    row.node.current_used_percent > 90 ? 'bg-red-500' :
                    row.node.current_used_percent > 70 ? 'bg-amber-500' : 'bg-sky-500'
                  }"
                  style="width: {Math.min(row.node.current_used_percent, 100)}%"
                ></div>
              </div>
              <span class="text-[11px] font-mono text-slate-600 w-10 text-right">{row.node.current_used_percent.toFixed(0)}%</span>
            </div>
          </td>

          <!-- Apps -->
          <td class="text-center px-2 py-2">
            <span class="font-semibold text-slate-800">{row.node.num_active_applications}</span>
            {#if row.node.num_pending_applications > 0}
              <span class="text-[10px] text-amber-600 ml-0.5">+{row.node.num_pending_applications}</span>
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
                    class="w-6 h-6 flex items-center justify-center rounded hover:bg-emerald-100 text-emerald-600 transition cursor-pointer"
                  >
                    <Plus class="w-3.5 h-3.5" />
                  </button>
                {/if}
                <button
                  onclick={() => onEditQueue(row.node)}
                  title="Edit Queue"
                  class="w-6 h-6 flex items-center justify-center rounded hover:bg-sky-100 text-sky-600 transition cursor-pointer"
                >
                  <Pencil class="w-3.5 h-3.5" />
                </button>
                {#if row.node.path !== 'root'}
                  <button
                    onclick={() => onDelete(row.node.path)}
                    title="Delete Queue"
                    class="w-6 h-6 flex items-center justify-center rounded hover:bg-red-100 text-red-500 transition cursor-pointer"
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
