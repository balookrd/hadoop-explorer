<script lang="ts">
  import type { BranchBalance } from '../types';
  import { CheckCircle, AlertTriangle, XCircle, HardDrive, Cpu } from 'lucide-svelte';
  import { formatMemory, formatVcores } from '../utils/resourceUtils';

  let {
    balances,
    resourceMode,
    displayMode = 'percentage',
    inline = false,
  }: {
    balances: BranchBalance[];
    resourceMode: string;
    displayMode?: 'percentage' | 'absolute';
    inline?: boolean;
  } = $props();
</script>

{#if balances.length > 0}
  <div class={inline ? "flex items-center gap-2 overflow-x-auto" : "px-4 sm:px-6 py-2 border-b border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900"}>
    <div class="flex items-center gap-2 overflow-x-auto">
      {#each balances as b}
        <div class="flex items-center gap-1.5 px-2.5 py-1 rounded-lg border text-xs shrink-0 {
          b.is_balanced
            ? 'bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200 dark:border-emerald-900 text-emerald-800 dark:text-emerald-300'
            : b.status === 'overallocated'
              ? 'bg-red-50 dark:bg-red-950/40 border-red-200 dark:border-red-900 text-red-800 dark:text-red-300'
              : 'bg-amber-50 dark:bg-amber-950/40 border-amber-200 dark:border-amber-900 text-amber-800 dark:text-amber-300'
        }">
          {#if b.is_balanced}
            <CheckCircle class="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400 shrink-0" />
          {:else if b.status === 'overallocated'}
            <XCircle class="w-3.5 h-3.5 text-red-600 dark:text-red-400 shrink-0" />
          {:else}
            <AlertTriangle class="w-3.5 h-3.5 text-amber-600 dark:text-amber-400 shrink-0" />
          {/if}
          
          <span class="font-mono font-bold text-slate-800 dark:text-slate-200">{b.parent_path}:</span>

          {#if displayMode === 'percentage'}
            <span class="font-semibold">
              {b.total_children_capacity.toFixed(1)}%
              {#if !b.is_balanced}
                <span class="font-normal opacity-80">(остаток: {b.unallocated_capacity.toFixed(1)}%)</span>
              {/if}
            </span>
          {:else}
            <div class="flex items-center gap-2 font-mono">
              {#if b.total_children_memory_mb !== undefined && b.total_children_memory_mb !== null}
                <span class="flex items-center gap-0.5">
                  <HardDrive class="w-3 h-3 text-indigo-500 dark:text-indigo-400" />
                  <span>{formatMemory(b.total_children_memory_mb)}</span>
                </span>
              {/if}
              {#if b.total_children_vcores !== undefined && b.total_children_vcores !== null}
                <span class="flex items-center gap-0.5">
                  <Cpu class="w-3 h-3 text-blue-500 dark:text-blue-400" />
                  <span>{formatVcores(b.total_children_vcores)}</span>
                </span>
              {/if}
              <span class="text-[11px] opacity-75">({b.total_children_capacity.toFixed(1)}%)</span>
            </div>
          {/if}
        </div>
      {/each}
    </div>
  </div>
{/if}
