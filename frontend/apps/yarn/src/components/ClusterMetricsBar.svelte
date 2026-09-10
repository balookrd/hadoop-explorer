<script lang="ts">
  import type { ClusterMetrics } from '../types';
  import { HardDrive, Cpu, Server, Container, Activity, AppWindow } from 'lucide-svelte';

  let { metrics }: { metrics: ClusterMetrics | null } = $props();

  function formatMem(mb: number): string {
    if (mb >= 1048576) return `${(mb / 1048576).toFixed(1)} TB`;
    if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`;
    return `${mb} MB`;
  }
</script>

{#if metrics}
  <div class="grid grid-cols-6 gap-3 px-4 sm:px-6 py-3 bg-slate-50 dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800">
    <!-- Total Memory -->
    <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg p-3 shadow-2xs">
      <div class="flex items-center gap-2 mb-1">
        <HardDrive class="w-4 h-4 text-sky-500 dark:text-sky-400" />
        <span class="text-[11px] text-slate-500 dark:text-slate-400 font-medium">Total Memory</span>
      </div>
      <div class="text-lg font-bold text-slate-900 dark:text-slate-100">{formatMem(metrics.total_memory_mb)}</div>
      <div class="text-[10px] text-slate-400 dark:text-slate-500">Allocated: {formatMem(metrics.allocated_memory_mb)}</div>
    </div>

    <!-- Total Vcores -->
    <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg p-3 shadow-2xs">
      <div class="flex items-center gap-2 mb-1">
        <Cpu class="w-4 h-4 text-indigo-500 dark:text-indigo-400" />
        <span class="text-[11px] text-slate-500 dark:text-slate-400 font-medium">Total Vcores</span>
      </div>
      <div class="text-lg font-bold text-slate-900 dark:text-slate-100">{metrics.total_vcores}</div>
      <div class="text-[10px] text-slate-400 dark:text-slate-500">Allocated: {metrics.allocated_vcores}</div>
    </div>

    <!-- Available Memory -->
    <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg p-3 shadow-2xs">
      <div class="flex items-center gap-2 mb-1">
        <HardDrive class="w-4 h-4 text-emerald-500 dark:text-emerald-400" />
        <span class="text-[11px] text-slate-500 dark:text-slate-400 font-medium">Available Memory</span>
      </div>
      <div class="text-lg font-bold text-emerald-700 dark:text-emerald-400">{formatMem(metrics.available_memory_mb)}</div>
    </div>

    <!-- Available Vcores -->
    <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg p-3 shadow-2xs">
      <div class="flex items-center gap-2 mb-1">
        <Cpu class="w-4 h-4 text-emerald-500 dark:text-emerald-400" />
        <span class="text-[11px] text-slate-500 dark:text-slate-400 font-medium">Available Vcores</span>
      </div>
      <div class="text-lg font-bold text-emerald-700 dark:text-emerald-400">{metrics.available_vcores}</div>
    </div>

    <!-- Active Nodes -->
    <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg p-3 shadow-2xs">
      <div class="flex items-center gap-2 mb-1">
        <Server class="w-4 h-4 text-amber-500 dark:text-amber-400" />
        <span class="text-[11px] text-slate-500 dark:text-slate-400 font-medium">Active Nodes</span>
      </div>
      <div class="text-lg font-bold text-slate-900 dark:text-slate-100">{metrics.active_nodes}</div>
      {#if metrics.unhealthy_nodes > 0}
        <div class="text-[10px] text-red-500 dark:text-red-400">Unhealthy: {metrics.unhealthy_nodes}</div>
      {/if}
    </div>

    <!-- Running Apps -->
    <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg p-3 shadow-2xs">
      <div class="flex items-center gap-2 mb-1">
        <Activity class="w-4 h-4 text-violet-500 dark:text-violet-400" />
        <span class="text-[11px] text-slate-500 dark:text-slate-400 font-medium">Running Apps</span>
      </div>
      <div class="text-lg font-bold text-slate-900 dark:text-slate-100">{metrics.running_apps}</div>
      <div class="text-[10px] text-slate-400 dark:text-slate-500">Containers: {metrics.total_containers}</div>
    </div>
  </div>
{/if}
