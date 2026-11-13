<script lang="ts">
  import { Tag } from 'lucide-svelte';

  let {
    partitions = [],
    selectedPartition = $bindable()
  }: {
    partitions: string[];
    selectedPartition: string;
  } = $props();

  const displayPartitions = $derived(
    partitions && partitions.length > 0 ? partitions : [selectedPartition || 'DEFAULT']
  );
</script>

<div class="flex items-center gap-1.5 bg-slate-50 dark:bg-slate-800/60 p-1 rounded-lg border border-slate-200 dark:border-slate-700 shadow-2xs">
  <div class="flex items-center gap-1 text-slate-500 dark:text-slate-400 px-1">
    <Tag class="w-3.5 h-3.5" />
    <span class="text-xs font-medium">Partition:</span>
  </div>
  {#each displayPartitions as p}
    <button
      onclick={() => selectedPartition = p}
      class="px-2.5 py-1 rounded-md text-xs font-medium transition cursor-pointer {
        selectedPartition === p
          ? 'bg-sky-600 text-white shadow-xs font-semibold'
          : 'bg-white dark:bg-slate-800 text-slate-700 dark:text-slate-200 border border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700'
      }"
    >
      {p}
    </button>
  {/each}
</div>
