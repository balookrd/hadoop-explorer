<script lang="ts">
  import { Tag } from 'lucide-svelte';

  let {
    partitions,
    selectedPartition = $bindable()
  }: {
    partitions: string[];
    selectedPartition: string;
  } = $props();
</script>

<div class="flex items-center gap-2 px-4 sm:px-6 py-2 bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800">
  <Tag class="w-4 h-4 text-slate-400 dark:text-slate-500" />
  <span class="text-xs text-slate-500 dark:text-slate-400 font-medium mr-1">Partition:</span>
  {#each partitions as p}
    <button
      onclick={() => selectedPartition = p}
      class="px-3 py-1 rounded-md text-xs font-medium transition cursor-pointer {
        selectedPartition === p
          ? 'bg-sky-600 text-white shadow-sm'
          : 'bg-white dark:bg-slate-800 text-slate-700 dark:text-slate-200 border border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700'
      }"
    >
      {p}
    </button>
  {/each}
</div>
