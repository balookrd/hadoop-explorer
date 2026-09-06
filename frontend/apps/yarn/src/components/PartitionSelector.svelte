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

<div class="flex items-center gap-2 px-4 sm:px-6 py-2 bg-white border-b border-slate-200">
  <Tag class="w-4 h-4 text-slate-400" />
  <span class="text-xs text-slate-500 font-medium mr-1">Partition:</span>
  {#each partitions as p}
    <button
      onclick={() => selectedPartition = p}
      class="px-3 py-1 rounded-md text-xs font-medium transition cursor-pointer {
        selectedPartition === p
          ? 'bg-sky-600 text-white shadow-sm'
          : 'bg-white text-slate-700 border border-slate-200 hover:bg-slate-50'
      }"
    >
      {p}
    </button>
  {/each}
</div>
