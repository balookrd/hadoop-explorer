<script lang="ts">
  import { X, Download, Copy, CheckCircle, Percent, Hash } from 'lucide-svelte';

  let {
    xmlContent,
    filename,
    instructions,
    currentMode = 'percentage',
    isOpen = $bindable(),
    clusterId,
    canAdmin = false,
    onModeChange,
  }: {
    xmlContent: string;
    filename: string;
    instructions: string;
    currentMode?: 'percentage' | 'absolute';
    isOpen: boolean;
    clusterId?: string;
    canAdmin?: boolean;
    onModeChange?: (mode: 'percentage' | 'absolute') => void;
  } = $props();

  let copied = $state(false);

  function downloadXml() {
    const blob = new Blob([xmlContent], { type: 'application/xml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function copyToClipboard() {
    await navigator.clipboard.writeText(xmlContent);
    copied = true;
    setTimeout(() => copied = false, 2000);
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isOpen) isOpen = false; }} />

{#if isOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 dark:bg-slate-950/80 backdrop-blur-sm p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) isOpen = false; }}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl w-full max-w-5xl max-h-[85vh] flex flex-col border border-slate-200 dark:border-slate-800 select-auto overflow-hidden"
      onclick={(e) => e.stopPropagation()}
    >
      <!-- Header -->
      <div class="flex items-center justify-between px-6 py-4 border-b border-slate-200 dark:border-slate-800">
        <div>
          <div class="flex items-center gap-3">
            <h2 class="text-sm font-bold text-slate-900 dark:text-slate-100">Правки capacity-scheduler.xml</h2>
            {#if onModeChange}
              <div class="flex items-center gap-1 bg-slate-100 dark:bg-slate-800 p-0.5 rounded-lg border border-slate-200 dark:border-slate-700">
                <button
                  onclick={() => onModeChange('percentage')}
                  class="flex items-center gap-1 px-2.5 py-0.5 rounded text-xs font-semibold cursor-pointer {
                    currentMode === 'percentage' ? 'bg-white dark:bg-slate-900 text-sky-700 dark:text-sky-400 shadow-xs' : 'text-slate-600 dark:text-slate-400'
                  }"
                >
                  <Percent class="w-3 h-3" />
                  <span>Проценты (%)</span>
                </button>
                <button
                  onclick={() => onModeChange('absolute')}
                  class="flex items-center gap-1 px-2.5 py-0.5 rounded text-xs font-semibold cursor-pointer {
                    currentMode === 'absolute' ? 'bg-white dark:bg-slate-900 text-sky-700 dark:text-sky-400 shadow-xs' : 'text-slate-600 dark:text-slate-400'
                  }"
                >
                  <Hash class="w-3 h-3" />
                  <span>Абсолютные [memory,vcores]</span>
                </button>
              </div>
            {/if}
          </div>
          <p class="text-[11px] text-slate-500 dark:text-slate-400 font-mono mt-0.5">{filename}</p>
        </div>
        <div class="flex items-center gap-2">
          <button onclick={copyToClipboard}
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-xs font-medium hover:bg-slate-50 dark:hover:bg-slate-800 transition cursor-pointer {copied ? 'text-emerald-600 dark:text-emerald-400 border-emerald-300 dark:border-emerald-800' : 'text-slate-700 dark:text-slate-300'}">
            {#if copied}
              <CheckCircle class="w-3.5 h-3.5" />
              Скопировано!
            {:else}
              <Copy class="w-3.5 h-3.5" />
              Копировать
            {/if}
          </button>
          <button onclick={downloadXml}
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-gradient-to-r from-sky-600 to-indigo-600 text-white text-xs font-semibold shadow-sm cursor-pointer hover:shadow-md transition">
            <Download class="w-3.5 h-3.5" />
            Скачать
          </button>
          <button onclick={() => isOpen = false} class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 cursor-pointer">
            <X class="w-4 h-4 text-slate-500 dark:text-slate-400" />
          </button>
        </div>
      </div>


      <!-- XML Content -->
      <div class="flex-1 overflow-auto p-4 bg-white dark:bg-slate-900">
        <pre class="bg-slate-900 dark:bg-slate-950 border border-slate-800 text-slate-100 rounded-xl p-4 text-xs font-mono leading-relaxed overflow-auto max-h-[50vh]">{xmlContent}</pre>
      </div>

      <!-- Instructions -->
      <div class="px-6 py-3 border-t border-slate-200 dark:border-slate-800 bg-amber-50 dark:bg-amber-950/40">
        <div class="text-[11px] font-semibold text-amber-800 dark:text-amber-300 mb-1">Инструкция по применению на кластере:</div>
        <pre class="text-[11px] text-amber-700 dark:text-amber-400 font-mono whitespace-pre-wrap">{instructions}</pre>
      </div>
    </div>
  </div>
{/if}
