<script lang="ts">
  import type { DiffItem, QueueMappingsDiff } from '../types';
  import { X, FileDown, Send, ArrowRightLeft } from 'lucide-svelte';

  let {
    diffs,
    queueMappingsDiff = null,
    canAdmin,
    isOpen = $bindable(),
    onGenerateXml,
  }: {
    diffs: DiffItem[];
    queueMappingsDiff?: QueueMappingsDiff | null;
    canAdmin: boolean;
    isOpen: boolean;
    onGenerateXml: () => void;
  } = $props();

  const changedDiffs = $derived(diffs.filter(d => d.action !== 'unchanged'));
  const hasMappingsDiff = $derived(queueMappingsDiff?.is_changed || false);
  const totalChangesCount = $derived(changedDiffs.length + (hasMappingsDiff ? 1 : 0));

  function actionBadge(action: string): string {
    switch (action) {
      case 'created': return 'bg-emerald-100 text-emerald-700';
      case 'modified': return 'bg-amber-100 text-amber-700';
      case 'deleted': return 'bg-red-100 text-red-700';
      default: return 'bg-slate-100 text-slate-500';
    }
  }
</script>

{#if isOpen}
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-sm">
    <div class="bg-white rounded-2xl shadow-2xl w-full max-w-4xl max-h-[85vh] flex flex-col">
      <!-- Header -->
      <div class="flex items-center justify-between px-6 py-4 border-b border-slate-200">
        <div>
          <h2 class="text-sm font-bold text-slate-900">Changes Review (Live → Draft)</h2>
          <p class="text-[11px] text-slate-500 mt-0.5">{totalChangesCount} change(s)</p>
        </div>
        <button onclick={() => isOpen = false} class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-100 cursor-pointer">
          <X class="w-4 h-4 text-slate-500" />
        </button>
      </div>

      <!-- Content -->
      <div class="flex-1 overflow-auto divide-y divide-slate-100">
        <!-- Queue Mappings Diff Card -->
        {#if hasMappingsDiff && queueMappingsDiff}
          <div class="p-4 bg-indigo-50/50 border-b border-indigo-100">
            <div class="flex items-center gap-2 mb-2">
              <ArrowRightLeft class="w-4 h-4 text-indigo-600" />
              <span class="text-xs font-bold text-slate-900">Queue Mappings & Overrides</span>
              <span class="text-[10px] px-2 py-0.5 rounded-full font-bold bg-amber-100 text-amber-700">modified</span>
            </div>
            
            <div class="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
              <div class="bg-white rounded-lg p-2.5 border border-slate-200">
                <div class="text-[10px] uppercase font-bold text-slate-400 mb-1">Live Configuration</div>
                <div class="font-mono text-[11px] text-slate-700 break-all">
                  {queueMappingsDiff.live_mappings || '<пусто>'}
                </div>
                <div class="mt-1.5 text-[11px] text-slate-500">
                  Override enabled: <span class="font-semibold">{queueMappingsDiff.live_override ? 'true' : 'false'}</span>
                </div>
              </div>

              <div class="bg-indigo-50/80 rounded-lg p-2.5 border border-indigo-200">
                <div class="text-[10px] uppercase font-bold text-indigo-600 mb-1">Draft Configuration</div>
                <div class="font-mono text-[11px] text-indigo-950 font-semibold break-all">
                  {queueMappingsDiff.draft_mappings || '<пусто>'}
                </div>
                <div class="mt-1.5 text-[11px] text-indigo-900">
                  Override enabled: <span class="font-semibold">{queueMappingsDiff.draft_override ? 'true' : 'false'}</span>
                </div>
              </div>
            </div>
          </div>
        {/if}

        <!-- Queues Table -->
        {#if changedDiffs.length > 0}
          <table class="w-full text-xs">
            <thead class="bg-slate-50 sticky top-0">
              <tr class="text-[11px] text-slate-500 font-semibold uppercase tracking-wider">
                <th class="text-left px-4 py-2.5">Queue</th>
                <th class="text-center px-2 py-2.5">Action</th>
                <th class="text-right px-2 py-2.5">Capacity</th>
                <th class="text-center px-2 py-2.5"></th>
                <th class="text-right px-2 py-2.5">Draft Capacity</th>
                <th class="text-right px-2 py-2.5">Max Cap</th>
                <th class="text-center px-2 py-2.5"></th>
                <th class="text-right px-2 py-2.5">Draft Max</th>
              </tr>
            </thead>
            <tbody>
              {#each changedDiffs as d}
                <tr class="border-b border-slate-100 hover:bg-slate-50">
                  <td class="px-4 py-2 font-mono text-slate-800">
                    <div class="font-semibold">{d.path}</div>
                    {#if d.draft_resource_mode && d.live_resource_mode && d.draft_resource_mode !== d.live_resource_mode}
                      <div class="text-[10px] text-amber-700 font-sans mt-0.5">
                        Режим: <span class="font-bold">{d.live_resource_mode === 'absolute' ? 'ABS' : '%'} → {d.draft_resource_mode === 'absolute' ? 'ABS' : '%'}</span>
                      </div>
                    {/if}
                    {#if d.draft_user_limit_factor != null && d.live_user_limit_factor !== d.draft_user_limit_factor}
                      <div class="text-[10px] text-sky-700 font-sans mt-0.5">
                        User Limit Factor: <span class="font-bold">{d.live_user_limit_factor ?? 1.0} → {d.draft_user_limit_factor}</span>
                      </div>
                    {/if}
                    {#if d.draft_ordering_policy && d.live_ordering_policy !== d.draft_ordering_policy}
                      <div class="text-[10px] text-purple-700 font-sans mt-0.5">
                        Ordering Policy: <span class="font-bold">{d.live_ordering_policy ?? 'fifo'} → {d.draft_ordering_policy}</span>
                      </div>
                    {/if}
                    {#if d.draft_max_applications != null && d.live_max_applications !== d.draft_max_applications}
                      <div class="text-[10px] text-indigo-700 font-sans mt-0.5">
                        Max Apps: <span class="font-bold">{d.live_max_applications ?? '—'} → {d.draft_max_applications}</span>
                      </div>
                    {/if}
                    {#if d.draft_max_parallel_apps != null && d.live_max_parallel_apps !== d.draft_max_parallel_apps}
                      <div class="text-[10px] text-indigo-700 font-sans mt-0.5">
                        Max Parallel Apps: <span class="font-bold">{d.live_max_parallel_apps ?? '—'} → {d.draft_max_parallel_apps}</span>
                      </div>
                    {/if}
                    {#if d.draft_max_am_resource_percent != null && d.live_max_am_resource_percent !== d.draft_max_am_resource_percent}
                      <div class="text-[10px] text-indigo-700 font-sans mt-0.5">
                        Max AM %: <span class="font-bold">{d.live_max_am_resource_percent != null ? `${(d.live_max_am_resource_percent <= 1 ? d.live_max_am_resource_percent * 100 : d.live_max_am_resource_percent).toFixed(1)}%` : '—'} → {(d.draft_max_am_resource_percent <= 1 ? d.draft_max_am_resource_percent * 100 : d.draft_max_am_resource_percent).toFixed(1)}%</span>
                      </div>
                    {/if}
                    {#if d.draft_max_application_lifetime != null && d.live_max_application_lifetime !== d.draft_max_application_lifetime}
                      <div class="text-[10px] text-indigo-700 font-sans mt-0.5">
                        Max Lifetime: <span class="font-bold">{d.live_max_application_lifetime ?? '—'}s → {d.draft_max_application_lifetime}s</span>
                      </div>
                    {/if}
                  </td>
                  <td class="text-center px-2 py-2">
                    <span class="text-[10px] px-2 py-0.5 rounded-full font-bold capitalize {actionBadge(d.action)}">{d.action}</span>
                  </td>
                  <td class="text-right px-2 py-2 font-mono text-slate-500">
                    {d.live_capacity != null ? `${d.live_capacity.toFixed(1)}%` : '—'}
                  </td>
                  <td class="text-center px-2 py-2 text-slate-400">→</td>
                  <td class="text-right px-2 py-2 font-mono font-bold {
                    d.delta_capacity != null && d.delta_capacity > 0 ? 'text-emerald-600' :
                    d.delta_capacity != null && d.delta_capacity < 0 ? 'text-red-600' : 'text-slate-800'
                  }">
                    {d.draft_capacity != null ? `${d.draft_capacity.toFixed(1)}%` : '—'}
                    {#if d.delta_capacity != null && Math.abs(d.delta_capacity) > 0.01}
                      <span class="text-[10px] ml-1">({d.delta_capacity > 0 ? '+' : ''}{d.delta_capacity.toFixed(1)})</span>
                    {/if}
                  </td>
                  <td class="text-right px-2 py-2 font-mono text-slate-500">
                    {d.live_max_capacity != null ? `${d.live_max_capacity.toFixed(1)}%` : '—'}
                  </td>
                  <td class="text-center px-2 py-2 text-slate-400">→</td>
                  <td class="text-right px-2 py-2 font-mono font-bold {
                    d.delta_max_capacity != null && d.delta_max_capacity > 0 ? 'text-emerald-600' :
                    d.delta_max_capacity != null && d.delta_max_capacity < 0 ? 'text-red-600' : 'text-slate-800'
                  }">
                    {d.draft_max_capacity != null ? `${d.draft_max_capacity.toFixed(1)}%` : '—'}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        {:else if !hasMappingsDiff}
          <div class="p-8 text-center text-slate-400 text-xs">
            Нет обнаруженных изменений
          </div>
        {/if}
      </div>

      <!-- Footer -->
      <div class="flex items-center justify-end gap-2 px-6 py-3 border-t border-slate-200">
        <button onclick={() => isOpen = false}
          class="px-4 py-2 rounded-lg border border-slate-200 text-slate-700 text-xs font-medium hover:bg-slate-50 transition cursor-pointer">
          Close
        </button>
        {#if canAdmin}
          <button onclick={() => { onGenerateXml(); isOpen = false; }}
            class="flex items-center gap-1.5 px-4 py-2 rounded-lg bg-gradient-to-r from-sky-600 to-indigo-600 text-white text-xs font-semibold shadow-md cursor-pointer">
            <FileDown class="w-3.5 h-3.5" />
            Generate XML
          </button>
        {:else}
          <button
            class="flex items-center gap-1.5 px-4 py-2 rounded-lg bg-amber-500 text-white text-xs font-semibold shadow-md cursor-pointer"
            title="Send draft for Admin review"
          >
            <Send class="w-3.5 h-3.5" />
            Send for Review
          </button>
        {/if}
      </div>
    </div>
  </div>
{/if}

