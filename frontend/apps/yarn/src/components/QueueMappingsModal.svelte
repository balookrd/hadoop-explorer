<script lang="ts">
  import type { QueueNode } from '../types';
  import { X, Plus, Trash2, ArrowUp, ArrowDown, Save, RotateCcw, ArrowRightLeft, User, Users, Info } from 'lucide-svelte';

  interface RuleItem {
    id: string;
    type: 'u' | 'g';
    source: string;
    target: string;
  }

  let {
    isOpen = $bindable(),
    liveMappings = '',
    draftMappings = '',
    liveOverride = false,
    draftOverride = false,
    rootQueue = null,
    canWrite = true,
    onSave,
  }: {
    isOpen: boolean;
    liveMappings?: string;
    draftMappings?: string;
    liveOverride?: boolean;
    draftOverride?: boolean;
    rootQueue: QueueNode | null;
    canWrite: boolean;
    onSave: (mappings: string, override: boolean) => void;
  } = $props();

  let mode = $state<'visual' | 'raw'>('visual');
  let rawText = $state('');
  let overrideEnable = $state(false);
  let rules = $state<RuleItem[]>([]);
  let wasOpen = $state(false);

  function collectQueuePaths(node: QueueNode | null): string[] {
    if (!node) return [];
    const res: string[] = [node.path];
    for (const ch of node.children) {
      res.push(...collectQueuePaths(ch));
    }
    return res;
  }

  const availableQueues = $derived(collectQueuePaths(rootQueue));

  function parseMappingsToRules(str: string): RuleItem[] {
    if (!str || !str.trim()) return [];
    const parts = str.split(',').map(s => s.trim()).filter(Boolean);
    return parts.map((p, idx) => {
      const tokens = p.split(':');
      const type = tokens[0] === 'g' ? 'g' : 'u';
      const source = tokens[1] || '%user';
      const target = tokens.slice(2).join(':') || 'root.default';
      return {
        id: `rule-${idx}-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`,
        type,
        source,
        target,
      };
    });
  }

  function rulesToMappings(items: RuleItem[]): string {
    return items
      .filter(r => r.source.trim() && r.target.trim())
      .map(r => `${r.type}:${r.source.trim()}:${r.target.trim()}`)
      .join(',');
  }

  $effect(() => {
    if (isOpen && !wasOpen) {
      wasOpen = true;
      const initial = draftMappings !== undefined && draftMappings !== '' ? draftMappings : liveMappings;
      rawText = initial || '';
      overrideEnable = draftOverride !== undefined ? draftOverride : liveOverride;
      rules = parseMappingsToRules(rawText);
    } else if (!isOpen) {
      wasOpen = false;
    }
  });

  function addRule() {
    rules = [
      ...rules,
      {
        id: `rule-new-${Date.now()}`,
        type: 'u',
        source: '%user',
        target: availableQueues[availableQueues.length - 1] || 'root.default',
      },
    ];
    rawText = rulesToMappings(rules);
  }

  function removeRule(id: string) {
    rules = rules.filter(r => r.id !== id);
    rawText = rulesToMappings(rules);
  }

  function moveRule(index: number, direction: 'up' | 'down') {
    const targetIdx = direction === 'up' ? index - 1 : index + 1;
    if (targetIdx < 0 || targetIdx >= rules.length) return;
    const newRules = [...rules];
    const temp = newRules[index];
    newRules[index] = newRules[targetIdx];
    newRules[targetIdx] = temp;
    rules = newRules;
    rawText = rulesToMappings(rules);
  }

  function handleModeChange(newMode: 'visual' | 'raw') {
    if (newMode === 'raw') {
      rawText = rulesToMappings(rules);
    } else {
      rules = parseMappingsToRules(rawText);
    }
    mode = newMode;
  }

  function handleReset() {
    rawText = liveMappings || '';
    overrideEnable = liveOverride;
    rules = parseMappingsToRules(rawText);
  }

  function handleSave() {
    const finalMappings = mode === 'raw' ? rawText.trim() : rulesToMappings(rules);
    onSave(finalMappings, overrideEnable);
    isOpen = false;
  }
</script>

{#if isOpen}
  <!-- Backdrop -->
  <div
    class="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4 backdrop-blur-xs"
    onclick={() => isOpen = false}
    role="presentation"
  >
    <!-- Modal Content -->
    <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_noninteractive_element_interactions -->
    <div
      class="bg-white rounded-2xl shadow-2xl border border-slate-200 w-full max-w-3xl max-h-[85vh] flex flex-col overflow-hidden"
      onclick={(e) => e.stopPropagation()}
      role="dialog"
      aria-modal="true"
      aria-label="Queue Mappings Dialog"
      tabindex="-1"
    >
      <!-- Header -->
      <div class="flex items-center justify-between px-6 py-4 border-b border-slate-200 bg-slate-50/70">
        <div class="flex items-center gap-2.5">
          <div class="w-8 h-8 rounded-lg bg-sky-100 text-sky-700 flex items-center justify-center">
            <ArrowRightLeft class="w-4 h-4" />
          </div>
          <div>
            <h2 class="text-sm font-bold text-slate-900">Управление Queue Mappings</h2>
            <p class="text-[11px] text-slate-500 font-mono mt-0.5">yarn.scheduler.capacity.queue-mappings</p>
          </div>
        </div>
        <button
          onclick={() => isOpen = false}
          class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-200 text-slate-500 transition cursor-pointer"
        >
          <X class="w-4 h-4" />
        </button>
      </div>

      <!-- Toolbar & Mode Selector -->
      <div class="px-6 py-3 bg-slate-100/60 border-b border-slate-200 flex items-center justify-between gap-4">
        <div class="flex items-center gap-1 bg-white p-0.5 rounded-lg border border-slate-200 shadow-xs">
          <button
            onclick={() => handleModeChange('visual')}
            class="px-3 py-1 rounded text-xs font-semibold transition cursor-pointer {
              mode === 'visual' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 hover:text-slate-900'
            }"
          >
            Конструктор правил
          </button>
          <button
            onclick={() => handleModeChange('raw')}
            class="px-3 py-1 rounded text-xs font-semibold transition cursor-pointer {
              mode === 'raw' ? 'bg-sky-600 text-white shadow-xs' : 'text-slate-600 hover:text-slate-900'
            }"
          >
            Raw String (текст)
          </button>
        </div>

        <!-- Override Switch -->
        <label class="flex items-center gap-2 text-xs font-medium text-slate-700 cursor-pointer select-none">
          <input
            type="checkbox"
            bind:checked={overrideEnable}
            disabled={!canWrite}
            class="w-4 h-4 rounded text-sky-600 accent-sky-600 cursor-pointer"
          />
          <span>queue-mappings-override.enable</span>
        </label>
      </div>

      <!-- Info Banner -->
      <div class="px-6 py-2.5 bg-sky-50 border-b border-sky-100 flex items-start gap-2 text-xs text-sky-900">
        <Info class="w-4 h-4 text-sky-600 shrink-0 mt-0.5" />
        <div class="leading-relaxed">
          Правила сопоставления проверяются <strong>сверху вниз</strong> до первого совпадения.
          Синтаксис: <code class="bg-sky-100 px-1 py-0.2 rounded font-mono text-[11px]">u:&lt;user&gt;:&lt;queue&gt;</code> или <code class="bg-sky-100 px-1 py-0.2 rounded font-mono text-[11px]">g:&lt;group&gt;:&lt;queue&gt;</code>.
        </div>
      </div>

      <!-- Content Area -->
      <div class="flex-1 overflow-auto p-6">
        {#if mode === 'visual'}
          {#if rules.length === 0}
            <div class="text-center py-10 border-2 border-dashed border-slate-200 rounded-xl">
              <ArrowRightLeft class="w-8 h-8 text-slate-300 mx-auto mb-2" />
              <p class="text-xs text-slate-500 font-medium">Нет настроенных правил маппинга</p>
              {#if canWrite}
                <button
                  onclick={addRule}
                  class="mt-3 inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-sky-600 text-white text-xs font-semibold hover:bg-sky-700 transition cursor-pointer"
                >
                  <Plus class="w-3.5 h-3.5" />
                  Добавить первое правило
                </button>
              {/if}
            </div>
          {:else}
            <div class="space-y-2.5">
              {#each rules as r, index (r.id)}
                <div class="flex items-center gap-2 p-2.5 bg-slate-50 border border-slate-200 rounded-xl hover:bg-white hover:border-slate-300 transition">
                  <span class="w-5 text-center text-xs font-mono font-bold text-slate-400">{index + 1}</span>

                  <!-- Type Selector (User / Group) -->
                  <div class="flex items-center bg-white border border-slate-200 rounded-lg p-0.5">
                    <button
                      type="button"
                      disabled={!canWrite}
                      onclick={() => { r.type = 'u'; rawText = rulesToMappings(rules); }}
                      class="flex items-center gap-1 px-2 py-1 rounded text-xs font-semibold cursor-pointer {
                        r.type === 'u' ? 'bg-sky-100 text-sky-800' : 'text-slate-500 hover:text-slate-800'
                      }"
                      title="Пользователь (u:)"
                    >
                      <User class="w-3 h-3" />
                      <span>User</span>
                    </button>
                    <button
                      type="button"
                      disabled={!canWrite}
                      onclick={() => { r.type = 'g'; rawText = rulesToMappings(rules); }}
                      class="flex items-center gap-1 px-2 py-1 rounded text-xs font-semibold cursor-pointer {
                        r.type === 'g' ? 'bg-indigo-100 text-indigo-800' : 'text-slate-500 hover:text-slate-800'
                      }"
                      title="Группа (g:)"
                    >
                      <Users class="w-3 h-3" />
                      <span>Group</span>
                    </button>
                  </div>

                  <span class="text-xs text-slate-400 font-mono">:</span>

                  <!-- Source Input with Quick Substitutions -->
                  <div class="flex-1 relative">
                    <input
                      type="text"
                      bind:value={r.source}
                      oninput={() => rawText = rulesToMappings(rules)}
                      disabled={!canWrite}
                      placeholder={r.type === 'u' ? '%user или admin_user' : '%primary_group или hadoop-admins'}
                      class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-900 outline-none focus:border-sky-500"
                    />
                    <div class="absolute right-1.5 top-1/2 -translate-y-1/2 flex items-center gap-1">
                      <button
                        type="button"
                        onclick={() => { r.source = '%user'; rawText = rulesToMappings(rules); }}
                        class="text-[9px] px-1 py-0.5 bg-slate-100 text-slate-600 hover:bg-slate-200 rounded font-mono cursor-pointer"
                        title="Подставить %user"
                      >
                        %user
                      </button>
                      <button
                        type="button"
                        onclick={() => { r.source = '%primary_group'; rawText = rulesToMappings(rules); }}
                        class="text-[9px] px-1 py-0.5 bg-slate-100 text-slate-600 hover:bg-slate-200 rounded font-mono cursor-pointer"
                        title="Подставить %primary_group"
                      >
                        %group
                      </button>
                    </div>
                  </div>

                  <span class="text-xs text-slate-400 font-mono">→</span>

                  <!-- Target Queue Selector -->
                  <div class="flex-1">
                    <select
                      bind:value={r.target}
                      onchange={() => rawText = rulesToMappings(rules)}
                      disabled={!canWrite}
                      class="w-full px-3 py-1.5 rounded-lg border border-slate-300 bg-white text-xs font-mono text-slate-900 outline-none focus:border-sky-500 cursor-pointer"
                    >
                      {#each availableQueues as qPath}
                        <option value={qPath}>{qPath}</option>
                      {/each}
                      <option value="%user">%user (персональная очередь)</option>
                      <option value="root.users.%user">root.users.%user</option>
                      <option value="root.%primary_group">root.%primary_group</option>
                    </select>
                  </div>

                  <!-- Move & Delete Actions -->
                  {#if canWrite}
                    <div class="flex items-center gap-0.5">
                      <button
                        type="button"
                        disabled={index === 0}
                        onclick={() => moveRule(index, 'up')}
                        class="p-1 rounded hover:bg-slate-200 text-slate-500 disabled:opacity-30 cursor-pointer"
                        title="Поднять выше"
                      >
                        <ArrowUp class="w-3.5 h-3.5" />
                      </button>
                      <button
                        type="button"
                        disabled={index === rules.length - 1}
                        onclick={() => moveRule(index, 'down')}
                        class="p-1 rounded hover:bg-slate-200 text-slate-500 disabled:opacity-30 cursor-pointer"
                        title="Опустить ниже"
                      >
                        <ArrowDown class="w-3.5 h-3.5" />
                      </button>
                      <button
                        type="button"
                        onclick={() => removeRule(r.id)}
                        class="p-1 rounded hover:bg-red-100 text-red-500 transition cursor-pointer"
                        title="Удалить правило"
                      >
                        <Trash2 class="w-3.5 h-3.5" />
                      </button>
                    </div>
                  {/if}
                </div>
              {/each}

              {#if canWrite}
                <button
                  type="button"
                  onclick={addRule}
                  class="mt-2 inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-dashed border-sky-300 text-sky-700 bg-sky-50/50 hover:bg-sky-100 text-xs font-semibold transition cursor-pointer"
                >
                  <Plus class="w-3.5 h-3.5" />
                  Добавить правило маппинга
                </button>
              {/if}
            </div>
          {/if}
        {:else}
          <!-- Raw Text Mode -->
          <div class="space-y-3">
            <div>
              <label for="raw-queue-mappings-input" class="block text-xs font-semibold text-slate-700 mb-1">
                Значение yarn.scheduler.capacity.queue-mappings
              </label>
              <textarea
                id="raw-queue-mappings-input"
                bind:value={rawText}
                disabled={!canWrite}
                rows="6"
                placeholder="u:%user:%user,g:hadoop-admins:root.production"
                class="w-full px-3 py-2 rounded-xl border border-slate-300 bg-slate-50 font-mono text-xs text-slate-900 outline-none focus:border-sky-500 focus:bg-white leading-relaxed"
              ></textarea>
              <p class="text-[11px] text-slate-500 mt-1">
                Разделяйте правила запятыми: <code class="font-mono text-slate-700">u:admin:root.production,g:analysts:root.analytics,u:%user:root.default</code>
              </p>
            </div>
          </div>
        {/if}
      </div>

      <!-- Footer -->
      <div class="flex items-center justify-between px-6 py-3.5 border-t border-slate-200 bg-slate-50/70">
        {#if canWrite}
          <button
            type="button"
            onclick={handleReset}
            class="flex items-center gap-1 px-3 py-1.5 rounded-lg border border-slate-200 text-slate-700 text-xs font-medium hover:bg-white transition cursor-pointer"
          >
            <RotateCcw class="w-3.5 h-3.5" />
            Сбросить к live
          </button>
        {:else}
          <div></div>
        {/if}

        <div class="flex items-center gap-2">
          <button
            type="button"
            onclick={() => isOpen = false}
            class="px-4 py-2 rounded-lg border border-slate-200 text-slate-700 text-xs font-medium hover:bg-white transition cursor-pointer"
          >
            Закрыть
          </button>
          {#if canWrite}
            <button
              type="button"
              onclick={handleSave}
              class="flex items-center gap-1.5 px-4 py-2 rounded-lg bg-gradient-to-r from-sky-600 to-indigo-600 text-white text-xs font-semibold shadow-md shadow-sky-500/20 hover:shadow-lg transition cursor-pointer"
            >
              <Save class="w-3.5 h-3.5" />
              Применить в черновик
            </button>
          {/if}
        </div>
      </div>
    </div>
  </div>
{/if}
