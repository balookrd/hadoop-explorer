<script lang="ts">
  import type { DraftQueueItem } from '../types';
  import { X, Send, GitPullRequest, AlertCircle } from 'lucide-svelte';

  let {
    clusterId,
    changes,
    isOpen = $bindable(),
    onSubmit,
  }: {
    clusterId: string;
    changes: DraftQueueItem[];
    isOpen: boolean;
    onSubmit: (title: string, description: string) => Promise<void>;
  } = $props();

  let title = $state('');
  let description = $state('');
  let error = $state('');
  let isSubmitting = $state(false);

  function resetForm() {
    title = '';
    description = '';
    error = '';
    isSubmitting = false;
  }

  async function handleSubmit() {
    if (!title.trim()) {
      error = 'Укажите краткое название заявки';
      return;
    }
    if (changes.length === 0) {
      error = 'Нет изменений для отправки';
      return;
    }

    try {
      isSubmitting = true;
      error = '';
      await onSubmit(title.trim(), description.trim());
      resetForm();
      isOpen = false;
    } catch (err: any) {
      error = err.message || 'Ошибка при отправке заявки';
    } finally {
      isSubmitting = false;
    }
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isOpen) { isOpen = false; resetForm(); } }} />

{#if isOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 dark:bg-slate-950/80 backdrop-blur-sm p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) { isOpen = false; resetForm(); } }}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl p-6 w-full max-w-lg border border-slate-200 dark:border-slate-800 select-auto"
      onclick={(e) => e.stopPropagation()}
    >
      <!-- Header -->
      <div class="flex items-center justify-between mb-4 border-b border-slate-100 dark:border-slate-800 pb-3">
        <div class="flex items-center gap-2">
          <div class="w-8 h-8 rounded-lg bg-sky-100 dark:bg-sky-950/60 flex items-center justify-center text-sky-600 dark:text-sky-400">
            <GitPullRequest class="w-4 h-4" />
          </div>
          <div>
            <h2 class="text-sm font-bold text-slate-900 dark:text-slate-100">Заявка на согласование изменений</h2>
            <p class="text-[11px] text-slate-500 dark:text-slate-400">Кластер: <span class="font-mono font-semibold text-slate-700 dark:text-slate-300">{clusterId}</span></p>
          </div>
        </div>
        <button
          onclick={() => { isOpen = false; resetForm(); }}
          class="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-400 dark:text-slate-500 hover:text-slate-600 dark:hover:text-slate-300 cursor-pointer"
        >
          <X class="w-4 h-4" />
        </button>
      </div>

      {#if error}
        <div class="mb-4 p-2.5 rounded-lg bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 text-xs flex items-center gap-1.5">
          <AlertCircle class="w-4 h-4 shrink-0 text-red-500" />
          <span>{error}</span>
        </div>
      {/if}

      <div class="space-y-4">
        <!-- Title -->
        <div>
          <label for="cr-title" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Название заявки <span class="text-red-500">*</span>
          </label>
          <input
            id="cr-title"
            type="text"
            bind:value={title}
            placeholder="Например: Выделение ресурсов для отдела аналитики"
            class="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-950 text-sm text-slate-900 dark:text-slate-100 outline-none focus:border-sky-500 focus:ring-2 focus:ring-sky-500/20"
          />
        </div>

        <!-- Description -->
        <div>
          <label for="cr-desc" class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
            Обоснование / Комментарий для администратора
          </label>
          <textarea
            id="cr-desc"
            bind:value={description}
            rows="3"
            placeholder="Опишите причину изменения квот, номер задачи в трекере и планируемый срок нагрузки..."
            class="w-full px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-950 text-sm text-slate-900 dark:text-slate-100 outline-none focus:border-sky-500 focus:ring-2 focus:ring-sky-500/20 resize-none"
          ></textarea>
        </div>

        <!-- Changes Summary -->
        <div class="p-3 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl space-y-2">
          <div class="flex justify-between items-center text-xs font-semibold text-slate-800 dark:text-slate-200">
            <span>Включаемые изменения</span>
            <span class="text-[11px] text-slate-500 dark:text-slate-400 font-normal">{changes.length} очер.</span>
          </div>

          <div class="max-h-32 overflow-y-auto space-y-1.5 pr-1">
            {#each changes as item}
              <div class="flex items-center justify-between text-xs py-1 px-2 rounded bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800">
                <span class="font-mono text-slate-800 dark:text-slate-200 truncate mr-2">{item.path}</span>
                <span class="text-[10px] font-bold px-1.5 py-0.5 rounded {
                  item.action === 'create' ? 'bg-emerald-100 dark:bg-emerald-950/60 text-emerald-700 dark:text-emerald-300' :
                  item.action === 'delete' ? 'bg-red-100 dark:bg-red-950/60 text-red-700 dark:text-red-300' :
                  'bg-sky-100 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300'
                }">
                  {item.action.toUpperCase()}
                </span>
              </div>
            {/each}
          </div>
        </div>
      </div>

      <!-- Footer Buttons -->
      <div class="flex items-center gap-2 mt-6">
        <button
          onclick={() => { isOpen = false; resetForm(); }}
          class="flex-1 py-2 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-300 text-xs font-medium hover:bg-slate-50 dark:hover:bg-slate-800 transition cursor-pointer"
        >
          Отмена
        </button>
        <button
          onclick={handleSubmit}
          disabled={isSubmitting}
          class="flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg bg-gradient-to-r from-sky-600 to-indigo-600 text-white text-xs font-semibold shadow-md hover:shadow-lg disabled:opacity-50 transition cursor-pointer"
        >
          <Send class="w-3.5 h-3.5" />
          <span>{isSubmitting ? 'Отправка...' : 'Отправить на согласование'}</span>
        </button>
      </div>
    </div>
  </div>
{/if}
