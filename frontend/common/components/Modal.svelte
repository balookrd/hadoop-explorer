<script lang="ts">
  import type { Snippet } from 'svelte';
  import { X } from 'lucide-svelte';

  interface Props {
    isOpen: boolean;
    title?: string;
    subtitle?: string;
    icon?: any;
    maxWidth?: string;
    onClose: () => void;
    header?: Snippet;
    children?: Snippet;
    footer?: Snippet;
  }

  let {
    isOpen,
    title,
    subtitle,
    icon: IconComponent,
    maxWidth = 'max-w-lg',
    onClose,
    header,
    children,
    footer,
  }: Props = $props();

  function handleBackdropClick(e: MouseEvent) {
    if (e.target === e.currentTarget) {
      onClose();
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'Escape' && isOpen) {
      onClose();
    }
  }
</script>

<svelte:window onkeydown={handleKeydown} />

{#if isOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={handleBackdropClick}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl w-full {maxWidth} border border-slate-200 dark:border-slate-800 flex flex-col select-auto max-h-[90vh] overflow-hidden"
      onclick={(e) => e.stopPropagation()}
    >
      {#if header}
        {@render header()}
      {:else if title}
        <div class="px-6 py-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between shrink-0">
          <div class="flex items-center gap-3">
            {#if IconComponent}
              <div class="w-9 h-9 rounded-xl bg-sky-50 dark:bg-sky-950/50 text-sky-600 dark:text-sky-400 flex items-center justify-center border border-sky-100 dark:border-sky-800">
                <IconComponent class="w-5 h-5" />
              </div>
            {/if}
            <div>
              <h3 class="text-base font-bold text-slate-800 dark:text-slate-100">{title}</h3>
              {#if subtitle}
                <p class="text-xs text-slate-500 dark:text-slate-400">{subtitle}</p>
              {/if}
            </div>
          </div>
          <button
            onclick={onClose}
            class="p-1 rounded-lg text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
          >
            <X class="w-4 h-4" />
          </button>
        </div>
      {/if}

      <div class="p-6 overflow-y-auto flex-1 text-slate-800 dark:text-slate-200">
        {#if children}
          {@render children()}
        {/if}
      </div>

      {#if footer}
        <div class="px-6 py-3.5 bg-slate-50 dark:bg-slate-950 border-t border-slate-100 dark:border-slate-800 flex items-center justify-end gap-2 shrink-0">
          {@render footer()}
        </div>
      {/if}
    </div>
  </div>
{/if}
