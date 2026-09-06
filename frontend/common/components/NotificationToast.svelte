<script lang="ts">
  interface Props {
    type?: 'success' | 'error' | 'info';
    message: string;
    onClose?: () => void;
  }

  let { type = 'info', message, onClose }: Props = $props();
</script>

<div class="fixed bottom-4 right-4 z-50 flex items-center gap-3 px-4 py-3 rounded-lg shadow-xl border text-sm font-medium backdrop-blur-md transition-all
  {type === 'success' ? 'bg-emerald-950/90 border-emerald-800 text-emerald-200' : ''}
  {type === 'error' ? 'bg-rose-950/90 border-rose-800 text-rose-200' : ''}
  {type === 'info' ? 'bg-slate-900/90 border-slate-700 text-slate-200' : ''}">
  <span>{message}</span>
  {#if onClose}
    <button onclick={onClose} class="opacity-70 hover:opacity-100 transition-opacity ml-2">
      ✕
    </button>
  {/if}
</div>
