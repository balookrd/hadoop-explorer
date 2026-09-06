<script lang="ts">
  interface Props {
    status: 'success' | 'warning' | 'error' | 'info' | 'running' | 'queued' | string;
    text?: string;
  }

  let { status, text }: Props = $props();

  const colorMap: Record<string, string> = {
    success: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    finished: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    warning: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
    error: 'bg-rose-500/10 text-rose-400 border-rose-500/20',
    failed: 'bg-rose-500/10 text-rose-400 border-rose-500/20',
    info: 'bg-blue-500/10 text-blue-400 border-blue-500/20',
    running: 'bg-sky-500/10 text-sky-400 border-sky-500/20 animate-pulse',
    queued: 'bg-purple-500/10 text-purple-400 border-purple-500/20',
  };

  const badgeClass = $derived(
    colorMap[status.toLowerCase()] || 'bg-slate-500/10 text-slate-400 border-slate-500/20'
  );
</script>

<span class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border {badgeClass}">
  <span class="w-1.5 h-1.5 rounded-full bg-current"></span>
  {text || status}
</span>
