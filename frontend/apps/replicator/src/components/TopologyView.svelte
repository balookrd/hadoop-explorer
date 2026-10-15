<script lang="ts">
  import type { TopologyData, AgentInfo, StreamingLeaseStatus } from '../types';
  import {
    Layers,
    RefreshCw,
    Shield,
    Check,
    HardDrive,
    Gauge,
    Server,
    AlertCircle,
    Radio
  } from 'lucide-svelte';

  interface Props {
    topology: TopologyData | null;
    topologyLoading: boolean;
    registeredAgents: AgentInfo[];
    agentsLoading: boolean;
    streamingLease: StreamingLeaseStatus | null;
    streamingLeases: Record<string, StreamingLeaseStatus>;
    isAdmin: boolean;
    onRefreshTopology: () => void;
    onRefreshAgents: () => void;
    onSaveGlobalLimit: (limitBytesPerSec: number) => Promise<void>;
    onSaveDcLimit: (srcDc: string, dstDc: string, mbPerSec: number) => Promise<void>;
    onSaveHdfsLimit: (srcCluster: string, dstCluster: string, mbPerSec: number) => Promise<void>;
  }

  let {
    topology,
    topologyLoading,
    registeredAgents,
    agentsLoading,
    streamingLease,
    streamingLeases,
    isAdmin,
    onRefreshTopology,
    onRefreshAgents,
    onSaveGlobalLimit,
    onSaveDcLimit,
    onSaveHdfsLimit
  }: Props = $props();

  let topologyStatusMsg = $state<string | null>(null);
  let globalLimitMb = $state<number>(100);
  let globalUnlimited = $state<boolean>(false);
  let dcLimits = $state<Record<string, { mb: number; unlimited: boolean }>>({});
  let hdfsLimits = $state<Record<string, { mb: number; unlimited: boolean }>>({});

  const onlineStreamers = $derived(
    registeredAgents.filter((a) => a.mode === 'streamer' && a.status === 'online')
  );

  $effect(() => {
    if (topology) {
      const gMb = topology.global_limit_bytes_per_sec / (1024 * 1024);
      globalLimitMb = Math.round(gMb);
      globalUnlimited = topology.global_limit_bytes_per_sec === 0;

      const nextDc: Record<string, { mb: number; unlimited: boolean }> = {};
      for (const item of topology.dc_limits || []) {
        const key = `${item.source_dc}->${item.target_dc}`;
        nextDc[key] = {
          mb: Math.round(item.limit_mb_per_sec),
          unlimited: item.limit_mb_per_sec === 0 || item.limit_bytes_per_sec === 0,
        };
      }
      dcLimits = nextDc;

      const nextHdfs: Record<string, { mb: number; unlimited: boolean }> = {};
      for (const item of topology.hdfs_limits || []) {
        const key = `${item.source_cluster}->${item.target_cluster}`;
        nextHdfs[key] = {
          mb: Math.round(item.limit_mb_per_sec),
          unlimited: item.limit_mb_per_sec === 0 || item.limit_bytes_per_sec === 0,
        };
      }
      hdfsLimits = nextHdfs;
    }
  });

  async function handleSaveGlobalLimit() {
    topologyStatusMsg = null;
    const limitMb = globalUnlimited ? 0 : Number(globalLimitMb) || 0;
    try {
      await onSaveGlobalLimit(limitMb * 1024 * 1024);
      topologyStatusMsg = 'Глобальный лимит WAN успешно обновлен';
    } catch (e: any) {
      topologyStatusMsg = `Ошибка: ${e.message || 'Не удалось сохранить'}`;
    }
  }

  async function handleSaveDcLimit(key: string) {
    topologyStatusMsg = null;
    const parts = key.split('->');
    if (parts.length !== 2) return;
    const item = dcLimits[key];
    const mb = item.unlimited ? 0 : Number(item.mb) || 0;

    try {
      await onSaveDcLimit(parts[0], parts[1], mb);
      topologyStatusMsg = `Лимит для канала ${key} успешно обновлен`;
    } catch (e: any) {
      topologyStatusMsg = `Ошибка: ${e.message || 'Не удалось сохранить'}`;
    }
  }

  async function handleSaveHdfsLimit(key: string) {
    topologyStatusMsg = null;
    const parts = key.split('->');
    if (parts.length !== 2) return;
    const item = hdfsLimits[key];
    const mb = item.unlimited ? 0 : Number(item.mb) || 0;

    try {
      await onSaveHdfsLimit(parts[0], parts[1], mb);
      topologyStatusMsg = `Лимит для кластеров ${key} успешно обновлен`;
    } catch (e: any) {
      topologyStatusMsg = `Ошибка: ${e.message || 'Не удалось сохранить'}`;
    }
  }

  function formatBytes(bytes: number): string {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  function formatDcLabel(key: string): string {
    const parts = key.split('->');
    if (parts.length !== 2) return key;
    const d1 = topology?.datacenters.find((d) => d.id === parts[0]);
    const d2 = topology?.datacenters.find((d) => d.id === parts[1]);
    const name1 = d1 ? d1.id.toUpperCase() : parts[0].toUpperCase();
    const name2 = d2 ? d2.id.toUpperCase() : parts[1].toUpperCase();
    return `${name1} → ${name2}`;
  }

  function formatDcSub(key: string): string {
    const parts = key.split('->');
    if (parts.length !== 2) return 'магистраль ЦОД';
    const d1 = topology?.datacenters.find((d) => d.id === parts[0]);
    const d2 = topology?.datacenters.find((d) => d.id === parts[1]);
    const loc1 = d1?.name ? d1.name.split(' ')[0] + ' ' + d1.name.split(' ')[1] : parts[0];
    const loc2 = d2?.name ? d2.name.split(' ')[0] + ' ' + d2.name.split(' ')[1] : parts[1];
    return `${loc1} → ${loc2}`;
  }

  function formatHdfsLabel(key: string): string {
    const parts = key.split('->');
    if (parts.length !== 2) return key;
    const c1 = topology?.clusters.find((c) => c.id === parts[0]);
    const c2 = topology?.clusters.find((c) => c.id === parts[1]);
    const name1 = c1 ? c1.name : parts[0];
    const name2 = c2 ? c2.name : parts[1];
    return `${name1} → ${name2}`;
  }
</script>

<main class="flex-1 w-full px-4 sm:px-6 py-5 space-y-6 pb-20">
  <!-- Заголовок и статус -->
  <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
    <div>
      <h2 class="text-lg font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
        <Layers class="w-5 h-5 text-sky-600" />
        Топология ЦОД и Управление полосой WAN
      </h2>
      <p class="text-xs text-slate-500 dark:text-slate-400">
        Настройка ограничений сетевого канала между Дата-центрами (DC-DC) и кластерами HDFS в рантайме.
      </p>
    </div>
    <button
      onclick={onRefreshTopology}
      class="flex items-center gap-1.5 px-3 py-1.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-xs font-medium text-slate-600 dark:text-slate-300 transition cursor-pointer self-start sm:self-auto shadow-2xs"
    >
      <RefreshCw class="w-3.5 h-3.5 {topologyLoading ? 'animate-spin' : ''}" />
      <span>Обновить топологию</span>
    </button>
  </div>

  {#if !isAdmin}
    <!-- Предупреждение для не-админов -->
    <div class="p-3 bg-amber-50 dark:bg-amber-950/40 border border-amber-200 dark:border-amber-800 rounded-xl flex items-center gap-2.5 text-xs text-amber-800 dark:text-amber-200 font-medium">
      <Shield class="w-4 h-4 text-amber-600 shrink-0" />
      <span>Режим только для чтения: изменять сетевые лимиты и топологию каналов могут только администраторы платформы (ADM).</span>
    </div>
  {/if}

  {#if topologyStatusMsg}
    <div class="p-3 bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800 rounded-xl flex items-center gap-2.5 text-xs text-emerald-800 dark:text-emerald-200 font-medium">
      <Check class="w-4 h-4 text-emerald-600 shrink-0" />
      <span>{topologyStatusMsg}</span>
    </div>
  {/if}

  <!-- 1. Дата-центры и привязка кластеров (2 ЦОД по 50% экрана) -->
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
    {#if topology && topology.datacenters}
      {#each topology.datacenters as dc}
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs flex flex-col justify-between">
          <div>
            <div class="flex items-center justify-between mb-2">
              <span class="px-2 py-0.5 rounded font-mono font-bold text-xs bg-sky-50 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300 border border-sky-200 dark:border-sky-800">
                {dc.id.toUpperCase()}
              </span>
              <span class="text-[11px] text-slate-400 font-medium">{dc.location}</span>
            </div>
            <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">{dc.name}</h3>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{dc.description}</p>
          </div>

          <!-- Кластеры в этом ЦОД -->
          <div class="mt-4 pt-3 border-t border-slate-100 dark:border-slate-800">
            <div class="flex items-center justify-between mb-2">
              <span class="text-[10px] font-semibold uppercase text-slate-400 tracking-wider">
                Кластеры HDFS ({(topology.clusters || []).filter((c) => c.dc_id === dc.id).length}):
              </span>
            </div>
            <div class="space-y-2">
              {#each (topology.clusters || []).filter((c) => c.dc_id === dc.id) as cluster}
                <div class="flex items-center justify-between p-2.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/60 dark:border-slate-800 text-xs">
                  <div class="flex items-center gap-2 font-medium">
                    <HardDrive class="w-4 h-4 text-sky-500 shrink-0" />
                    <div class="flex flex-col">
                      <span class="font-semibold text-slate-800 dark:text-slate-200">{cluster.name}</span>
                      <span class="text-[10px] text-slate-400 font-mono">{cluster.default_path || '/'}</span>
                    </div>
                  </div>
                  <span class="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-200/60 dark:bg-slate-800 text-slate-600 dark:text-slate-300 shrink-0">
                    {cluster.id}
                  </span>
                </div>
              {/each}
            </div>
          </div>
        </div>
      {/each}
    {/if}
  </div>

  <!-- 2. Глобальный лимит WAN (Token Bucket Throttler) -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
    <div class="flex items-center gap-2 mb-3">
      <Gauge class="w-5 h-5 text-sky-600" />
      <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">Глобальный лимит сетевого канала (Global WAN Throttler)</h3>
    </div>
    <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">
      Максимальная совокупная скорость передачи данных по всей платформе репликации между всеми воркерами.
    </p>

    <div class="flex flex-wrap items-center gap-4 bg-slate-50 dark:bg-slate-950/60 p-4 rounded-xl border border-slate-200/80 dark:border-slate-800">
      <div class="flex items-center gap-2">
        <label class="text-xs font-semibold text-slate-700 dark:text-slate-300">Лимит полосы:</label>
        <input
          type="number"
          min="0"
          step="5"
          disabled={!isAdmin || globalUnlimited}
          bind:value={globalLimitMb}
          class="w-24 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg text-xs font-mono font-bold focus:outline-none focus:ring-1 focus:ring-sky-500 disabled:opacity-50"
        />
        <span class="text-xs text-slate-500 font-medium">МБ/с</span>
      </div>

      <label class="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300 cursor-pointer select-none">
        <input
          type="checkbox"
          disabled={!isAdmin}
          bind:checked={globalUnlimited}
          class="rounded border-slate-300 text-sky-600 focus:ring-sky-500 cursor-pointer disabled:opacity-50"
        />
        <span>Без лимита (Unlimited / 0 МБ/с)</span>
      </label>

      {#if isAdmin}
        <button
          onclick={handleSaveGlobalLimit}
          class="px-4 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-xs transition cursor-pointer"
        >
          Сохранить
        </button>
      {/if}
    </div>
  </div>

  <!-- 3. Матрица лимитов между ЦОД (DC-DC WAN) -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
    <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100 mb-1">Ограничения между ЦОД (DC-DC WAN Channels)</h3>
    <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">Индивидуальные ограничения магистральных каналов связи между дата-центрами.</p>

    {#if Object.keys(dcLimits).length === 0}
      <div class="py-6 text-center text-xs text-slate-400 dark:text-slate-500">
        Каналы между ЦОД не обнаружены в топологии
      </div>
    {:else}
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {#each Object.keys(dcLimits) as key}
          <div class="p-3.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 flex flex-col justify-between gap-3 shadow-2xs">
            <div class="flex items-center justify-between">
              <span class="font-mono font-bold text-xs text-slate-800 dark:text-slate-200 uppercase">{formatDcLabel(key)}</span>
              <span class="text-[10px] text-slate-400">{formatDcSub(key)}</span>
            </div>
            <div class="flex items-center gap-2">
              <input
                type="number"
                min="0"
                step="5"
                disabled={!isAdmin || dcLimits[key].unlimited}
                bind:value={dcLimits[key].mb}
                class="w-20 px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold disabled:opacity-50"
              />
              <span class="text-xs text-slate-500">МБ/с</span>
              <label class="flex items-center gap-1.5 text-[11px] text-slate-600 dark:text-slate-300 ml-auto cursor-pointer select-none">
                <input
                  type="checkbox"
                  disabled={!isAdmin}
                  bind:checked={dcLimits[key].unlimited}
                  class="rounded border-slate-300 text-sky-600 focus:ring-sky-500 cursor-pointer disabled:opacity-50"
                />
                <span>Без лимита</span>
              </label>
              {#if isAdmin}
                <button
                  onclick={() => handleSaveDcLimit(key)}
                  class="px-2.5 py-1 rounded-md bg-sky-600 hover:bg-sky-500 text-white text-[11px] font-semibold transition cursor-pointer shadow-2xs shrink-0"
                  title="Сохранить лимит канала"
                >
                  Сохранить
                </button>
              {/if}
            </div>
          </div>
        {/each}
      </div>
    {/if}
  </div>

  <!-- 4. Ограничения каналов между кластерами HDFS-HDFS -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
    <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100 mb-1">Лимиты каналов HDFS-HDFS</h3>
    <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">Тонкая настройка полосы репликации между конкретными парами NameNode.</p>

    {#if Object.keys(hdfsLimits).length === 0}
      <div class="py-6 text-center text-xs text-slate-400 dark:text-slate-500">
        Каналы между кластерами HDFS не обнаружены в топологии
      </div>
    {:else}
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {#each Object.keys(hdfsLimits) as key}
          <div class="p-3.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 flex flex-col justify-between gap-3 shadow-2xs">
            <div class="flex items-center justify-between">
              <span class="font-mono font-bold text-xs text-slate-800 dark:text-slate-200 truncate pr-2" title={formatHdfsLabel(key)}>
                {formatHdfsLabel(key)}
              </span>
              <span class="text-[10px] font-mono text-slate-400 shrink-0">{key}</span>
            </div>
            <div class="flex items-center gap-2">
              <input
                type="number"
                min="0"
                step="5"
                disabled={!isAdmin || hdfsLimits[key].unlimited}
                bind:value={hdfsLimits[key].mb}
                class="w-20 px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold disabled:opacity-50"
              />
              <span class="text-xs text-slate-500">МБ/с</span>
              <label class="flex items-center gap-1.5 text-[11px] text-slate-600 dark:text-slate-300 ml-auto cursor-pointer select-none">
                <input
                  type="checkbox"
                  disabled={!isAdmin}
                  bind:checked={hdfsLimits[key].unlimited}
                  class="rounded border-slate-300 text-sky-600 focus:ring-sky-500 cursor-pointer disabled:opacity-50"
                />
                <span>Без лимита</span>
              </label>
              {#if isAdmin}
                <button
                  onclick={() => handleSaveHdfsLimit(key)}
                  class="px-2.5 py-1 rounded-md bg-sky-600 hover:bg-sky-500 text-white text-[11px] font-semibold transition cursor-pointer shadow-2xs shrink-0"
                  title="Сохранить лимит канала"
                >
                  Сохранить
                </button>
              {/if}
            </div>
          </div>
        {/each}
      </div>
    {/if}
  </div>

  <!-- 5. Динамически зарегистрированные агенты (Keepalive Registry) -->
  <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs">
    <div class="flex items-center justify-between mb-2">
      <div class="flex items-center gap-2">
        <Server class="w-5 h-5 text-sky-600" />
        <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">
          Зарегистрированные агенты репликации (Keepalive Registry)
        </h3>
      </div>
      <div class="flex items-center gap-2">
        <span class="text-xs text-slate-500 font-medium">
          Онлайн: {registeredAgents.filter((a) => a.status === 'online').length} из {registeredAgents.length}
        </span>
        <button
          onclick={onRefreshAgents}
          class="p-1 rounded-lg border border-slate-200 dark:border-slate-800 hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-500 cursor-pointer shadow-2xs"
          title="Обновить список агентов"
        >
          <RefreshCw class="w-3.5 h-3.5 {agentsLoading ? 'animate-spin' : ''}" />
        </button>
      </div>
    </div>
    <p class="text-xs text-slate-500 dark:text-slate-400 mb-4">
      Агенты динамически регистрируются на Оркестраторе при старте, передают свой внешний gRPC адрес и подтверждают жизнеспособность через keepalive-пинги (таймаут 15 сек).
    </p>

    <!-- Баннер отказоустойчивости Inotify Streamers (HA) -->
    {#if topology?.streaming_enabled || onlineStreamers.length > 0}
      {#if onlineStreamers.length < 2}
        <div class="mb-4 p-3 bg-amber-50 dark:bg-amber-950/40 border border-amber-300 dark:border-amber-800 rounded-xl flex items-start gap-3 text-xs text-amber-800 dark:text-amber-300">
          <AlertCircle class="w-4 h-4 text-amber-600 dark:text-amber-400 mt-0.5 shrink-0" />
          <div class="space-y-0.5">
            <div class="font-bold flex items-center gap-1.5">
              <span>Предупреждение отказоустойчивости стриминга (NO_REDUNDANCY)</span>
              <span class="px-1.5 py-0.2 rounded text-[10px] font-mono bg-amber-200 dark:bg-amber-900/80 text-amber-900 dark:text-amber-200">
                Онлайн: {onlineStreamers.length} из 2 требуемых
              </span>
            </div>
            <p class="text-[11px] text-amber-700/90 dark:text-amber-300/80">
              Для обеспечения отказоустойчивости потоковой репликации HDFS Inotify и автоматического failover запустите как минимум 2 экземпляра стримера (<code class="font-mono">AGENT_MODE=streamer</code>).
            </p>
          </div>
        </div>
      {:else}
        <div class="mb-4 p-3 bg-emerald-50/70 dark:bg-emerald-950/40 border border-emerald-300 dark:border-emerald-800 rounded-xl flex items-center justify-between text-xs text-emerald-800 dark:text-emerald-300">
          <div class="flex items-center gap-2.5">
            <Shield class="w-4 h-4 text-emerald-600 dark:text-emerald-400 shrink-0" />
            <div>
              <span class="font-bold">Высокая доступность Inotify Streamers (HA): Active-Standby кластер активен</span>
              <span class="text-[11px] text-emerald-700/90 dark:text-emerald-300/80 block">
                Онлайн: {onlineStreamers.length} стримера в кластерах ({Object.keys(streamingLeases).join(', ') || 'dc1'}) · Готовность к прямому потоку и обратной синхронизации при Failover
              </span>
            </div>
          </div>
          <span class="px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-100 dark:bg-emerald-900 text-emerald-800 dark:text-emerald-200 border border-emerald-300 dark:border-emerald-700">
            HEALTHY HA
          </span>
        </div>
      {/if}
    {/if}

    {#if registeredAgents.length === 0}
      <div class="py-8 text-center text-xs text-slate-400 dark:text-slate-500 border border-dashed border-slate-200 dark:border-slate-800 rounded-xl">
        Нет активных зарегистрированных агентов. При запуске агенты `backend.replicator.agent` автоматически появятся здесь.
      </div>
    {:else}
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {#each registeredAgents as agent}
          <div class="p-3.5 rounded-xl bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 flex flex-col justify-between gap-3 shadow-2xs">
            <div class="flex items-center justify-between">
              <div class="flex flex-col">
                <span class="font-mono font-bold text-xs text-slate-800 dark:text-slate-200">
                  {agent.agent_id}
                </span>
                {#if agent.mode === 'streamer'}
                  {@const agentClusterLease = streamingLeases[agent.cluster_id || 'dc1'] || streamingLease}
                  <div class="flex items-center gap-1.5 mt-1">
                    <span class="inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[10px] font-bold bg-purple-50 dark:bg-purple-950/70 text-purple-700 dark:text-purple-300 border border-purple-300 dark:border-purple-800">
                      <Radio class="w-2.5 h-2.5 text-purple-600 dark:text-purple-400" />
                      <span>Inotify Streamer</span>
                    </span>
                    {#if (agentClusterLease?.active_agent_id || agentClusterLease?.active_streamer_id) === agent.agent_id}
                      <span class="px-1.5 py-0.2 rounded text-[9px] font-mono font-bold bg-emerald-100 dark:bg-emerald-950/80 text-emerald-800 dark:text-emerald-300 border border-emerald-300 dark:border-emerald-700">
                        ACTIVE (Ep {agentClusterLease?.epoch ?? 1})
                      </span>
                    {:else}
                      <span class="px-1.5 py-0.2 rounded text-[9px] font-mono font-medium bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300">
                        STANDBY
                      </span>
                    {/if}
                  </div>
                {/if}
              </div>
              {#if agent.status === 'online'}
                <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-50 dark:bg-emerald-950/60 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800">
                  <span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
                  ONLINE
                </span>
              {:else if agent.status === 'stale'}
                <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-amber-50 dark:bg-amber-950/60 text-amber-700 dark:text-amber-300 border border-amber-200 dark:border-amber-800">
                  <span class="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
                  STALE ({agent.heartbeat_age_seconds}с)
                </span>
              {:else}
                <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 border border-slate-200 dark:border-slate-700">
                  OFFLINE
                </span>
              {/if}
            </div>

            <div class="space-y-1 text-[11px]">
              <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                <span class="text-slate-400">Кластер:</span>
                <span class="font-mono font-medium">{agent.cluster_id || 'любой'}</span>
              </div>
              <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                <span class="text-slate-400">gRPC адрес:</span>
                <span class="font-mono font-semibold text-sky-600 dark:text-sky-400">
                  {agent.grpc_address || '—'}
                </span>
              </div>
              <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                <span class="text-slate-400">Режим / Нагрузка:</span>
                <span class="font-mono">
                  {#if agent.mode === 'streamer'}
                    <span class="text-purple-600 dark:text-purple-400 font-semibold">Streamer (изолирован от задач)</span>
                  {:else}
                    {agent.mode} (активно: {agent.active_transfers})
                  {/if}
                </span>
              </div>
              <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                <span class="text-slate-400">Лимит полосы:</span>
                <span class="font-mono {agent.max_bandwidth_mb_s ? 'text-amber-600 dark:text-amber-400 font-semibold' : 'text-slate-400'}">
                  {agent.max_bandwidth_mb_s ? `${agent.max_bandwidth_mb_s} МБ/с` : 'без ограничений'}
                </span>
              </div>
              <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                <span class="text-slate-400">Keepalive:</span>
                <span class="text-[10px] text-slate-400 font-mono">
                  {agent.heartbeat_age_seconds < 5 ? 'только что' : `${agent.heartbeat_age_seconds}с назад`}
                </span>
              </div>

              <!-- Enterprise телеметрия Micrometer (Сжатие, Latency, Staging) -->
              <div class="mt-2 pt-2 border-t border-slate-200/60 dark:border-slate-800/80 space-y-1">
                <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                  <span class="text-slate-400">Сжатие (Wire):</span>
                  <span class="font-mono flex items-center gap-1">
                    <span class="px-1 py-0.2 rounded text-[10px] font-bold {agent.compression_ratio && agent.compression_ratio > 1.2 ? 'bg-emerald-50 dark:bg-emerald-950/70 text-emerald-700 dark:text-emerald-300 border border-emerald-300 dark:border-emerald-800' : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400'}">
                      x{agent.compression_ratio ? agent.compression_ratio.toFixed(2) : '1.00'}
                    </span>
                    {#if (agent.compression_raw_bytes || 0) > 0}
                      <span class="text-[10px] text-slate-400">
                        ({formatBytes(agent.compression_compressed_bytes || 0)} / {formatBytes(agent.compression_raw_bytes || 0)})
                      </span>
                    {/if}
                  </span>
                </div>
                <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                  <span class="text-slate-400">Задержка gRPC:</span>
                  <span class="font-mono">
                    {#if agent.chunk_latency_avg_ms !== undefined && agent.chunk_latency_avg_ms > 0}
                      <span class="text-sky-600 dark:text-sky-400 font-semibold">{agent.chunk_latency_avg_ms.toFixed(1)} мс</span>
                      <span class="text-[10px] text-slate-400">(пик {agent.chunk_latency_max_ms?.toFixed(1) || '0'} мс)</span>
                    {:else}
                      <span class="text-slate-400">—</span>
                    {/if}
                  </span>
                </div>
                <div class="flex items-center justify-between text-slate-600 dark:text-slate-300">
                  <span class="text-slate-400">Staging буфер:</span>
                  <span class="font-mono">
                    {#if (agent.staging_active_files || 0) > 0 || (agent.staging_active_bytes || 0) > 0}
                      <span class="text-amber-600 dark:text-amber-400 font-semibold">{agent.staging_active_files || 0} файлов</span>
                      <span class="text-[10px] text-slate-400">({formatBytes(agent.staging_active_bytes || 0)})</span>
                    {:else}
                      <span class="text-slate-400">0 файлов</span>
                    {/if}
                  </span>
                </div>
                {#if (agent.staging_orphans_cleaned || 0) > 0}
                  <div class="flex items-center justify-between text-[10px] text-emerald-600 dark:text-emerald-400">
                    <span>Reaper очистил сирот:</span>
                    <span class="font-mono font-semibold">{agent.staging_orphans_cleaned} файлов</span>
                  </div>
                {/if}
              </div>
            </div>
          </div>
        {/each}
      </div>
    {/if}
  </div>
</main>
