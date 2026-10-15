<script lang="ts">
  import { onMount } from 'svelte';

  interface TopicSetting {
    topic: string;
    payloadMode: 'PASSTHROUGH' | 'AUTO_DETECT' | 'SCHEMA_REGISTRY';
  }

  interface KafkaJob {
    id: string;
    name: string;
    sourceDc: string;
    targetDc: string;
    direction: string;
    topics: TopicSetting[];
    consumerGroups: string[];
    schemaTranscodingEnabled: boolean;
    sourceSrType?: string;
    targetSrType?: string;
    sourceSrUrl?: string;
    targetSrUrl?: string;
    sourceSrKerberosEnabled?: boolean;
    targetSrKerberosEnabled?: boolean;
    bandwidthLimitMb: number;
    offsetSyncIntervalSec: number;
    sourceKerberosEnabled?: boolean;
    sourcePrincipal?: string;
    sourceKeytabPath?: string;
    targetKerberosEnabled?: boolean;
    targetPrincipal?: string;
    targetKeytabPath?: string;
    status: 'ACTIVE' | 'PAUSED' | 'ERROR';
    messagesReplicated: number;
    bytesReplicated: number;
    currentLagMs: number;
    lastErrorMessage?: string;
  }

  let jobs = $state<KafkaJob[]>([
    {
      id: 'kafka-job-orders',
      name: 'Межцодовая репликация Orders & Payments',
      sourceDc: 'DC-East (Москва)',
      targetDc: 'DC-West (СПб)',
      direction: 'BIDIRECTIONAL',
      topics: [
        { topic: 'orders-v1', payloadMode: 'AUTO_DETECT' },
        { topic: 'payments-raw', payloadMode: 'PASSTHROUGH' },
        { topic: 'user-events', payloadMode: 'SCHEMA_REGISTRY' }
      ],
      consumerGroups: ['billing-service-prod', 'fraud-detection-engine'],
      schemaTranscodingEnabled: true,
      sourceSrType: 'HORTONWORKS',
      targetSrType: 'CONFLUENT',
      sourceSrUrl: 'http://hwx-sr.dc1:7788',
      targetSrUrl: 'http://confluent-sr.dc2:8081',
      sourceSrKerberosEnabled: true,
      targetSrKerberosEnabled: true,
      sourceKerberosEnabled: true,
      sourcePrincipal: 'kafka-replicator@DC1.CORP',
      sourceKeytabPath: '/etc/security/keytabs/kafka-replicator.keytab',
      targetKerberosEnabled: true,
      targetPrincipal: 'kafka-replicator@DC2.CORP',
      targetKeytabPath: '/etc/security/keytabs/kafka-replicator.keytab',
      bandwidthLimitMb: 60.0,
      offsetSyncIntervalSec: 5,
      status: 'ACTIVE',
      messagesReplicated: 1845200,
      bytesReplicated: 924500000,
      currentLagMs: 8
    }
  ]);

  let isCreateModalOpen = $state(false);
  let availableTopics = $state<Array<{ name: string; partitions: number; detectedFormat: string; hasSchema: boolean }>>([
    { name: 'orders-v1', partitions: 12, detectedFormat: 'AUTO_DETECT', hasSchema: true },
    { name: 'payments-raw', partitions: 6, detectedFormat: 'PASSTHROUGH', hasSchema: false },
    { name: 'user-events', partitions: 8, detectedFormat: 'AUTO_DETECT', hasSchema: true },
    { name: 'system-audit-logs', partitions: 4, detectedFormat: 'PASSTHROUGH', hasSchema: false },
    { name: 'telemetry-stream', partitions: 16, detectedFormat: 'PASSTHROUGH', hasSchema: false }
  ]);

  let availableGroups = $state<Array<{ groupId: string; state: string; members: number; lag: number }>>([
    { groupId: 'billing-service-prod', state: 'EMPTY', members: 0, lag: 0 },
    { groupId: 'fraud-detection-engine', state: 'STABLE', members: 3, lag: 4 },
    { groupId: 'audit-archiver-service', state: 'EMPTY', members: 0, lag: 120 }
  ]);

  // Поля формы создания
  let newJobName = $state('Репликация Kafka DC1 <-> DC2');
  let newSourceDc = $state('DC-East (Москва)');
  let newTargetDc = $state('DC-West (СПб)');
  let newDirection = $state<'BIDIRECTIONAL' | 'UNIDIRECTIONAL'>('BIDIRECTIONAL');
  let selectedTopics = $state<TopicSetting[]>([
    { topic: 'orders-v1', payloadMode: 'AUTO_DETECT' },
    { topic: 'payments-raw', payloadMode: 'PASSTHROUGH' }
  ]);
  let selectedGroups = $state<string[]>(['billing-service-prod']);
  let enableSchemaTranscoding = $state(true);
  let sourceSrType = $state('HORTONWORKS');
  let targetSrType = $state('CONFLUENT');
  let sourceSrUrl = $state('http://hwx-sr:7788');
  let targetSrUrl = $state('http://confluent-sr:8081');
  let enableSrKerberos = $state(true);

  // Kerberos поля формы
  let enableKerberos = $state(true);
  let sourcePrincipal = $state('kafka-replicator@DC1.CORP');
  let sourceKeytabPath = $state('/etc/security/keytabs/kafka-replicator.keytab');
  let targetPrincipal = $state('kafka-replicator@DC2.CORP');
  let targetKeytabPath = $state('/etc/security/keytabs/kafka-replicator.keytab');

  let bandwidthLimitMb = $state(50);
  let offsetSyncIntervalSec = $state(5);

  let totalMessages = $derived(jobs.reduce((acc, j) => acc + (j.messagesReplicated || 0), 0));
  let totalBytesMb = $derived((jobs.reduce((acc, j) => acc + (j.bytesReplicated || 0), 0) / (1024 * 1024)).toFixed(1));

  function toggleTopic(topicName: string, defaultFormat: string) {
    const idx = selectedTopics.findIndex(t => t.topic === topicName);
    if (idx >= 0) {
      selectedTopics = selectedTopics.filter(t => t.topic !== topicName);
    } else {
      selectedTopics = [...selectedTopics, { topic: topicName, payloadMode: (defaultFormat as any) || 'AUTO_DETECT' }];
    }
  }

  function changeTopicMode(topicName: string, mode: 'PASSTHROUGH' | 'AUTO_DETECT' | 'SCHEMA_REGISTRY') {
    selectedTopics = selectedTopics.map(t => t.topic === topicName ? { ...t, payloadMode: mode } : t);
  }

  function toggleGroup(groupId: string) {
    if (selectedGroups.includes(groupId)) {
      selectedGroups = selectedGroups.filter(g => g !== groupId);
    } else {
      selectedGroups = [...selectedGroups, groupId];
    }
  }

  function handleCreateJob() {
    const newJob: KafkaJob = {
      id: `kafka-job-${Date.now().toString(36)}`,
      name: newJobName,
      sourceDc: newSourceDc,
      targetDc: newTargetDc,
      direction: newDirection,
      topics: [...selectedTopics],
      consumerGroups: [...selectedGroups],
      schemaTranscodingEnabled: enableSchemaTranscoding,
      sourceSrType,
      targetSrType,
      sourceSrUrl,
      targetSrUrl,
      sourceSrKerberosEnabled: enableSchemaTranscoding && enableSrKerberos,
      targetSrKerberosEnabled: enableSchemaTranscoding && enableSrKerberos,
      sourceKerberosEnabled: enableKerberos,
      sourcePrincipal,
      sourceKeytabPath,
      targetKerberosEnabled: enableKerberos,
      targetPrincipal,
      targetKeytabPath,
      bandwidthLimitMb,
      offsetSyncIntervalSec,
      status: 'ACTIVE',
      messagesReplicated: 0,
      bytesReplicated: 0,
      currentLagMs: 0
    };

    jobs = [newJob, ...jobs];
    isCreateModalOpen = false;
  }

  function toggleJobStatus(job: KafkaJob) {
    jobs = jobs.map(j => {
      if (j.id === job.id) {
        return { ...j, status: j.status === 'ACTIVE' ? 'PAUSED' : 'ACTIVE' };
      }
      return j;
    });
  }

  function triggerFailover(job: KafkaJob) {
    if (confirm(`Инициировать принудительный Failover смещений для задачи "${job.name}"?`)) {
      alert(`Смещения консьюмер-групп форсированно применены в ${job.targetDc}. Трафик переключен.`);
    }
  }

  function deleteJob(id: string) {
    if (confirm('Удалить эту задачу репликации Kafka?')) {
      jobs = jobs.filter(j => j.id !== id);
    }
  }
</script>

<div class="space-y-6">
  <!-- ВЕРХНЯЯ ПАНЕЛЬ СТАТИСТИКИ -->
  <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
    <div>
      <h2 class="text-xl font-bold text-slate-900 dark:text-white flex items-center gap-2">
        <span class="p-1.5 rounded-lg bg-emerald-500/10 text-emerald-600 dark:text-emerald-400">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
        </span>
        Kafka gRPC Replicator
      </h2>
      <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
        Двунаправленная репликация топиков, трансляция смещений консьюмер-групп и транскодинг Schema Registry
      </p>
    </div>

    <button
      type="button"
      onclick={() => (isCreateModalOpen = true)}
      class="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white font-medium text-sm transition shadow-sm cursor-pointer"
    >
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
      </svg>
      Новая репликация Kafka
    </button>
  </div>

  <!-- КАРТОЧКИ KPI -->
  <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
    <div class="p-3.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-2xs">
      <div class="text-xs text-slate-500 dark:text-slate-400">Активных задач</div>
      <div class="text-xl font-bold text-slate-900 dark:text-white mt-0.5">
        {jobs.filter(j => j.status === 'ACTIVE').length} <span class="text-xs font-normal text-slate-400">/ {jobs.length}</span>
      </div>
    </div>
    <div class="p-3.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-2xs">
      <div class="text-xs text-slate-500 dark:text-slate-400">Передано сообщений</div>
      <div class="text-xl font-bold text-slate-900 dark:text-white mt-0.5">
        {totalMessages.toLocaleString()}
      </div>
    </div>
    <div class="p-3.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-2xs">
      <div class="text-xs text-slate-500 dark:text-slate-400">Объем данных WAN</div>
      <div class="text-xl font-bold text-slate-900 dark:text-white mt-0.5">
        {totalBytesMb} <span class="text-xs font-normal text-slate-400">МБ</span>
      </div>
    </div>
    <div class="p-3.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-2xs">
      <div class="text-xs text-slate-500 dark:text-slate-400">Сквозной лаг (RTT)</div>
      <div class="text-xl font-bold text-emerald-600 dark:text-emerald-400 mt-0.5">
        &lt; 10 <span class="text-xs font-normal text-slate-400">мс</span>
      </div>
    </div>
  </div>

  <!-- СПИСОК ЗАДАЧ РЕПЛИКАЦИИ -->
  <div class="space-y-4">
    {#each jobs as job (job.id)}
      <div class="p-5 rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 shadow-sm space-y-4">
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <div class="space-y-1">
            <div class="flex items-center gap-2.5">
              <span class="w-2.5 h-2.5 rounded-full {job.status === 'ACTIVE' ? 'bg-emerald-500 animate-pulse' : 'bg-amber-500'}"></span>
              <h3 class="font-bold text-slate-900 dark:text-white text-base">{job.name}</h3>
              <span class="text-2xs font-semibold px-2 py-0.5 rounded-md {job.direction === 'BIDIRECTIONAL' ? 'bg-indigo-50 dark:bg-indigo-950/60 text-indigo-600 dark:text-indigo-400 border border-indigo-200 dark:border-indigo-800' : 'bg-slate-100 text-slate-700'}">
                {job.direction === 'BIDIRECTIONAL' ? '⇄ Active-Active' : '➔ Однонаправленная'}
              </span>
              {#if job.schemaTranscodingEnabled}
                <span class="text-2xs font-semibold px-2 py-0.5 rounded-md bg-purple-50 dark:bg-purple-950/60 text-purple-600 dark:text-purple-400 border border-purple-200 dark:border-purple-800">
                  SR: {job.sourceSrType} ➔ {job.targetSrType}
                </span>
              {/if}
              {#if job.sourceSrKerberosEnabled || job.targetSrKerberosEnabled}
                <span class="text-2xs font-semibold px-2 py-0.5 rounded-md bg-purple-100 dark:bg-purple-900/60 text-purple-800 dark:text-purple-300 border border-purple-300 dark:border-purple-700 flex items-center gap-1">
                  SR SPNEGO
                </span>
              {/if}
              {#if job.sourceKerberosEnabled}
                <span class="text-2xs font-semibold px-2 py-0.5 rounded-md bg-amber-50 dark:bg-amber-950/60 text-amber-700 dark:text-amber-400 border border-amber-200 dark:border-amber-800 flex items-center gap-1">
                  <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                  </svg>
                  Kerberos (GSSAPI)
                </span>
              {/if}
            </div>
            <div class="text-xs text-slate-500 flex items-center gap-2">
              <span>{job.sourceDc}</span>
              <span>⇄</span>
              <span>{job.targetDc}</span>
              <span>•</span>
              <span>Квота WAN: <strong>{job.bandwidthLimitMb} МБ/с</strong></span>
              <span>•</span>
              <span>Лаг: <strong class="text-emerald-600 dark:text-emerald-400">{job.currentLagMs} мс</strong></span>
            </div>
          </div>

          <!-- Кнопки действий -->
          <div class="flex items-center gap-2">
            <button
              type="button"
              onclick={() => toggleJobStatus(job)}
              class="px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800 text-xs font-medium text-slate-700 dark:text-slate-300 transition cursor-pointer"
            >
              {job.status === 'ACTIVE' ? 'Пауза' : 'Возобновить'}
            </button>
            <button
              type="button"
              onclick={() => triggerFailover(job)}
              class="px-3 py-1.5 rounded-lg border border-rose-200 dark:border-rose-900/60 bg-rose-50 dark:bg-rose-950/40 text-rose-600 dark:text-rose-400 hover:bg-rose-100 dark:hover:bg-rose-900/60 text-xs font-semibold transition cursor-pointer"
            >
              Failover групп
            </button>
            <button
              type="button"
              onclick={() => deleteJob(job.id)}
              class="p-1.5 text-slate-400 hover:text-rose-500 transition cursor-pointer"
              title="Удалить задачу"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </button>
          </div>
        </div>

        <!-- ТОПИКИ И РЕЖИМЫ СЕРИАЛИЗАЦИИ -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-3 pt-2 border-t border-slate-100 dark:border-slate-800">
          <div>
            <div class="text-xs font-semibold text-slate-500 dark:text-slate-400 mb-2">Реплицируемые топики:</div>
            <div class="flex flex-wrap gap-1.5">
              {#each job.topics as t}
                <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs bg-slate-100 dark:bg-slate-800 text-slate-800 dark:text-slate-200 border border-slate-200 dark:border-slate-700">
                  <span class="font-mono">{t.topic}</span>
                  <span class="text-3xs font-bold px-1.5 py-0.2 rounded {t.payloadMode === 'PASSTHROUGH' ? 'bg-amber-100 dark:bg-amber-950/80 text-amber-700 dark:text-amber-400' : 'bg-emerald-100 dark:bg-emerald-950/80 text-emerald-700 dark:text-emerald-400'}">
                    {t.payloadMode}
                  </span>
                </span>
              {/each}
            </div>
          </div>

          <div>
            <div class="text-xs font-semibold text-slate-500 dark:text-slate-400 mb-2">Синхронизируемые Consumer Groups:</div>
            <div class="flex flex-wrap gap-1.5">
              {#each job.consumerGroups as group}
                <span class="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs bg-slate-100 dark:bg-slate-800 text-slate-800 dark:text-slate-200 border border-slate-200 dark:border-slate-700">
                  <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
                  <span class="font-mono">{group}</span>
                  <span class="text-3xs text-emerald-600 dark:text-emerald-400 font-semibold">[Синхронизировано]</span>
                </span>
              {/each}
            </div>
          </div>
        </div>
      </div>
    {/each}
  </div>

  <!-- МОДАЛЬНОЕ ОКНО СОЗДАНИЯ ЗАДАЧИ -->
  {#if isCreateModalOpen}
    <div class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-xs">
      <div class="w-full max-w-2xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-xl overflow-hidden max-h-[90vh] flex flex-col">
        <div class="px-6 py-4 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between">
          <h3 class="text-base font-bold text-slate-900 dark:text-white">Создание задачи репликации Kafka</h3>
          <button
            type="button"
            onclick={() => (isCreateModalOpen = false)}
            class="p-1 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-400 hover:text-slate-600 cursor-pointer"
          >
            ✕
          </button>
        </div>

        <div class="p-6 space-y-4 overflow-y-auto flex-1">
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Название задачи</label>
            <input
              type="text"
              bind:value={newJobName}
              class="w-full px-3 py-2 rounded-xl border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 text-sm focus:outline-hidden focus:ring-2 focus:ring-emerald-500"
            />
          </div>

          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">ЦОД Источник</label>
              <input
                type="text"
                bind:value={newSourceDc}
                class="w-full px-3 py-2 rounded-xl border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 text-sm"
              />
            </div>
            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">ЦОД Приемник</label>
              <input
                type="text"
                bind:value={newTargetDc}
                class="w-full px-3 py-2 rounded-xl border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 text-sm"
              />
            </div>
          </div>

          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Режим топологии</label>
            <div class="grid grid-cols-2 gap-3">
              <label class="p-3 rounded-xl border cursor-pointer {newDirection === 'BIDIRECTIONAL' ? 'border-emerald-500 bg-emerald-50/40 dark:bg-emerald-950/30' : 'border-slate-200 dark:border-slate-700'}">
                <input type="radio" bind:group={newDirection} value="BIDIRECTIONAL" class="sr-only" />
                <div class="font-bold text-xs text-slate-900 dark:text-white">Active-Active (Двунаправленная)</div>
                <div class="text-3xs text-slate-500 mt-0.5">Одинаковые имена топиков с защитой от циклов</div>
              </label>
              <label class="p-3 rounded-xl border cursor-pointer {newDirection === 'UNIDIRECTIONAL' ? 'border-emerald-500 bg-emerald-50/40 dark:bg-emerald-950/30' : 'border-slate-200 dark:border-slate-700'}">
                <input type="radio" bind:group={newDirection} value="UNIDIRECTIONAL" class="sr-only" />
                <div class="font-bold text-xs text-slate-900 dark:text-white">Однонаправленная (DC1 ➔ DC2)</div>
                <div class="text-3xs text-slate-500 mt-0.5">Прямой поток данных для Disaster Recovery</div>
              </label>
            </div>
          </div>

          <!-- ВЫБОР ТОПИКОВ С НАСТРОЙКОЙ РЕЖИМА ПЕРЕДАЧИ -->
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1.5">
              Выберите топики и режим сериализации:
            </label>
            <div class="space-y-2 border border-slate-200 dark:border-slate-800 rounded-xl p-3 max-h-48 overflow-y-auto bg-slate-50/50 dark:bg-slate-900/50">
              {#each availableTopics as t}
                {@const isSelected = selectedTopics.some(item => item.topic === t.name)}
                {@const currentMode = selectedTopics.find(item => item.topic === t.name)?.payloadMode || 'AUTO_DETECT'}
                <div class="flex items-center justify-between p-2 rounded-lg bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700">
                  <label class="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onchange={() => toggleTopic(t.name, t.detectedFormat)}
                      class="rounded text-emerald-600 focus:ring-emerald-500"
                    />
                    <span class="font-mono text-xs font-medium text-slate-900 dark:text-white">{t.name}</span>
                    <span class="text-3xs text-slate-400">({t.partitions} парт.)</span>
                  </label>

                  {#if isSelected}
                    <select
                      value={currentMode}
                      onchange={(e) => changeTopicMode(t.name, e.currentTarget.value as any)}
                      class="text-xs px-2 py-1 rounded border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-700 text-slate-900 dark:text-white"
                    >
                      <option value="AUTO_DETECT">Auto-detect</option>
                      <option value="PASSTHROUGH">Passthrough (Plain/JSON/Raw)</option>
                      <option value="SCHEMA_REGISTRY">Schema Registry (Avro)</option>
                    </select>
                  {/if}
                </div>
              {/each}
            </div>
          </div>

          <!-- СЕКЦИЯ SCHEMA REGISTRY -->
          <div class="p-3.5 rounded-xl border border-purple-200 dark:border-purple-900/60 bg-purple-50/30 dark:bg-purple-950/20 space-y-3">
            <label class="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" bind:checked={enableSchemaTranscoding} class="rounded text-purple-600" />
              <span class="text-xs font-bold text-slate-900 dark:text-white">Включить кросс-регистрацию и транскодинг Schema Registry</span>
            </label>

            {#if enableSchemaTranscoding}
              <div class="grid grid-cols-2 gap-3 pt-2">
                <div>
                  <label class="block text-3xs font-semibold text-slate-600 dark:text-slate-400 mb-1">Реестр источника</label>
                  <select bind:value={sourceSrType} class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800">
                    <option value="HORTONWORKS">Hortonworks / Cloudera SR</option>
                    <option value="CONFLUENT">Confluent Schema Registry</option>
                  </select>
                  <input type="text" bind:value={sourceSrUrl} placeholder="http://hwx-sr:7788" class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800" />
                </div>
                <div>
                  <label class="block text-3xs font-semibold text-slate-600 dark:text-slate-400 mb-1">Реестр приемника</label>
                  <select bind:value={targetSrType} class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800">
                    <option value="CONFLUENT">Confluent Schema Registry</option>
                    <option value="HORTONWORKS">Hortonworks / Cloudera SR</option>
                  </select>
                  <input type="text" bind:value={targetSrUrl} placeholder="http://confluent-sr:8081" class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800" />
                </div>
              </div>
              <div class="pt-1">
                <label class="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" bind:checked={enableSrKerberos} class="rounded text-purple-600 focus:ring-purple-500" />
                  <span class="text-xs font-medium text-slate-700 dark:text-slate-300">
                    Керберизованный Schema Registry (SPNEGO / HTTP Negotiate)
                  </span>
                </label>
              </div>
            {/if}
          </div>

          <!-- ВЫБОР ГРУПП -->
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Синхронизировать Consumer Groups (оффсеты):
            </label>
            <div class="space-y-1.5 border border-slate-200 dark:border-slate-800 rounded-xl p-3 bg-slate-50/50 dark:bg-slate-900/50">
              {#each availableGroups as g}
                <label class="flex items-center justify-between p-1.5 rounded hover:bg-white dark:hover:bg-slate-800 cursor-pointer">
                  <div class="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={selectedGroups.includes(g.groupId)}
                      onchange={() => toggleGroup(g.groupId)}
                      class="rounded text-emerald-600 focus:ring-emerald-500"
                    />
                    <span class="font-mono text-xs text-slate-900 dark:text-white">{g.groupId}</span>
                  </div>
                  <span class="text-3xs text-slate-400">Статус: {g.state} (Lag: {g.lag})</span>
                </label>
              {/each}
            </div>
          </div>

          <!-- СЕКЦИЯ KERBEROS АУТЕНТИФИКАЦИИ -->
          <div class="p-3.5 rounded-xl border border-amber-200 dark:border-amber-900/60 bg-amber-50/30 dark:bg-amber-950/20 space-y-3">
            <label class="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" bind:checked={enableKerberos} class="rounded text-amber-600 focus:ring-amber-500" />
              <span class="text-xs font-bold text-slate-900 dark:text-white flex items-center gap-1.5">
                <svg class="w-3.5 h-3.5 text-amber-600 dark:text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                </svg>
                Керберизованная Kafka (Kerberos / SASL GSSAPI)
              </span>
            </label>

            {#if enableKerberos}
              <div class="grid grid-cols-2 gap-3 pt-1">
                <div>
                  <label class="block text-3xs font-semibold text-slate-600 dark:text-slate-400 mb-1">Source Principal & Keytab</label>
                  <input
                    type="text"
                    bind:value={sourcePrincipal}
                    placeholder="kafka-replicator@DC1.CORP"
                    class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800"
                  />
                  <input
                    type="text"
                    bind:value={sourceKeytabPath}
                    placeholder="/etc/security/keytabs/kafka.keytab"
                    class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800"
                  />
                </div>
                <div>
                  <label class="block text-3xs font-semibold text-slate-600 dark:text-slate-400 mb-1">Target Principal & Keytab</label>
                  <input
                    type="text"
                    bind:value={targetPrincipal}
                    placeholder="kafka-replicator@DC2.CORP"
                    class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800"
                  />
                  <input
                    type="text"
                    bind:value={targetKeytabPath}
                    placeholder="/etc/security/keytabs/kafka.keytab"
                    class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800"
                  />
                </div>
              </div>
            {/if}
          </div>

          <!-- ЛИМИТ СКОРОСТИ И ИНТЕРВАЛ -->
          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Лимит WAN (МБ/с)</label>
              <input
                type="number"
                bind:value={bandwidthLimitMb}
                min="1"
                max="1000"
                class="w-full px-3 py-2 rounded-xl border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 text-sm"
              />
            </div>
            <div>
              <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">Интервал синка оффсетов (сек)</label>
              <input
                type="number"
                bind:value={offsetSyncIntervalSec}
                min="1"
                max="60"
                class="w-full px-3 py-2 rounded-xl border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-800 text-sm"
              />
            </div>
          </div>
        </div>

        <div class="px-6 py-4 border-t border-slate-200 dark:border-slate-800 flex justify-end gap-3 bg-slate-50 dark:bg-slate-900/80">
          <button
            type="button"
            onclick={() => (isCreateModalOpen = false)}
            class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-300 text-sm font-medium hover:bg-slate-100 dark:hover:bg-slate-800 cursor-pointer"
          >
            Отмена
          </button>
          <button
            type="button"
            onclick={handleCreateJob}
            class="px-4 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium transition cursor-pointer"
          >
            Запустить репликацию
          </button>
        </div>
      </div>
    </div>
  {/if}
</div>
