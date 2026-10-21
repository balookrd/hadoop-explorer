<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '../api/client';
  import {
    ArrowLeftRight,
    RefreshCw,
    Plus,
    X,
    Server,
    Shield,
    Check,
    AlertCircle,
    Play,
    Pause,
    Trash2,
    Search,
    Filter,
    Gauge,
    Zap,
    Layers,
    Radio,
    Clock,
  } from 'lucide-svelte';

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
    consumerThreads?: number;
    batchSize?: number;
    targetEndpoints?: string[];
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

  interface DiscoveryCluster {
    id: string;
    name: string;
    dcId?: string;
    location?: string;
    bootstrapServers?: string;
    srUrl?: string;
    srType?: string;
    kerberosEnabled?: boolean;
    principal?: string;
    keytabPath?: string;
  }

  interface DiscoveredTopic {
    name: string;
    partitions: number;
    detectedFormat: string;
    hasSchema: boolean;
  }

  interface DiscoveredGroup {
    groupId: string;
    state: string;
    members: number;
    lag: number;
  }

  // Состояние задач
  let jobs = $state<KafkaJob[]>([]);
  let jobsLoading = $state(false);
  let actionMessage = $state<string | null>(null);

  // Справочники кластеров
  let availableClusters = $state<DiscoveryCluster[]>([]);
  let selectedSourceClusterId = $state<string>('');
  let selectedTargetClusterId = $state<string>('');

  // QoS, HTB и автоматическое масштабирование по агентам
  let isAutoBandwidth = $state(true);
  let isMultiAgentScaling = $state(true);
  let isCustomQosOpen = $state(false);
  let isManualEndpointsOpen = $state(false);
  let liveReceivers = $state<string[]>([]);
  let liveSenders = $state<string[]>([]);
  let liveSenderDetails = $state<any[]>([]);
  let detectedDcLimitMb = $state(100);
  let isLoadingAgents = $state(false);

  // Справочники обнаруженных топиков и групп
  let availableTopics = $state<DiscoveredTopic[]>([]);
  let availableGroups = $state<DiscoveredGroup[]>([]);
  let isLoadingDiscovery = $state(false);

  // Модальное окно
  let isCreateModalOpen = $state(false);

  // Основные поля задачи
  let newJobName = $state('Межкластерная репликация Kafka');
  let newSourceDc = $state('');
  let newTargetDc = $state('');
  let newDirection = $state<'BIDIRECTIONAL' | 'UNIDIRECTIONAL'>('BIDIRECTIONAL');

  // Режимы выбора топиков: 'REGEX' (для сотен и тысяч) или 'CATALOG' (ручной выбор)
  let topicsSelectionMode = $state<'REGEX' | 'CATALOG'>('REGEX');
  let topicRegexPattern = $state('orders-.*|payments-.*|billing-.*');
  let topicExcludePattern = $state('__.*|.*-dlq');
  let defaultTopicPayloadMode = $state<'AUTO_DETECT' | 'PASSTHROUGH' | 'SCHEMA_REGISTRY'>('AUTO_DETECT');
  let isTopicMatchPreviewOpen = $state(false);

  // Каталог топиков
  let topicSearchQuery = $state('');
  let topicFilterFormat = $state<'ALL' | 'SCHEMA_REGISTRY' | 'PASSTHROUGH'>('ALL');
  let topicFilterSelectedOnly = $state(false);
  let selectedTopics = $state<TopicSetting[]>([]);

  // Режимы выбора Consumer Groups: 'REGEX' (для сотен и тысяч) или 'CATALOG' (ручной)
  let groupsSelectionMode = $state<'REGEX' | 'CATALOG'>('REGEX');
  let groupRegexPattern = $state('billing-.*|fraud-.*|analytics-.*');
  let groupExcludePattern = $state('.*-archiver.*|dev-.*');
  let isGroupMatchPreviewOpen = $state(false);

  // Каталог групп
  let groupSearchQuery = $state('');
  let groupFilterState = $state<'ALL' | 'STABLE' | 'EMPTY'>('ALL');
  let groupFilterSelectedOnly = $state(false);
  let selectedGroups = $state<string[]>([]);

  // Schema Registry
  let enableSchemaTranscoding = $state(true);
  let sourceSrType = $state('HORTONWORKS');
  let targetSrType = $state('CONFLUENT');
  let sourceSrUrl = $state('http://schema-registry-1:8081');
  let targetSrUrl = $state('http://schema-registry-2:8082');
  let enableSrKerberos = $state(true);

  // Kerberos Kafka
  let enableKerberos = $state(true);
  let sourcePrincipal = $state('kafka-replicator@COMPANY.LOCAL');
  let sourceKeytabPath = $state('/etc/security/keytabs/kafka-replicator.keytab');
  let targetPrincipal = $state('kafka-replicator@COMPANY.LOCAL');
  let targetKeytabPath = $state('/etc/security/keytabs/kafka-replicator.keytab');

  // QoS и параллелизм
  let bandwidthLimitMb = $state(50);
  let offsetSyncIntervalSec = $state(5);
  let consumerThreads = $state(8);
  let batchSize = $state(2000);
  let targetEndpointsInput = $state('');

  // Проверка Regex
  function matchesPattern(name: string, includePattern: string, excludePattern: string): boolean {
    if (!includePattern || includePattern.trim() === '' || includePattern.trim() === '*') {
      if (excludePattern && excludePattern.trim() !== '') {
        try {
          const exRe = new RegExp(excludePattern.trim());
          return !exRe.test(name);
        } catch {
          return true;
        }
      }
      return true;
    }
    try {
      const inRe = new RegExp(includePattern.trim());
      if (!inRe.test(name)) return false;
      if (excludePattern && excludePattern.trim() !== '') {
        const exRe = new RegExp(excludePattern.trim());
        if (exRe.test(name)) return false;
      }
      return true;
    } catch {
      return name.includes(includePattern.trim());
    }
  }

  let regexMatchedTopics = $derived(
    availableTopics.filter(t => matchesPattern(t.name, topicRegexPattern, topicExcludePattern))
  );

  let filteredCatalogTopics = $derived(
    availableTopics.filter(t => {
      if (topicSearchQuery && !t.name.toLowerCase().includes(topicSearchQuery.toLowerCase())) {
        return false;
      }
      if (topicFilterFormat === 'SCHEMA_REGISTRY' && !t.hasSchema) {
        return false;
      }
      if (topicFilterFormat === 'PASSTHROUGH' && t.hasSchema) {
        return false;
      }
      if (topicFilterSelectedOnly && !selectedTopics.some(item => item.topic === t.name)) {
        return false;
      }
      return true;
    })
  );

  let regexMatchedGroups = $derived(
    availableGroups.filter(g => matchesPattern(g.groupId, groupRegexPattern, groupExcludePattern))
  );

  let filteredCatalogGroups = $derived(
    availableGroups.filter(g => {
      if (groupSearchQuery && !g.groupId.toLowerCase().includes(groupSearchQuery.toLowerCase())) {
        return false;
      }
      if (groupFilterState !== 'ALL' && g.state !== groupFilterState) {
        return false;
      }
      if (groupFilterSelectedOnly && !selectedGroups.includes(g.groupId)) {
        return false;
      }
      return true;
    })
  );

  let effectiveTopicsCount = $derived(
    topicsSelectionMode === 'REGEX' ? regexMatchedTopics.length : selectedTopics.length
  );

  let effectiveGroupsCount = $derived(
    groupsSelectionMode === 'REGEX' ? regexMatchedGroups.length : selectedGroups.length
  );

  // Статистика
  let totalMessages = $derived(jobs.reduce((acc, j) => acc + (j.messagesReplicated || 0), 0));
  let totalBytesMb = $derived((jobs.reduce((acc, j) => acc + (j.bytesReplicated || 0), 0) / (1024 * 1024)).toFixed(1));

  // Согласованное QoS регулирование: межцодовая WAN HTB квота vs сумма явно заданных лимитов агентов источника
  let wanHtbBandwidthMb = $derived(Math.round(detectedDcLimitMb * 0.40));
  let totalSendersCapacityMb = $derived(
    liveSenderDetails.length > 0
      ? liveSenderDetails.reduce((acc, a) => acc + (a.max_bandwidth_mb_s && a.max_bandwidth_mb_s > 0 ? a.max_bandwidth_mb_s : 100), 0)
      : null
  );
  let effectiveBandwidthMb = $derived(
    totalSendersCapacityMb !== null ? Math.min(wanHtbBandwidthMb, totalSendersCapacityMb) : wanHtbBandwidthMb
  );
  let autoBandwidthMb = $derived(effectiveBandwidthMb);
  let isAgentBottleneck = $derived(
    totalSendersCapacityMb !== null && totalSendersCapacityMb < wanHtbBandwidthMb
  );

  let liveSendersCount = $derived(liveSenders.length > 0 ? liveSenders.length : 1);

  // Расчет потоков вычитки на каждый агент-отправитель (шардинг пула без искусственного потолка в 40G: масштабирование на 100G, 200G, 400G+)
  let targetTotalThreads = $derived.by(() => {
    const bw = autoBandwidthMb;
    if (bw <= 50) return 8;
    if (bw <= 100) return 16;
    if (bw <= 300) return 24;
    if (bw <= 800) return 32;
    if (bw <= 2000) return 64;
    return Math.max(64, Math.round(bw / 30.0));
  });

  let autoConsumerThreadsPerAgent = $derived(
    Math.min(32, Math.max(1, Math.ceil(targetTotalThreads / liveSendersCount)))
  );
  let autoTotalThreads = $derived(autoConsumerThreadsPerAgent * liveSendersCount);

  let autoBatchSize = $derived.by(() => {
    const bw = autoBandwidthMb;
    if (bw <= 20) return 1000;
    if (bw <= 80) return 2000;
    if (bw <= 200) return 5000;
    if (bw <= 800) return 10000;
    if (bw <= 1500) return 15000;
    if (bw <= 3000) return 20000;
    if (bw <= 6000) return 35000;
    return 50000;
  });

  let autoOffsetSyncInterval = $derived.by(() => {
    const bw = autoBandwidthMb;
    if (bw >= 1500) return 1;
    if (bw >= 500) return 2;
    if (bw >= 100) return 3;
    if (bw <= 20) return 10;
    return 5;
  });

  onMount(() => {
    loadJobs();
    loadClusters();
    loadTopologyLimits();
    const interval = setInterval(() => {
      // Фоновое обновление телеметрии задач без мерцания индикатора
      api.getKafkaJobs().then(data => {
        if (Array.isArray(data)) {
          jobs = data;
        }
      }).catch(() => {});
    }, 4000);
    return () => clearInterval(interval);
  });

  async function loadTopologyLimits() {
    try {
      const topo = await api.getTopology();
      if (topo && Array.isArray(topo.dc_limits)) {
        updateBandwidthFromTopology(newSourceDc, newTargetDc, topo.dc_limits);
      }
    } catch (e) {
      console.warn('Не удалось загрузить лимиты топологии:', e);
    }
  }

  function updateBandwidthFromTopology(src: string, dst: string, limits?: any[]) {
    if (!src || !dst) return;
    const s = src.toLowerCase().replace(/[^a-z0-9]/g, '');
    const d = dst.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (limits && limits.length > 0) {
      const found = limits.find((l: any) => {
        const ls = (l.source_dc || '').toLowerCase().replace(/[^a-z0-9]/g, '');
        const ld = (l.target_dc || '').toLowerCase().replace(/[^a-z0-9]/g, '');
        return (ls === s && ld === d) || (ls === d && ld === s);
      });
      if (found && found.limit_mb_per_sec) {
        detectedDcLimitMb = found.limit_mb_per_sec;
        if (isAutoBandwidth) {
          bandwidthLimitMb = Math.round(found.limit_mb_per_sec * 0.40);
        }
        return;
      }
    }
    detectedDcLimitMb = 100;
    if (isAutoBandwidth) {
      bandwidthLimitMb = 40;
    }
  }

  async function loadLiveAgents(srcDc: string, dstDc: string) {
    isLoadingAgents = true;
    try {
      const [senders, senderDetails, receivers] = await Promise.all([
        api.getKafkaSenderEndpoints(srcDc).catch(() => []),
        api.getKafkaSenderDetails(srcDc).catch(() => []),
        api.getKafkaReceiverEndpoints(dstDc).catch(() => [])
      ]);
      liveSenders = Array.isArray(senders) ? senders : [];
      liveSenderDetails = Array.isArray(senderDetails) ? senderDetails : [];
      liveReceivers = Array.isArray(receivers) ? receivers : [];
      if (isMultiAgentScaling && (!targetEndpointsInput || targetEndpointsInput.trim() === '')) {
        targetEndpointsInput = liveReceivers.join(', ');
      }
    } catch (e) {
      console.warn('Не удалось загрузить активных агентов:', e);
    } finally {
      isLoadingAgents = false;
    }
  }

  async function loadJobs() {
    jobsLoading = true;
    try {
      const data = await api.getKafkaJobs();
      jobs = Array.isArray(data) ? data : [];
    } catch (e: any) {
      console.warn('Не удалось загрузить задачи Kafka из API:', e);
      jobs = [];
    } finally {
      jobsLoading = false;
    }
  }

  async function loadClusters() {
    try {
      const clusters = await api.getKafkaDiscoveryClusters();
      if (Array.isArray(clusters) && clusters.length > 0) {
        availableClusters = clusters;
        if (!selectedSourceClusterId && clusters[0]) {
          selectedSourceClusterId = clusters[0].id;
          applyClusterToSource(clusters[0]);
        }
        if (!selectedTargetClusterId && clusters.length > 1) {
          selectedTargetClusterId = clusters[1].id;
          applyClusterToTarget(clusters[1]);
        }
        if (clusters[0]) {
          loadDiscovery(clusters[0].dcId || clusters[0].id || '');
        }
        loadLiveAgents(newSourceDc, newTargetDc);
        loadTopologyLimits();
      }
    } catch (e) {
      console.warn('Не удалось загрузить кластеры:', e);
    }
  }

  async function loadDiscovery(dc: string) {
    isLoadingDiscovery = true;
    try {
      const [topics, groups] = await Promise.all([
        api.getKafkaDiscoveryTopics(dc),
        api.getKafkaDiscoveryGroups(dc)
      ]);
      if (Array.isArray(topics) && topics.length > 0) {
        availableTopics = topics;
      }
      if (Array.isArray(groups) && groups.length > 0) {
        availableGroups = groups;
      }
    } catch (e) {
      console.warn('Не удалось загрузить discovery топиков/групп:', e);
    } finally {
      isLoadingDiscovery = false;
    }
  }

  function applyClusterToSource(cluster: DiscoveryCluster) {
    newSourceDc = cluster.name || cluster.id;
    if (cluster.srUrl) sourceSrUrl = cluster.srUrl;
    if (cluster.srType) sourceSrType = cluster.srType;
    if (cluster.principal) sourcePrincipal = cluster.principal;
    if (cluster.keytabPath) sourceKeytabPath = cluster.keytabPath;
    if (cluster.kerberosEnabled !== undefined) enableKerberos = cluster.kerberosEnabled;
  }

  function applyClusterToTarget(cluster: DiscoveryCluster) {
    newTargetDc = cluster.name || cluster.id;
    if (cluster.srUrl) targetSrUrl = cluster.srUrl;
    if (cluster.srType) targetSrType = cluster.srType;
    if (cluster.principal) targetPrincipal = cluster.principal;
    if (cluster.keytabPath) targetKeytabPath = cluster.keytabPath;
  }

  function handleSourceClusterSelect(clusterId: string) {
    selectedSourceClusterId = clusterId;
    const found = availableClusters.find(c => c.id === clusterId);
    if (found) {
      applyClusterToSource(found);
      loadDiscovery(found.dcId || found.id || '');
      loadLiveAgents(newSourceDc, newTargetDc);
      loadTopologyLimits();
    }
  }

  function handleTargetClusterSelect(clusterId: string) {
    selectedTargetClusterId = clusterId;
    const found = availableClusters.find(c => c.id === clusterId);
    if (found) {
      applyClusterToTarget(found);
      loadLiveAgents(newSourceDc, newTargetDc);
      loadTopologyLimits();
    }
  }

  function swapClusters() {
    const tempId = selectedSourceClusterId;
    selectedSourceClusterId = selectedTargetClusterId;
    selectedTargetClusterId = tempId;

    const tempName = newSourceDc;
    newSourceDc = newTargetDc;
    newTargetDc = tempName;

    const tempSrUrl = sourceSrUrl;
    sourceSrUrl = targetSrUrl;
    targetSrUrl = tempSrUrl;

    const tempSrType = sourceSrType;
    sourceSrType = targetSrType;
    targetSrType = tempSrType;

    const tempPrincipal = sourcePrincipal;
    sourcePrincipal = targetPrincipal;
    targetPrincipal = tempPrincipal;

    const tempKeytab = sourceKeytabPath;
    sourceKeytabPath = targetKeytabPath;
    targetKeytabPath = tempKeytab;

    const src = availableClusters.find(c => c.id === selectedSourceClusterId);
    loadDiscovery(src?.dcId || src?.id || '');
    loadLiveAgents(newSourceDc, newTargetDc);
    loadTopologyLimits();
  }

  function toggleTopic(topicName: string, defaultFormat: string) {
    const idx = selectedTopics.findIndex(t => t.topic === topicName);
    if (idx >= 0) {
      selectedTopics = selectedTopics.filter(t => t.topic !== topicName);
    } else {
      selectedTopics = [...selectedTopics, { topic: topicName, payloadMode: (defaultFormat as any) || 'AUTO_DETECT' }];
    }
  }

  function changeTopicMode(topicName: string, mode: 'PASSTHROUGH' | 'AUTO_DETECT' | 'SCHEMA_REGISTRY') {
    selectedTopics = selectedTopics.map(t => (t.topic === topicName ? { ...t, payloadMode: mode } : t));
  }

  function selectAllFilteredTopics() {
    const toAdd: TopicSetting[] = filteredCatalogTopics
      .filter(ft => !selectedTopics.some(st => st.topic === ft.name))
      .map(ft => ({ topic: ft.name, payloadMode: (ft.detectedFormat as any) || 'AUTO_DETECT' }));
    selectedTopics = [...selectedTopics, ...toAdd];
  }

  function clearAllTopics() {
    selectedTopics = [];
  }

  function applyModeToAllSelectedTopics(mode: 'PASSTHROUGH' | 'AUTO_DETECT' | 'SCHEMA_REGISTRY') {
    selectedTopics = selectedTopics.map(t => ({ ...t, payloadMode: mode }));
  }

  function removeTopic(topicName: string) {
    selectedTopics = selectedTopics.filter(t => t.topic !== topicName);
  }

  function toggleGroup(groupId: string) {
    if (selectedGroups.includes(groupId)) {
      selectedGroups = selectedGroups.filter(g => g !== groupId);
    } else {
      selectedGroups = [...selectedGroups, groupId];
    }
  }

  function selectAllFilteredGroups() {
    const toAdd = filteredCatalogGroups.map(g => g.groupId).filter(gid => !selectedGroups.includes(gid));
    selectedGroups = [...selectedGroups, ...toAdd];
  }

  function clearAllGroups() {
    selectedGroups = [];
  }

  function removeGroup(groupId: string) {
    selectedGroups = selectedGroups.filter(g => g !== groupId);
  }

  async function handleCreateJob() {
    let resolvedTopics: TopicSetting[] = [];
    if (topicsSelectionMode === 'REGEX') {
      resolvedTopics = regexMatchedTopics.map(t => ({
        topic: t.name,
        payloadMode: defaultTopicPayloadMode === 'AUTO_DETECT' ? ((t.detectedFormat as any) || 'AUTO_DETECT') : defaultTopicPayloadMode
      }));
      if (resolvedTopics.length === 0 && topicRegexPattern) {
        resolvedTopics = [{ topic: topicRegexPattern, payloadMode: defaultTopicPayloadMode }];
      }
    } else {
      resolvedTopics = [...selectedTopics];
    }

    let resolvedGroups: string[] = [];
    if (groupsSelectionMode === 'REGEX') {
      resolvedGroups = regexMatchedGroups.map(g => g.groupId);
      if (resolvedGroups.length === 0 && groupRegexPattern) {
        resolvedGroups = [groupRegexPattern];
      }
    } else {
      resolvedGroups = [...selectedGroups];
    }

    const payload = {
      name: newJobName,
      sourceDc: newSourceDc,
      targetDc: newTargetDc,
      direction: newDirection,
      topics: resolvedTopics,
      consumerGroups: resolvedGroups,
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
      bandwidthLimitMb: isAutoBandwidth ? autoBandwidthMb : bandwidthLimitMb,
      offsetSyncIntervalSec: isAutoBandwidth ? autoOffsetSyncInterval : offsetSyncIntervalSec,
      consumerThreads: isAutoBandwidth ? autoConsumerThreadsPerAgent : consumerThreads,
      batchSize: isAutoBandwidth ? autoBatchSize : batchSize,
      targetEndpoints: targetEndpointsInput.trim()
        ? targetEndpointsInput.split(',').map(s => s.trim()).filter(Boolean)
        : (isMultiAgentScaling && liveReceivers.length > 0 ? liveReceivers : undefined)
    };

    try {
      const created = await api.createKafkaJob(payload);
      if (created) {
        jobs = [created, ...jobs.filter(j => j.id !== created.id)];
      } else {
        await loadJobs();
      }
      isCreateModalOpen = false;
      showNotification(`Задача "${newJobName}" успешно запущена!`);
    } catch (e: any) {
      alert(`Ошибка при создании задачи: ${e.message || e}`);
    }
  }

  async function toggleJobStatus(job: KafkaJob) {
    try {
      if (job.status === 'ACTIVE') {
        await api.pauseKafkaJob(job.id);
        job.status = 'PAUSED';
      } else {
        await api.resumeKafkaJob(job.id);
        job.status = 'ACTIVE';
      }
      jobs = [...jobs];
      showNotification(`Статус задачи "${job.name}" изменен на ${job.status}`);
    } catch (e: any) {
      alert(`Ошибка изменения статуса: ${e.message || e}`);
    }
  }

  async function triggerFailover(job: KafkaJob) {
    if (confirm(`Инициировать принудительный Failover смещений для задачи "${job.name}"?\nВсе потребители в ${job.targetDc} продолжат чтение с актуальных реплицированных оффсетов.`)) {
      try {
        await api.triggerKafkaFailover(job.id);
        showNotification(`Смещения консьюмер-групп успешно форсированы в ${job.targetDc}. Трафик переключен.`);
      } catch (e: any) {
        alert(`Ошибка Failover: ${e.message || e}`);
      }
    }
  }

  async function deleteJob(id: string) {
    if (confirm('Вы уверены, что хотите удалить эту задачу репликации Kafka?')) {
      try {
        await api.deleteKafkaJob(id);
        jobs = jobs.filter(j => j.id !== id);
        showNotification('Задача репликации удалена');
      } catch (e: any) {
        alert(`Ошибка удаления: ${e.message || e}`);
      }
    }
  }

  function showNotification(msg: string) {
    actionMessage = msg;
    setTimeout(() => {
      if (actionMessage === msg) actionMessage = null;
    }, 4500);
  }
</script>

<div class="space-y-6">
  <!-- ВСПЛЫВАЮЩЕЕ УВЕДОМЛЕНИЕ В ЕДИНОМ СТИЛЕ -->
  {#if actionMessage}
    <div class="fixed top-4 right-4 z-50 flex items-center gap-2.5 px-4 py-3 rounded-xl bg-slate-900 dark:bg-slate-100 text-white dark:text-slate-900 shadow-xl border border-slate-700 dark:border-slate-300 text-xs font-semibold">
      <Check class="w-4 h-4 text-emerald-400 dark:text-emerald-600 shrink-0" />
      <span>{actionMessage}</span>
    </div>
  {/if}

  <!-- ВЕРХНЯЯ ПАНЕЛЬ СТАТИСТИКИ (СТИЛЬ HDFS REPLICATION) -->
  <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
    <!-- KPI Карточки -->
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 flex-1">
      <div class="p-3 rounded-xl border bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 shadow-2xs">
        <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Всего задач</span>
        <span class="text-xl font-bold text-slate-900 dark:text-slate-100">{jobs.length}</span>
      </div>

      <div class="p-3 rounded-xl border bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 shadow-2xs">
        <span class="text-[11px] font-medium text-sky-600 dark:text-sky-400 block">В работе</span>
        <span class="text-xl font-bold text-sky-600 dark:text-sky-400">{jobs.filter(j => j.status === 'ACTIVE').length}</span>
      </div>

      <div class="p-3 rounded-xl border bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 shadow-2xs">
        <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Передано сообщений</span>
        <span class="text-xl font-bold text-slate-900 dark:text-slate-100">{totalMessages.toLocaleString()}</span>
      </div>

      <div class="p-3 rounded-xl border bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 shadow-2xs">
        <div class="flex items-center justify-between">
          <span class="text-[11px] font-medium text-slate-500 dark:text-slate-400 block">Объем WAN / RTT</span>
          <span class="text-[10px] font-mono text-emerald-600 font-bold">&lt; 10 мс</span>
        </div>
        <span class="text-xl font-bold text-slate-900 dark:text-slate-100">{totalBytesMb} <span class="text-xs font-normal text-slate-400">МБ</span></span>
      </div>
    </div>

    <!-- Кнопки действий -->
    <div class="flex items-center gap-2 self-end sm:self-auto shrink-0">
      <button
        type="button"
        onclick={loadJobs}
        class="p-2.5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-600 dark:text-slate-300 transition cursor-pointer shadow-2xs"
        title="Обновить список"
      >
        <RefreshCw class="w-4 h-4 {jobsLoading ? 'animate-spin' : ''}" />
      </button>

      <button
        type="button"
        onclick={() => (isCreateModalOpen = true)}
        class="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-sky-600 hover:bg-sky-500 text-white font-semibold text-xs transition cursor-pointer shadow-md shadow-sky-600/20"
      >
        <Plus class="w-4 h-4" />
        <span>Новая репликация Kafka</span>
      </button>
    </div>
  </div>

  <!-- СПИСОК ЗАДАЧ РЕПЛИКАЦИИ KAFKA -->
  <div class="space-y-4">
    {#if jobsLoading}
      <div class="py-12 text-center text-xs text-slate-400 dark:text-slate-500">
        Загрузка задач репликации Kafka...
      </div>
    {:else if jobs.length === 0}
      <div class="py-12 text-center text-xs text-slate-400 dark:text-slate-500 border border-dashed border-slate-200 dark:border-slate-800 rounded-2xl bg-white dark:bg-slate-900">
        <Radio class="w-8 h-8 mx-auto mb-2 text-slate-300 dark:text-slate-600" />
        <p class="font-medium text-slate-600 dark:text-slate-400">Нет настроенных задач репликации Kafka.</p>
        <button
          type="button"
          onclick={() => (isCreateModalOpen = true)}
          class="mt-3 px-3.5 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-xs cursor-pointer inline-flex items-center gap-1.5"
        >
          <Plus class="w-3.5 h-3.5" />
          <span>Создать задачу</span>
        </button>
      </div>
    {:else}
      {#each jobs as job (job.id)}
        <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-2xs space-y-4">
          <!-- Верхняя строка карточки: название, бейджи и действия -->
          <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div class="space-y-1.5">
              <div class="flex items-center flex-wrap gap-2">
                <span class="w-2 h-2 rounded-full {job.status === 'ACTIVE' ? 'bg-emerald-500 animate-pulse' : 'bg-amber-500'}"></span>
                <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100">{job.name}</h3>

                <!-- Бейдж направления -->
                <span class="px-2 py-0.5 rounded font-mono font-bold text-[10px] {job.direction === 'BIDIRECTIONAL' ? 'bg-sky-50 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300 border border-sky-200 dark:border-sky-800' : 'bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300'}">
                  {job.direction === 'BIDIRECTIONAL' ? '⇄ Active-Active' : '➔ Однонаправленная'}
                </span>

                <!-- Бейдж Schema Registry -->
                {#if job.schemaTranscodingEnabled}
                  <span class="px-2 py-0.5 rounded text-[10px] font-semibold bg-indigo-50 dark:bg-indigo-950/60 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800 flex items-center gap-1">
                    <Layers class="w-3 h-3 text-indigo-500" />
                    <span>SR: {job.sourceSrType} ➔ {job.targetSrType}</span>
                  </span>
                {/if}

                <!-- Бейдж Kerberos -->
                {#if job.sourceKerberosEnabled}
                  <span class="px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700 flex items-center gap-1">
                    <Shield class="w-3 h-3 text-slate-500" />
                    <span>Kerberos SASL</span>
                  </span>
                {/if}

                <!-- Бейдж Multi-Agent Load Balancing -->
                {#if job.targetEndpoints && job.targetEndpoints.length > 0}
                  <span class="px-2 py-0.5 rounded text-[10px] font-semibold bg-sky-50 dark:bg-sky-950/60 text-sky-700 dark:text-sky-300 border border-sky-200 dark:border-sky-800 flex items-center gap-1">
                    <Radio class="w-3 h-3 text-sky-500" />
                    <span>{job.targetEndpoints.length} целевых агентов (балансировка)</span>
                  </span>
                {:else}
                  <span class="px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 border border-slate-200 dark:border-slate-700 flex items-center gap-1">
                    <Radio class="w-3 h-3 text-slate-400" />
                    <span>Partition-Pinned Routing</span>
                  </span>
                {/if}
              </div>

              <!-- Метаданные канала -->
              <div class="text-xs text-slate-500 dark:text-slate-400 flex flex-wrap items-center gap-2">
                <span class="font-medium text-slate-700 dark:text-slate-300">{job.sourceDc}</span>
                <span class="text-slate-400">⇄</span>
                <span class="font-medium text-slate-700 dark:text-slate-300">{job.targetDc}</span>
                <span>•</span>
                <span>Полоса WAN: <strong class="text-slate-700 dark:text-slate-300 font-mono">{job.bandwidthLimitMb} МБ/с</strong></span>
                <span>•</span>
                <span>Потоков: <strong class="text-slate-700 dark:text-slate-300 font-mono">{job.consumerThreads ?? 8}</strong></span>
                <span>•</span>
                <span>Батч: <strong class="text-slate-700 dark:text-slate-300 font-mono">{job.batchSize ?? 2000}</strong></span>
                <span>•</span>
                <span>Сквозной лаг: <strong class="text-emerald-600 dark:text-emerald-400 font-mono">{job.currentLagMs ?? 0} мс</strong></span>
                <span>•</span>
                <span>Передано: <strong class="text-sky-600 dark:text-sky-400 font-mono">{job.messagesReplicated?.toLocaleString() ?? 0} сообщ. ({((job.bytesReplicated ?? 0) / (1024 * 1024)).toFixed(2)} МБ)</strong></span>
                <span>•</span>
                <span>Синхронизация смещений: <strong class="font-mono">{job.offsetSyncIntervalSec}с</strong></span>
              </div>
            </div>

            <!-- Кнопки управления задачей -->
            <div class="flex items-center gap-2 shrink-0">
              <button
                type="button"
                onclick={() => toggleJobStatus(job)}
                class="px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 hover:bg-slate-50 dark:hover:bg-slate-700/60 text-xs font-semibold text-slate-700 dark:text-slate-300 transition cursor-pointer shadow-2xs flex items-center gap-1.5"
              >
                {#if job.status === 'ACTIVE'}
                  <Pause class="w-3.5 h-3.5 text-amber-500" />
                  <span>Пауза</span>
                {:else}
                  <Play class="w-3.5 h-3.5 text-emerald-500" />
                  <span>Возобновить</span>
                {/if}
              </button>

              <button
                type="button"
                onclick={() => triggerFailover(job)}
                class="px-3 py-1.5 rounded-lg border border-rose-200 dark:border-rose-900/60 bg-rose-50 dark:bg-rose-950/40 text-rose-600 dark:text-rose-400 hover:bg-rose-100 dark:hover:bg-rose-900/60 text-xs font-semibold transition cursor-pointer shadow-2xs flex items-center gap-1.5"
                title="Форсировать смещения консьюмеров в целевом ЦОД"
              >
                <Zap class="w-3.5 h-3.5" />
                <span>Failover</span>
              </button>

              <button
                type="button"
                onclick={() => deleteJob(job.id)}
                class="p-2 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-400 hover:text-rose-600 hover:bg-rose-50 dark:hover:bg-rose-950/40 transition cursor-pointer"
                title="Удалить задачу"
              >
                <Trash2 class="w-3.5 h-3.5" />
              </button>
            </div>
          </div>

          <!-- Детали: топики и группы (строгий нейтральный стиль) -->
          <div class="grid grid-cols-1 md:grid-cols-2 gap-3 pt-3 border-t border-slate-100 dark:border-slate-800">
            <!-- Топики -->
            <div class="bg-slate-50 dark:bg-slate-950/60 p-3 rounded-xl border border-slate-200/60 dark:border-slate-800 space-y-2">
              <div class="flex items-center justify-between text-xs font-semibold text-slate-700 dark:text-slate-300">
                <span class="flex items-center gap-1.5">
                  <Radio class="w-3.5 h-3.5 text-sky-500" />
                  <span>Реплицируемые топики ({job.topics.length}):</span>
                </span>
              </div>
              <div class="flex flex-wrap gap-1.5 max-h-24 overflow-y-auto pr-1">
                {#each job.topics as t}
                  <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 font-mono text-slate-800 dark:text-slate-200 shadow-2xs">
                    <span>{t.topic}</span>
                    <span class="text-[9px] font-bold px-1 rounded {t.payloadMode === 'PASSTHROUGH' ? 'bg-amber-100 dark:bg-amber-950/80 text-amber-700 dark:text-amber-400' : 'bg-sky-100 dark:bg-sky-950/80 text-sky-700 dark:text-sky-400'}">
                      {t.payloadMode}
                    </span>
                  </span>
                {/each}
              </div>
            </div>

            <!-- Группы -->
            <div class="bg-slate-50 dark:bg-slate-950/60 p-3 rounded-xl border border-slate-200/60 dark:border-slate-800 space-y-2">
              <div class="flex items-center justify-between text-xs font-semibold text-slate-700 dark:text-slate-300">
                <span class="flex items-center gap-1.5">
                  <Clock class="w-3.5 h-3.5 text-indigo-500" />
                  <span>Синхронизируемые Consumer Groups ({job.consumerGroups.length}):</span>
                </span>
              </div>
              <div class="flex flex-wrap gap-1.5 max-h-24 overflow-y-auto pr-1">
                {#each job.consumerGroups as group}
                  <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 font-mono text-slate-800 dark:text-slate-200 shadow-2xs">
                    <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
                    <span>{group}</span>
                  </span>
                {/each}
              </div>
            </div>
          </div>
        </div>
      {/each}
    {/if}
  </div>

  <!-- МОДАЛЬНОЕ ОКНО СОЗДАНИЯ ЗАДАЧИ (СТИЛЬ HDFS REPLICATION) -->
  {#if isCreateModalOpen}
    <div
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
      onclick={(e) => { if (e.target === e.currentTarget) isCreateModalOpen = false; }}
    >
      <div class="w-full max-w-3xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl shadow-2xl p-6 flex flex-col gap-5 select-auto text-slate-900 dark:text-slate-100 max-h-[92vh] overflow-y-auto">
        <!-- Шапка модала -->
        <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-4">
          <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-xl bg-sky-50 dark:bg-sky-950/60 border border-sky-200 dark:border-sky-800 flex items-center justify-center text-sky-600 dark:text-sky-400">
              <Radio class="w-5 h-5" />
            </div>
            <div>
              <h2 class="text-base font-bold text-slate-900 dark:text-slate-100">Новая репликация Kafka & Schema Registry</h2>
              <p class="text-xs text-slate-500 dark:text-slate-400">Межцодовая синхронизация топиков, смещений и транскодинг схем</p>
            </div>
          </div>
          <button
            type="button"
            onclick={() => (isCreateModalOpen = false)}
            class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 cursor-pointer"
          >
            <X class="w-4 h-4" />
          </button>
        </div>

        <!-- Тело формы -->
        <div class="space-y-4 text-xs">
          <!-- 1. Название задачи -->
          <div>
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1">
              Название задачи:
            </label>
            <input
              type="text"
              bind:value={newJobName}
              placeholder="Например: Межцодовая репликация Orders & Payments"
              class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg text-xs text-slate-800 dark:text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
          </div>

          <!-- 2. ВЫБОР КЛАСТЕРОВ (ЦОД) ВЫПАДАЮЩИМИ СПИСКАМИ -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-2.5">
            <div class="flex items-center justify-between">
              <span class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                <Server class="w-3.5 h-3.5 text-sky-500" />
                <span>Кластеры Kafka и дата-центры:</span>
              </span>
              <span class="text-[11px] text-slate-400 font-medium">Конфигурация TopologyRegistry</span>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-5 gap-2.5 items-center">
              <!-- Источник -->
              <div class="md:col-span-2">
                <label class="block text-[11px] font-medium text-slate-500 mb-1">Кластер Источник (Source):</label>
                <select
                  value={selectedSourceClusterId}
                  onchange={(e) => handleSourceClusterSelect(e.currentTarget.value)}
                  class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
                >
                  {#each availableClusters as c}
                    <option value={c.id}>
                      {c.name} {c.location ? `(${c.location})` : ''}
                    </option>
                  {/each}
                </select>
              </div>

              <!-- Swap кнопка ⇄ -->
              <div class="flex justify-center pt-4">
                <button
                  type="button"
                  onclick={swapClusters}
                  title="Поменять местами источник и приемник (⇄)"
                  class="p-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 hover:bg-slate-100 dark:hover:bg-slate-800 text-sky-600 dark:text-sky-400 font-bold transition shadow-2xs cursor-pointer flex items-center gap-1"
                >
                  <ArrowLeftRight class="w-4 h-4" />
                  <span class="text-[10px] hidden sm:inline">Swap</span>
                </button>
              </div>

              <!-- Приемник -->
              <div class="md:col-span-2">
                <label class="block text-[11px] font-medium text-slate-500 mb-1">Кластер Приемник (Target):</label>
                <select
                  value={selectedTargetClusterId}
                  onchange={(e) => handleTargetClusterSelect(e.currentTarget.value)}
                  class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg text-xs font-medium focus:outline-none focus:ring-1 focus:ring-sky-500"
                >
                  {#each availableClusters as c}
                    <option value={c.id}>
                      {c.name} {c.location ? `(${c.location})` : ''}
                    </option>
                  {/each}
                </select>
              </div>
            </div>
          </div>

          <!-- 3. Топология репликации -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-2">
            <label class="block text-xs font-semibold text-slate-700 dark:text-slate-300">Режим топологии:</label>
            <div class="grid grid-cols-2 gap-3">
              <label class="p-3 rounded-xl border cursor-pointer transition-all {newDirection === 'BIDIRECTIONAL' ? 'border-sky-500 bg-white dark:bg-slate-900 ring-2 ring-sky-500/20' : 'border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:border-slate-300'}">
                <input type="radio" bind:group={newDirection} value="BIDIRECTIONAL" class="sr-only" />
                <div class="font-bold text-slate-900 dark:text-white">Active-Active (Двунаправленная)</div>
                <div class="text-[10px] text-slate-400 mt-0.5">Одинаковые имена топиков с origin заголовком (защита от циклов)</div>
              </label>

              <label class="p-3 rounded-xl border cursor-pointer transition-all {newDirection === 'UNIDIRECTIONAL' ? 'border-sky-500 bg-white dark:bg-slate-900 ring-2 ring-sky-500/20' : 'border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:border-slate-300'}">
                <input type="radio" bind:group={newDirection} value="UNIDIRECTIONAL" class="sr-only" />
                <div class="font-bold text-slate-900 dark:text-white">Однонаправленная (DC1 ➔ DC2)</div>
                <div class="text-[10px] text-slate-400 mt-0.5">Прямой поток данных для Disaster Recovery стенда</div>
              </label>
            </div>
          </div>

          <!-- 4. МАСШТАБИРУЕМЫЙ ВЫБОР ТОПИКОВ (ДЛЯ ТЫСЯЧ СУЩНОСТЕЙ: REGEX / ПОИСК) -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-3">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-200/60 dark:border-slate-800 pb-2.5">
              <div>
                <span class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                  <Radio class="w-3.5 h-3.5 text-sky-500" />
                  <span>Выбор топиков для репликации ({effectiveTopicsCount} выбрано):</span>
                </span>
              </div>

              <!-- Табы выбора режима: Regex vs Каталог -->
              <div class="flex items-center gap-1 bg-slate-200/60 dark:bg-slate-800 p-0.5 rounded-lg text-[11px] font-semibold">
                <button
                  type="button"
                  onclick={() => (topicsSelectionMode = 'REGEX')}
                  class="px-2.5 py-1 rounded-md transition cursor-pointer {topicsSelectionMode === 'REGEX' ? 'bg-white dark:bg-slate-700 text-sky-600 dark:text-sky-400 shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900'}"
                >
                  По шаблону (Regex)
                </button>
                <button
                  type="button"
                  onclick={() => (topicsSelectionMode = 'CATALOG')}
                  class="px-2.5 py-1 rounded-md transition cursor-pointer {topicsSelectionMode === 'CATALOG' ? 'bg-white dark:bg-slate-700 text-sky-600 dark:text-sky-400 shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900'}"
                >
                  Выбор из каталога
                </button>
              </div>
            </div>

            <!-- РЕЖИМ REGEX (ПРЕДНАЗНАЧЕН ДЛЯ ТЫСЯЧ ТОПИКОВ) -->
            {#if topicsSelectionMode === 'REGEX'}
              <div class="space-y-3 bg-white dark:bg-slate-900 p-3.5 rounded-xl border border-slate-200 dark:border-slate-800">
                <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div>
                    <label class="block text-[11px] font-medium text-slate-500 mb-1">
                      Включающая маска топиков (Regex Whitelist):
                    </label>
                    <input
                      type="text"
                      bind:value={topicRegexPattern}
                      placeholder="orders-.*|payments-.*|billing-.* (или * для всех)"
                      class="w-full font-mono text-xs px-3 py-1.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500"
                    />
                    <div class="flex items-center gap-1.5 mt-1.5 text-[10px] text-slate-400">
                      <span>Шаблоны:</span>
                      <button type="button" onclick={() => (topicRegexPattern = 'orders-.*')} class="px-1.5 py-0.2 rounded bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 cursor-pointer">orders-.*</button>
                      <button type="button" onclick={() => (topicRegexPattern = 'billing-.*')} class="px-1.5 py-0.2 rounded bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 cursor-pointer">billing-.*</button>
                      <button type="button" onclick={() => (topicRegexPattern = '*')} class="px-1.5 py-0.2 rounded bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 cursor-pointer">* (Все)</button>
                    </div>
                  </div>

                  <div>
                    <label class="block text-[11px] font-medium text-slate-500 mb-1">
                      Исключающая маска (Blacklist regex):
                    </label>
                    <input
                      type="text"
                      bind:value={topicExcludePattern}
                      placeholder="__.*|.*-dlq|.*-temp"
                      class="w-full font-mono text-xs px-3 py-1.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500"
                    />
                    <div class="flex items-center gap-1.5 mt-1.5 text-[10px] text-slate-400">
                      <span>Исключать системные:</span>
                      <button type="button" onclick={() => (topicExcludePattern = '__.*|.*-dlq')} class="px-1.5 py-0.2 rounded bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 cursor-pointer">__.*|.*-dlq</button>
                    </div>
                  </div>
                </div>

                <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pt-2 border-t border-slate-100 dark:border-slate-800">
                  <div class="flex items-center gap-2">
                    <span class="text-[11px] text-slate-500">Режим кодирования по умолчанию:</span>
                    <select
                      bind:value={defaultTopicPayloadMode}
                      class="text-xs px-2.5 py-1 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg font-medium"
                    >
                      <option value="AUTO_DETECT">Auto-detect (Автоопределение)</option>
                      <option value="SCHEMA_REGISTRY">Schema Registry (Avro)</option>
                      <option value="PASSTHROUGH">Passthrough (Raw)</option>
                    </select>
                  </div>

                  <div class="flex items-center gap-2">
                    <span class="text-xs font-semibold text-slate-700 dark:text-slate-300">
                      Совпадает: <strong class="text-sky-600 dark:text-sky-400">{regexMatchedTopics.length}</strong> из {availableTopics.length}
                    </span>
                    <button
                      type="button"
                      onclick={() => (isTopicMatchPreviewOpen = !isTopicMatchPreviewOpen)}
                      class="text-[11px] px-2 py-0.5 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-300 cursor-pointer"
                    >
                      {isTopicMatchPreviewOpen ? 'Скрыть список' : 'Показать совпадения'}
                    </button>
                  </div>
                </div>

                {#if isTopicMatchPreviewOpen}
                  <div class="p-2.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 max-h-36 overflow-y-auto">
                    <div class="flex flex-wrap gap-1">
                      {#each regexMatchedTopics as mt}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[11px] font-mono bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-300">
                          {mt.name} ({mt.partitions} парт.)
                        </span>
                      {/each}
                    </div>
                  </div>
                {/if}
              </div>

            <!-- РЕЖИМ КАТАЛОГА С МГНОВЕННЫМ ПОИСКОМ -->
            {:else}
              <div class="space-y-2.5 bg-white dark:bg-slate-900 p-3.5 rounded-xl border border-slate-200 dark:border-slate-800">
                <div class="flex flex-col sm:flex-row gap-2 items-center justify-between">
                  <div class="relative flex-1 w-full">
                    <Search class="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
                    <input
                      type="search"
                      bind:value={topicSearchQuery}
                      placeholder="Поиск топика (orders, billing, payments)..."
                      class="w-full pl-8 pr-3 py-1.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-xs focus:outline-none focus:ring-1 focus:ring-sky-500"
                    />
                  </div>

                  <div class="flex items-center gap-1.5 flex-wrap shrink-0">
                    <select
                      bind:value={topicFilterFormat}
                      class="text-xs px-2 py-1 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg"
                    >
                      <option value="ALL">Все форматы</option>
                      <option value="SCHEMA_REGISTRY">Со схемами (SR)</option>
                      <option value="PASSTHROUGH">Passthrough</option>
                    </select>

                    <label class="flex items-center gap-1 text-[11px] cursor-pointer px-2 py-1 rounded bg-slate-100 dark:bg-slate-800">
                      <input type="checkbox" bind:checked={topicFilterSelectedOnly} class="rounded border-slate-300 text-sky-600" />
                      <span>Выбранные ({selectedTopics.length})</span>
                    </label>

                    <button
                      type="button"
                      onclick={selectAllFilteredTopics}
                      class="px-2 py-1 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-xs font-semibold hover:bg-slate-100 cursor-pointer"
                    >
                      Выбрать все ({filteredCatalogTopics.length})
                    </button>
                    <button
                      type="button"
                      onclick={clearAllTopics}
                      class="px-2 py-1 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-xs text-slate-400 hover:text-slate-600 cursor-pointer"
                    >
                      Снять
                    </button>
                  </div>
                </div>

                <!-- Список топиков -->
                <div class="space-y-1.5 border border-slate-200 dark:border-slate-800 rounded-lg p-2 max-h-48 overflow-y-auto bg-slate-50/50 dark:bg-slate-950/40">
                  {#if filteredCatalogTopics.length === 0}
                    <div class="text-center py-4 text-slate-400 text-xs">
                      Топики не найдены. Уточните поисковый запрос.
                    </div>
                  {:else}
                    {#each filteredCatalogTopics as t}
                      {@const isSelected = selectedTopics.some(item => item.topic === t.name)}
                      {@const currentMode = selectedTopics.find(item => item.topic === t.name)?.payloadMode || 'AUTO_DETECT'}
                      <div class="flex items-center justify-between p-2 rounded-lg bg-white dark:bg-slate-900 border {isSelected ? 'border-sky-500/60 ring-1 ring-sky-500/20' : 'border-slate-200 dark:border-slate-800'}">
                        <label class="flex items-center gap-2 cursor-pointer flex-1">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onchange={() => toggleTopic(t.name, t.detectedFormat)}
                            class="rounded border-slate-300 text-sky-600 focus:ring-sky-500"
                          />
                          <span class="font-mono text-xs font-medium text-slate-800 dark:text-slate-200">{t.name}</span>
                          <span class="text-[10px] text-slate-400">({t.partitions} парт.)</span>
                          {#if t.hasSchema}
                            <span class="text-[9px] px-1 rounded bg-indigo-50 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-300 font-semibold">Schema</span>
                          {/if}
                        </label>

                        {#if isSelected}
                          <select
                            value={currentMode}
                            onchange={(e) => changeTopicMode(t.name, e.currentTarget.value as any)}
                            class="text-[11px] px-2 py-0.5 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-slate-800 dark:text-slate-200 font-medium"
                          >
                            <option value="AUTO_DETECT">Auto-detect</option>
                            <option value="SCHEMA_REGISTRY">Schema Registry</option>
                            <option value="PASSTHROUGH">Passthrough</option>
                          </select>
                        {/if}
                      </div>
                    {/each}
                  {/if}
                </div>

                {#if selectedTopics.length > 0}
                  <div class="pt-1">
                    <div class="flex items-center justify-between text-[11px] font-medium text-slate-500 mb-1">
                      <span>Выбрано ({selectedTopics.length}):</span>
                      <div class="flex items-center gap-1 text-[10px]">
                        <span>Массово:</span>
                        <button type="button" onclick={() => applyModeToAllSelectedTopics('AUTO_DETECT')} class="hover:underline text-sky-600">Auto</button>
                        <span>|</span>
                        <button type="button" onclick={() => applyModeToAllSelectedTopics('SCHEMA_REGISTRY')} class="hover:underline text-indigo-600">Schema</button>
                        <span>|</span>
                        <button type="button" onclick={() => applyModeToAllSelectedTopics('PASSTHROUGH')} class="hover:underline text-amber-600">Raw</button>
                      </div>
                    </div>
                    <div class="flex flex-wrap gap-1 max-h-16 overflow-y-auto pr-1">
                      {#each selectedTopics as st}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-mono bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700">
                          <span>{st.topic}</span>
                          <span class="text-sky-600 font-bold">[{st.payloadMode}]</span>
                          <button type="button" onclick={() => removeTopic(st.topic)} class="hover:text-rose-500 font-bold ml-0.5 cursor-pointer">✕</button>
                        </span>
                      {/each}
                    </div>
                  </div>
                {/if}
              </div>
            {/if}
          </div>

          <!-- 5. SCHEMA REGISTRY (СТИЛЬ HDFS) -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-2.5">
            <label class="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" bind:checked={enableSchemaTranscoding} class="rounded border-slate-300 text-sky-600 focus:ring-sky-500" />
              <span class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                <Layers class="w-3.5 h-3.5 text-indigo-500" />
                <span>Кросс-регистрация и транскодинг Schema Registry</span>
              </span>
            </label>

            {#if enableSchemaTranscoding}
              <div class="grid grid-cols-1 md:grid-cols-2 gap-3 pt-1">
                <div>
                  <label class="block text-[11px] font-medium text-slate-500 mb-1">Реестр источника (Source SR):</label>
                  <select bind:value={sourceSrType} class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-medium">
                    <option value="HORTONWORKS">Hortonworks / Cloudera SR</option>
                    <option value="CONFLUENT">Confluent Schema Registry</option>
                  </select>
                  <input
                    type="text"
                    bind:value={sourceSrUrl}
                    placeholder="http://schema-registry-1:8081"
                    class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-mono"
                  />
                </div>
                <div>
                  <label class="block text-[11px] font-medium text-slate-500 mb-1">Реестр приемника (Target SR):</label>
                  <select bind:value={targetSrType} class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-medium">
                    <option value="CONFLUENT">Confluent Schema Registry</option>
                    <option value="HORTONWORKS">Hortonworks / Cloudera SR</option>
                  </select>
                  <input
                    type="text"
                    bind:value={targetSrUrl}
                    placeholder="http://schema-registry-2:8082"
                    class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-mono"
                  />
                </div>
              </div>
              <div class="pt-1">
                <label class="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" bind:checked={enableSrKerberos} class="rounded border-slate-300 text-sky-600 focus:ring-sky-500" />
                  <span class="text-[11px] text-slate-600 dark:text-slate-400">
                    Керберизованный Schema Registry (SPNEGO / HTTP Negotiate Kerberos)
                  </span>
                </label>
              </div>
            {/if}
          </div>

          <!-- 6. МАСШТАБИРУЕМЫЙ ВЫБОР CONSUMER GROUPS -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-3">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-200/60 dark:border-slate-800 pb-2.5">
              <div>
                <span class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                  <Clock class="w-3.5 h-3.5 text-indigo-500" />
                  <span>Синхронизация Consumer Groups ({effectiveGroupsCount} выбрано):</span>
                </span>
              </div>

              <!-- Табы выбора режима групп -->
              <div class="flex items-center gap-1 bg-slate-200/60 dark:bg-slate-800 p-0.5 rounded-lg text-[11px] font-semibold">
                <button
                  type="button"
                  onclick={() => (groupsSelectionMode = 'REGEX')}
                  class="px-2.5 py-1 rounded-md transition cursor-pointer {groupsSelectionMode === 'REGEX' ? 'bg-white dark:bg-slate-700 text-sky-600 dark:text-sky-400 shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900'}"
                >
                  По шаблону (Regex)
                </button>
                <button
                  type="button"
                  onclick={() => (groupsSelectionMode = 'CATALOG')}
                  class="px-2.5 py-1 rounded-md transition cursor-pointer {groupsSelectionMode === 'CATALOG' ? 'bg-white dark:bg-slate-700 text-sky-600 dark:text-sky-400 shadow-xs' : 'text-slate-600 dark:text-slate-400 hover:text-slate-900'}"
                >
                  Выбор из каталога
                </button>
              </div>
            </div>

            <!-- РЕЖИМ REGEX ДЛЯ ТЫСЯЧ ГРУПП -->
            {#if groupsSelectionMode === 'REGEX'}
              <div class="space-y-3 bg-white dark:bg-slate-900 p-3.5 rounded-xl border border-slate-200 dark:border-slate-800">
                <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div>
                    <label class="block text-[11px] font-medium text-slate-500 mb-1">
                      Маска групп (Regex Whitelist):
                    </label>
                    <input
                      type="text"
                      bind:value={groupRegexPattern}
                      placeholder="billing-.*|fraud-.*|analytics-.* (или * для всех)"
                      class="w-full font-mono text-xs px-3 py-1.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500"
                    />
                    <div class="flex items-center gap-1.5 mt-1.5 text-[10px] text-slate-400">
                      <span>Шаблоны:</span>
                      <button type="button" onclick={() => (groupRegexPattern = 'billing-.*')} class="px-1.5 py-0.2 rounded bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 cursor-pointer">billing-.*</button>
                      <button type="button" onclick={() => (groupRegexPattern = 'fraud-.*')} class="px-1.5 py-0.2 rounded bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 cursor-pointer">fraud-.*</button>
                      <button type="button" onclick={() => (groupRegexPattern = '*')} class="px-1.5 py-0.2 rounded bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 cursor-pointer">* (Все)</button>
                    </div>
                  </div>

                  <div>
                    <label class="block text-[11px] font-medium text-slate-500 mb-1">
                      Исключающая маска (Blacklist regex):
                    </label>
                    <input
                      type="text"
                      bind:value={groupExcludePattern}
                      placeholder=".*-archiver.*|dev-.*"
                      class="w-full font-mono text-xs px-3 py-1.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500"
                    />
                  </div>
                </div>

                <div class="flex items-center justify-between pt-2 border-t border-slate-100 dark:border-slate-800">
                  <span class="text-xs font-semibold text-slate-700 dark:text-slate-300">
                    Совпадает: <strong class="text-sky-600 dark:text-sky-400">{regexMatchedGroups.length}</strong> из {availableGroups.length}
                  </span>
                  <button
                    type="button"
                    onclick={() => (isGroupMatchPreviewOpen = !isGroupMatchPreviewOpen)}
                    class="text-[11px] px-2 py-0.5 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-300 cursor-pointer"
                  >
                    {isGroupMatchPreviewOpen ? 'Скрыть список' : 'Показать совпадения'}
                  </button>
                </div>

                {#if isGroupMatchPreviewOpen}
                  <div class="p-2.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 max-h-32 overflow-y-auto">
                    <div class="flex flex-wrap gap-1">
                      {#each regexMatchedGroups as mg}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[11px] font-mono bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-300">
                          {mg.groupId} <span class="text-[9px] text-slate-400">({mg.state})</span>
                        </span>
                      {/each}
                    </div>
                  </div>
                {/if}
              </div>

            <!-- РЕЖИМ КАТАЛОГА ГРУПП С ПОИСКОМ -->
            {:else}
              <div class="space-y-2.5 bg-white dark:bg-slate-900 p-3.5 rounded-xl border border-slate-200 dark:border-slate-800">
                <div class="flex flex-col sm:flex-row gap-2 items-center justify-between">
                  <div class="relative flex-1 w-full">
                    <Search class="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
                    <input
                      type="search"
                      bind:value={groupSearchQuery}
                      placeholder="Поиск консьюмер-группы по имени..."
                      class="w-full pl-8 pr-3 py-1.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-xs focus:outline-none focus:ring-1 focus:ring-sky-500"
                    />
                  </div>

                  <div class="flex items-center gap-1.5 flex-wrap shrink-0">
                    <select
                      bind:value={groupFilterState}
                      class="text-xs px-2 py-1 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg"
                    >
                      <option value="ALL">Все состояния</option>
                      <option value="STABLE">STABLE</option>
                      <option value="EMPTY">EMPTY</option>
                    </select>

                    <label class="flex items-center gap-1 text-[11px] cursor-pointer px-2 py-1 rounded bg-slate-100 dark:bg-slate-800">
                      <input type="checkbox" bind:checked={groupFilterSelectedOnly} class="rounded border-slate-300 text-sky-600" />
                      <span>Выбранные ({selectedGroups.length})</span>
                    </label>

                    <button
                      type="button"
                      onclick={selectAllFilteredGroups}
                      class="px-2 py-1 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-xs font-semibold hover:bg-slate-100 cursor-pointer"
                    >
                      Выбрать все ({filteredCatalogGroups.length})
                    </button>
                    <button
                      type="button"
                      onclick={clearAllGroups}
                      class="px-2 py-1 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800 text-xs text-slate-400 hover:text-slate-600 cursor-pointer"
                    >
                      Снять
                    </button>
                  </div>
                </div>

                <!-- Список групп -->
                <div class="space-y-1.5 border border-slate-200 dark:border-slate-800 rounded-lg p-2 max-h-48 overflow-y-auto bg-slate-50/50 dark:bg-slate-950/40">
                  {#if filteredCatalogGroups.length === 0}
                    <div class="text-center py-4 text-slate-400 text-xs">
                      Консьюмер-группы не найдены. Уточните поиск.
                    </div>
                  {:else}
                    {#each filteredCatalogGroups as g}
                      <label class="flex items-center justify-between p-2 rounded-lg bg-white dark:bg-slate-900 border {selectedGroups.includes(g.groupId) ? 'border-sky-500/60 ring-1 ring-sky-500/20' : 'border-slate-200 dark:border-slate-800'} cursor-pointer">
                        <div class="flex items-center gap-2">
                          <input
                            type="checkbox"
                            checked={selectedGroups.includes(g.groupId)}
                            onchange={() => toggleGroup(g.groupId)}
                            class="rounded border-slate-300 text-sky-600 focus:ring-sky-500"
                          />
                          <span class="font-mono text-xs font-medium text-slate-800 dark:text-slate-200">{g.groupId}</span>
                        </div>
                        <div class="flex items-center gap-2 text-[10px]">
                          <span class="px-1.5 py-0.2 rounded font-semibold {g.state === 'STABLE' ? 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-400' : 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'}">
                            {g.state}
                          </span>
                          <span class="text-slate-400">Участников: {g.members}</span>
                          <span class="text-slate-400">Лаг: <strong class="font-mono">{g.lag}</strong></span>
                        </div>
                      </label>
                    {/each}
                  {/if}
                </div>

                {#if selectedGroups.length > 0}
                  <div class="pt-1">
                    <div class="text-[11px] font-medium text-slate-500 mb-1">
                      Выбранные ({selectedGroups.length}):
                    </div>
                    <div class="flex flex-wrap gap-1 max-h-16 overflow-y-auto pr-1">
                      {#each selectedGroups as sg}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-mono bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700">
                          <span>{sg}</span>
                          <button type="button" onclick={() => removeGroup(sg)} class="hover:text-rose-500 font-bold ml-0.5 cursor-pointer">✕</button>
                        </span>
                      {/each}
                    </div>
                  </div>
                {/if}
              </div>
            {/if}
          </div>

          <!-- 7. KERBEROS SASL GSSAPI ДЛЯ KAFKA -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-2.5">
            <label class="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" bind:checked={enableKerberos} class="rounded border-slate-300 text-sky-600 focus:ring-sky-500" />
              <span class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                <Shield class="w-3.5 h-3.5 text-slate-500" />
                <span>Керберизованная Kafka (Kerberos SASL GSSAPI)</span>
              </span>
            </label>

            {#if enableKerberos}
              <div class="grid grid-cols-1 md:grid-cols-2 gap-3 pt-1">
                <div>
                  <label class="block text-[11px] font-medium text-slate-500 mb-1">Source Principal & Keytab:</label>
                  <input
                    type="text"
                    bind:value={sourcePrincipal}
                    placeholder="kafka-replicator@COMPANY.LOCAL"
                    class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-mono"
                  />
                  <input
                    type="text"
                    bind:value={sourceKeytabPath}
                    placeholder="/etc/security/keytabs/kafka-replicator.keytab"
                    class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-mono"
                  />
                </div>
                <div>
                  <label class="block text-[11px] font-medium text-slate-500 mb-1">Target Principal & Keytab:</label>
                  <input
                    type="text"
                    bind:value={targetPrincipal}
                    placeholder="kafka-replicator@COMPANY.LOCAL"
                    class="w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-mono"
                  />
                  <input
                    type="text"
                    bind:value={targetKeytabPath}
                    placeholder="/etc/security/keytabs/kafka-replicator.keytab"
                    class="mt-1.5 w-full text-xs px-2.5 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-mono"
                  />
                </div>
              </div>
            {/if}
          </div>

          <!-- 8. ПОЛОСА WAN И КАЧЕСТВО ОБСЛУЖИВАНИЯ (HTB QOS) -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-3">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-200/60 dark:border-slate-800 pb-2">
              <span class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5 text-xs">
                <Gauge class="w-4 h-4 text-emerald-500" />
                <span>Межкластерная полоса и QoS (Hierarchical Token Bucket):</span>
              </span>
              <label class="flex items-center gap-1.5 cursor-pointer text-[11px] text-slate-600 dark:text-slate-400 font-medium">
                <input
                  type="checkbox"
                  bind:checked={isAutoBandwidth}
                  class="rounded border-slate-300 text-emerald-600 focus:ring-emerald-500"
                />
                <span>Авто-выделение HTB и тюнинг параметров</span>
              </label>
            </div>

            {#if isAutoBandwidth}
              <div class="p-3 {isAgentBottleneck ? 'bg-amber-50/80 dark:bg-amber-950/30 border-amber-200 dark:border-amber-800/60' : 'bg-emerald-50/70 dark:bg-emerald-950/30 border-emerald-200 dark:border-emerald-800/60'} border rounded-lg flex items-start gap-2.5">
                <Zap class="w-4 h-4 {isAgentBottleneck ? 'text-amber-600 dark:text-amber-400' : 'text-emerald-600 dark:text-emerald-400'} shrink-0 mt-0.5" />
                <div class="text-xs space-y-1">
                  <div class="flex items-center gap-2 flex-wrap">
                    <span class="font-semibold {isAgentBottleneck ? 'text-amber-900 dark:text-amber-200' : 'text-emerald-900 dark:text-emerald-200'}">
                      Двухконтурное авто-регулирование QoS: ~{autoBandwidthMb} МБ/с
                    </span>
                    {#if isAgentBottleneck}
                      <span class="text-[10px] font-bold px-1.5 py-0.5 rounded bg-amber-100 dark:bg-amber-900/60 text-amber-800 dark:text-amber-300">
                        Ограничено лимитами агентов
                      </span>
                    {:else}
                      <span class="text-[10px] font-bold px-1.5 py-0.5 rounded bg-emerald-100 dark:bg-emerald-900/60 text-emerald-800 dark:text-emerald-300">
                        Ограничено WAN HTB (40%)
                      </span>
                    {/if}
                  </div>
                  <p class="text-[11px] {isAgentBottleneck ? 'text-amber-800/90 dark:text-amber-300/80' : 'text-emerald-700/90 dark:text-emerald-300/80'} leading-relaxed">
                    WAN квота: <strong>{wanHtbBandwidthMb} МБ/с</strong> (40% от лимита канала {detectedDcLimitMb} МБ/с).
                    {#if totalSendersCapacityMb !== null}
                      Суммарный лимит подключенных агентов: <strong>{totalSendersCapacityMb} МБ/с</strong> ({liveSendersCount} {liveSendersCount === 1 ? 'узел' : 'узла'}).
                    {/if}
                    Параллелизм вычитки рассчитан индивидуально под полосу каждого узла (~{autoConsumerThreadsPerAgent} треда/узел, суммарно {autoTotalThreads}), размер пачки ~{autoBatchSize} зап., оффсеты ~{autoOffsetSyncInterval} с.
                  </p>
                  <p class="text-[10px] text-slate-500 dark:text-slate-400">
                    ℹ️ Соблюдаются локальные лимиты агентов (--max-bandwidth-mb-s), заданные инженером при старте демонов на узлах.
                  </p>
                </div>
              </div>
            {/if}

            <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <!-- 1. Полоса WAN -->
              <div class="p-2.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg">
                <div class="flex items-center justify-between mb-1">
                  <label class="text-[11px] font-medium text-slate-500">Полоса WAN:</label>
                  {#if isAutoBandwidth}
                    <span class="text-[10px] font-semibold text-emerald-600 bg-emerald-50 dark:bg-emerald-950/50 px-1.5 py-0.2 rounded">HTB Авто</span>
                  {/if}
                </div>
                {#if isAutoBandwidth}
                  <div class="text-xs font-mono font-bold text-emerald-700 dark:text-emerald-300 py-1">
                    ~{autoBandwidthMb} МБ/с
                  </div>
                  <div class="text-[10px] text-slate-400 dark:text-slate-500">40% квота линка</div>
                {:else}
                  <input
                    type="number"
                    bind:value={bandwidthLimitMb}
                    min="1"
                    max="100000"
                    class="w-full px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold"
                  />
                  <div class="text-[10px] text-slate-400 dark:text-slate-500 mt-1">Ручной лимит МБ/с</div>
                {/if}
              </div>

              <!-- 2. Смещение (сек) -->
              <div class="p-2.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg">
                <div class="flex items-center justify-between mb-1">
                  <label class="text-[11px] font-medium text-slate-500">Смещение (сек):</label>
                  {#if isAutoBandwidth}
                    <span class="text-[10px] font-semibold text-sky-600 bg-sky-50 dark:bg-sky-950/50 px-1.5 py-0.2 rounded">SLA Авто</span>
                  {/if}
                </div>
                {#if isAutoBandwidth}
                  <div class="text-xs font-mono font-bold text-sky-700 dark:text-sky-300 py-1">
                    ~{autoOffsetSyncInterval} сек
                  </div>
                  <div class="text-[10px] text-slate-400 dark:text-slate-500">Синхронизация групп</div>
                {:else}
                  <input
                    type="number"
                    bind:value={offsetSyncIntervalSec}
                    min="1"
                    max="60"
                    class="w-full px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold"
                  />
                  <div class="text-[10px] text-slate-400 dark:text-slate-500 mt-1">Интервал (1–60 с)</div>
                {/if}
              </div>

              <!-- 3. Потоки вычитки -->
              <div class="p-2.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg">
                <div class="flex items-center justify-between mb-1">
                  <label class="text-[11px] font-medium text-slate-500">Потоки вычитки:</label>
                  {#if isAutoBandwidth}
                    <span class="text-[10px] font-semibold text-indigo-600 bg-indigo-50 dark:bg-indigo-950/50 px-1.5 py-0.2 rounded">Шардинг</span>
                  {/if}
                </div>
                {#if isAutoBandwidth}
                  <div class="text-xs font-mono font-bold text-indigo-700 dark:text-indigo-300 py-1">
                    ~{autoConsumerThreadsPerAgent} / узел
                  </div>
                  <div class="text-[10px] text-slate-400 dark:text-slate-500">
                    всего ~{autoTotalThreads} на {liveSendersCount} {liveSendersCount === 1 ? 'агент' : (liveSendersCount >= 2 && liveSendersCount <= 4 ? 'агента' : 'агентов')}
                  </div>
                {:else}
                  <input
                    type="number"
                    bind:value={consumerThreads}
                    min="1"
                    max="128"
                    class="w-full px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold text-indigo-600 dark:text-indigo-400"
                  />
                  <div class="text-[10px] text-slate-400 dark:text-slate-500 mt-1">
                    на узел (всего {consumerThreads * liveSendersCount})
                  </div>
                {/if}
              </div>

              <!-- 4. Размер батча -->
              <div class="p-2.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg">
                <div class="flex items-center justify-between mb-1">
                  <label class="text-[11px] font-medium text-slate-500">Размер батча:</label>
                  {#if isAutoBandwidth}
                    <span class="text-[10px] font-semibold text-purple-600 bg-purple-50 dark:bg-purple-950/50 px-1.5 py-0.2 rounded">Micro-batch</span>
                  {/if}
                </div>
                {#if isAutoBandwidth}
                  <div class="text-xs font-mono font-bold text-purple-700 dark:text-purple-300 py-1">
                    ~{autoBatchSize} зап.
                  </div>
                  <div class="text-[10px] text-slate-400 dark:text-slate-500">Пачка gRPC стрима</div>
                {:else}
                  <input
                    type="number"
                    bind:value={batchSize}
                    min="100"
                    max="100000"
                    step="500"
                    class="w-full px-2 py-1 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded text-xs font-mono font-bold text-purple-600 dark:text-purple-400"
                  />
                  <div class="text-[10px] text-slate-400 dark:text-slate-500 mt-1">Размер (100–100k)</div>
                {/if}
              </div>
            </div>
          </div>

          <!-- 9. ГОРИЗОНТАЛЬНОЕ МАСШТАБИРОВАНИЕ ПО АГЕНТАМ (MULTI-AGENT LOAD BALANCING) -->
          <div class="p-3.5 bg-slate-50 dark:bg-slate-950/60 border border-slate-200/80 dark:border-slate-800 rounded-xl space-y-3">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-200/60 dark:border-slate-800 pb-2">
              <span class="font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1.5 text-xs">
                <Radio class="w-4 h-4 text-sky-500" />
                <span>Распределение нагрузки по агентам (Multi-Agent Cluster Scaling):</span>
              </span>
              <div class="flex items-center gap-2">
                <button
                  type="button"
                  onclick={() => loadLiveAgents(newSourceDc, newTargetDc)}
                  disabled={isLoadingAgents}
                  class="p-1 text-slate-400 hover:text-sky-600 dark:hover:text-sky-400 transition"
                  title="Обновить список активных агентов"
                >
                  <RefreshCw class="w-3.5 h-3.5 {isLoadingAgents ? 'animate-spin text-sky-500' : ''}" />
                </button>
                <label class="flex items-center gap-1.5 cursor-pointer text-[11px] text-slate-600 dark:text-slate-400 font-medium">
                  <input
                    type="checkbox"
                    bind:checked={isMultiAgentScaling}
                    class="rounded border-slate-300 text-sky-600 focus:ring-sky-500"
                  />
                  <span>Включено (Шардирование Source + Target)</span>
                </label>
              </div>
            </div>

            {#if isMultiAgentScaling}
              <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <!-- Пул отправителей -->
                <div class="p-3 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg space-y-2">
                  <div class="flex items-center justify-between">
                    <span class="text-[11px] font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1">
                      <Server class="w-3.5 h-3.5 text-indigo-500" />
                      <span>Отправители ({newSourceDc}):</span>
                    </span>
                    <span class="text-[10px] text-slate-400">Шардинг партиций</span>
                  </div>
                  {#if liveSenderDetails.length > 0}
                    <div class="space-y-1.5">
                      {#each liveSenderDetails as s}
                        {@const agentBw = s.max_bandwidth_mb_s || 100}
                        {@const agentThreads = Math.min(32, Math.max(1, Math.ceil(agentBw / 35)))}
                        <div class="flex items-center justify-between text-xs px-2 py-1.5 bg-slate-50 dark:bg-slate-800/60 rounded border border-slate-100 dark:border-slate-700/60 font-mono">
                          <div class="flex flex-col min-w-0 pr-1">
                            <span class="font-semibold text-slate-700 dark:text-slate-300 truncate">{s.agent_id || s.grpc_address}</span>
                            <span class="text-[10px] text-slate-400 truncate">{s.grpc_address}</span>
                          </div>
                          <div class="flex items-center gap-1.5 shrink-0">
                            <span class="text-[10px] px-1.5 py-0.5 rounded bg-indigo-50 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-300 font-semibold" title="Локальный лимит процесса агента, заданный при старте (--max-bandwidth-mb-s)">
                              {agentBw} МБ/с
                            </span>
                            <span class="text-[10px] px-1.5 py-0.5 rounded bg-sky-50 dark:bg-sky-950 text-sky-700 dark:text-sky-300 font-semibold" title="Рекомендуемое число потоков на агент">
                              {agentThreads} thr
                            </span>
                            <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold flex items-center gap-1">
                              <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span> ONLINE
                            </span>
                          </div>
                        </div>
                      {/each}
                    </div>
                  {:else if liveSenders.length > 0}
                    <div class="space-y-1">
                      {#each liveSenders as sender}
                        <div class="flex items-center justify-between text-xs px-2 py-1 bg-slate-50 dark:bg-slate-800/60 rounded border border-slate-100 dark:border-slate-700/60 font-mono">
                          <span>{sender}</span>
                          <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold flex items-center gap-1">
                            <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span> ONLINE
                          </span>
                        </div>
                      {/each}
                    </div>
                  {:else}
                    <div class="text-[11px] text-slate-500 dark:text-slate-400 italic py-1">
                      Агенты кластера {newSourceDc} подключатся автоматически при регистрации.
                    </div>
                  {/if}
                  <p class="text-[10px] text-slate-400">
                    Партиции распределяются между физическими нодами отправителей через общую Consumer Group <code class="text-indigo-500 font-mono">kafka-replicator-{newJobName.slice(0, 12)}</code>.
                  </p>
                </div>

                <!-- Пул приемников -->
                <div class="p-3 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-lg space-y-2">
                  <div class="flex items-center justify-between">
                    <span class="text-[11px] font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-1">
                      <Radio class="w-3.5 h-3.5 text-sky-500" />
                      <span>Приёмники ({newTargetDc}):</span>
                    </span>
                    <span class="text-[10px] text-slate-400">Partition-Pinned</span>
                  </div>
                  {#if liveReceivers.length > 0}
                    <div class="space-y-1">
                      {#each liveReceivers as receiver}
                        <div class="flex items-center justify-between text-xs px-2 py-1 bg-slate-50 dark:bg-slate-800/60 rounded border border-slate-100 dark:border-slate-700/60 font-mono">
                          <span>{receiver}</span>
                          <span class="text-[10px] text-emerald-600 dark:text-emerald-400 font-bold flex items-center gap-1">
                            <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span> ONLINE
                          </span>
                        </div>
                      {/each}
                    </div>
                  {:else}
                    <div class="text-[11px] text-slate-500 dark:text-slate-400 italic py-1">
                      Агенты целевого кластера {newTargetDc} будут обнаружены динамически.
                    </div>
                  {/if}
                  <p class="text-[10px] text-slate-400">
                    Партиции детерминированно привязываются к узлам (<code class="text-sky-500 font-mono">partition % N</code>) с сохранением строгого порядка сообщений.
                  </p>
                </div>
              </div>
            {/if}

            <details class="text-[11px] text-slate-500 pt-1">
              <summary class="cursor-pointer hover:text-slate-700 dark:hover:text-slate-300 transition font-medium">
                Ручное переопределение списка целевых эндпоинтов (опционально)
              </summary>
              <div class="mt-2 space-y-1">
                <input
                  type="text"
                  bind:value={targetEndpointsInput}
                  placeholder="agent-1:50052, agent-2:50052"
                  class="w-full text-xs px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 font-mono"
                />
                <p class="text-[10px] text-slate-400">
                  Оставьте пустым для полностью автоматического назначения живых приёмников из реестра оркестратора.
                </p>
              </div>
            </details>
          </div>
        </div>

        <!-- Подвал модального окна -->
        <div class="flex items-center justify-between pt-3 border-t border-slate-100 dark:border-slate-800">
          <div class="text-xs text-slate-500 dark:text-slate-400">
            Готово к репликации: <strong class="text-slate-800 dark:text-slate-200">{effectiveTopicsCount} топиков</strong>,
            смещения: <strong class="text-slate-800 dark:text-slate-200">{effectiveGroupsCount} групп</strong>
          </div>

          <div class="flex items-center gap-3">
            <button
              type="button"
              onclick={() => (isCreateModalOpen = false)}
              class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-800 text-xs font-semibold hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
            >
              Отмена
            </button>
            <button
              type="button"
              onclick={handleCreateJob}
              disabled={effectiveTopicsCount === 0}
              class="px-5 py-2 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-xs font-semibold shadow-md shadow-sky-600/20 transition cursor-pointer disabled:opacity-50"
            >
              Создать задачу
            </button>
          </div>
        </div>
      </div>
    </div>
  {/if}
</div>
