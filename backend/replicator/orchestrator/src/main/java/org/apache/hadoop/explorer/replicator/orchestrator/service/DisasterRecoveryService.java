package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrActionResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrReverseRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRunRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service
public class DisasterRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(DisasterRecoveryService.class);

    private final JobRepository jobRepository;
    private final JobRunRepository jobRunRepository;
    private final HmsReplicationJobRepository hmsReplicationJobRepository;
    private final AgentRegistry agentRegistry;
    private final TopologyRegistry topologyRegistry;
    private final TokenBucketThrottler throttler;
    private final HmsCoordinatorService hmsCoordinatorService;

    public DisasterRecoveryService(
            JobRepository jobRepository,
            JobRunRepository jobRunRepository,
            HmsReplicationJobRepository hmsReplicationJobRepository,
            AgentRegistry agentRegistry,
            TopologyRegistry topologyRegistry,
            TokenBucketThrottler throttler,
            HmsCoordinatorService hmsCoordinatorService
    ) {
        this.jobRepository = jobRepository;
        this.jobRunRepository = jobRunRepository;
        this.hmsReplicationJobRepository = hmsReplicationJobRepository;
        this.agentRegistry = agentRegistry;
        this.topologyRegistry = topologyRegistry;
        this.throttler = throttler;
        this.hmsCoordinatorService = hmsCoordinatorService;
    }

    /**
     * Формирование сводного статуса всех дата-центров, кластеров, маршрутов и готовности к Failover.
     */
    @Transactional(readOnly = true)
    public DrStatusResponse getDrStatus() {
        List<AgentResponseDto> agents = agentRegistry.getAgentDtos();
        List<ReplicatorProperties.DatacenterConfig> dcs = topologyRegistry.getDatacenters();
        List<JobEntity> allJobs = jobRepository.findAll().stream()
                .filter(j -> j.getParentJobId() == null)
                .toList();
        List<HmsReplicationJobEntity> allHmsJobs = hmsReplicationJobRepository.findAll();

        // 1. Статус Дата-Центров
        List<DrStatusResponse.DrDcStatus> dcStatuses = new ArrayList<>();
        boolean primaryOnline = false;
        boolean standbyOnline = false;

        for (var dc : dcs) {
            String dcId = dc.getId();
            List<AgentResponseDto> dcAgents = agents.stream()
                    .filter(a -> matchesDc(a.clusterId(), dcId))
                    .toList();

            int onlineCount = (int) dcAgents.stream().filter(a -> "online".equalsIgnoreCase(a.status())).count();
            int totalCount = dcAgents.size();

            String status = onlineCount > 0 ? "ONLINE" : (totalCount > 0 ? "OFFLINE" : "UNKNOWN");
            if (onlineCount > 0 && onlineCount < totalCount) {
                status = "DEGRADED";
            }

            // Роль: DC1 по умолчанию Primary, DC2 — Standby (или Promoted если в DC2 есть активные исходящие джобы)
            boolean isDc1 = dcId.toLowerCase().contains("dc1") || dcId.toLowerCase().contains("primary");
            boolean isDc2 = dcId.toLowerCase().contains("dc2") || dcId.toLowerCase().contains("standby");

            if (isDc1 && onlineCount > 0) primaryOnline = true;
            if (isDc2 && onlineCount > 0) standbyOnline = true;

            // Проверяем направление активных задач: если из DC2 идут активные задачи, значит DC2 сейчас Promoted
            long outJobsFromDc = allJobs.stream()
                    .filter(j -> matchesDc(j.getSourceClusterId(), dcId) && "RUNNING".equalsIgnoreCase(j.getStatus()))
                    .count();

            String role;
            if (isDc1) {
                role = (outJobsFromDc > 0 || totalCount == 0) ? "PRIMARY" : "STANDBY";
            } else if (isDc2) {
                role = outJobsFromDc > 0 ? "PROMOTED_PRIMARY" : "STANDBY";
            } else {
                role = "STANDBY";
            }

            // Проверяем лимиты пропускной способности (Fencing)
            long limitBytes = 100L * 1024 * 1024;
            boolean isFenced = false;
            for (var otherDc : dcs) {
                if (!otherDc.getId().equalsIgnoreCase(dcId)) {
                    Optional<Long> optLimit = topologyRegistry.getDcLimit(dcId, otherDc.getId());
                    if (optLimit.isPresent()) {
                        long l = optLimit.get();
                        if (l == 0) {
                            isFenced = true;
                        } else {
                            limitBytes = l;
                        }
                    }
                }
            }

            double limitMbS = Math.round((limitBytes / (1024.0 * 1024.0)) * 10.0) / 10.0;

            dcStatuses.add(new DrStatusResponse.DrDcStatus(
                    dcId,
                    dc.getName() != null ? dc.getName() : dcId.toUpperCase(),
                    status,
                    role,
                    onlineCount,
                    totalCount,
                    limitMbS,
                    isFenced
            ));
        }

        // 2. Статус Кластеров
        List<DrStatusResponse.DrClusterStatus> clusterStatuses = new ArrayList<>();
        for (var c : topologyRegistry.getClusters()) {
            String cId = c.getId();
            int active = 0, queued = 0, failed = 0, completed = 0;
            for (var j : allJobs) {
                if (matchesCluster(j.getSourceClusterId(), cId)) {
                    String st = j.getStatus() != null ? j.getStatus().toUpperCase() : "";
                    switch (st) {
                        case "RUNNING", "STREAMING" -> active++;
                        case "QUEUED", "ANALYZING" -> queued++;
                        case "FAILED" -> failed++;
                        case "COMPLETED" -> completed++;
                    }
                }
            }

            List<AgentResponseDto> clusterAgents = agents.stream()
                    .filter(a -> matchesCluster(a.clusterId(), cId))
                    .toList();
            boolean isOnline = clusterAgents.stream().anyMatch(a -> "online".equalsIgnoreCase(a.status()));

            clusterStatuses.add(new DrStatusResponse.DrClusterStatus(
                    cId,
                    c.getName() != null ? c.getName() : cId,
                    c.getDcId(),
                    isOnline ? "ONLINE" : "OFFLINE",
                    active,
                    queued,
                    failed,
                    completed
            ));
        }

        // 3. Маршруты HDFS
        List<DrStatusResponse.DrRouteItem> hdfsRoutes = new ArrayList<>();
        long totalUnreplicatedBytes = 0;
        int activeJobsCount = 0;
        int frozenJobsCount = 0;
        int failedJobsCount = 0;

        for (JobEntity j : allJobs) {
            String status = j.getStatus() != null ? j.getStatus().toUpperCase() : "UNKNOWN";
            if ("RUNNING".equalsIgnoreCase(status) || "STREAMING".equalsIgnoreCase(status)) activeJobsCount++;
            if ("STOPPED".equalsIgnoreCase(status) || "CANCELLED".equalsIgnoreCase(status)) frozenJobsCount++;
            if ("FAILED".equalsIgnoreCase(status)) failedJobsCount++;

            long lag = Math.max(0, j.getTotalBytes() - j.getCopiedBytes());
            totalUnreplicatedBytes += lag;

            // Поиск зеркальной обратной задачи
            Optional<JobEntity> reverse = allJobs.stream()
                    .filter(rj -> !rj.getId().equals(j.getId())
                            && matchesCluster(rj.getSourceClusterId(), j.getTargetClusterId())
                            && matchesCluster(rj.getTargetClusterId(), j.getSourceClusterId())
                            && isSameOrFlippedPath(rj.getSourcePath(), j.getTargetPath()))
                    .findFirst();

            hdfsRoutes.add(new DrStatusResponse.DrRouteItem(
                    j.getId(),
                    "HDFS",
                    j.getSourcePath() + " ➔ " + j.getTargetPath(),
                    j.getSourceClusterId(),
                    j.getTargetClusterId(),
                    j.getSourcePath(),
                    j.getTargetPath(),
                    status,
                    j.isScheduled(),
                    j.getCronExpression(),
                    lag,
                    reverse.isPresent(),
                    reverse.map(JobEntity::getId).orElse(null)
            ));
        }

        // 4. Маршруты HMS
        List<DrStatusResponse.DrRouteItem> hmsRoutes = new ArrayList<>();
        long totalUnreplicatedEvents = 0;

        for (HmsReplicationJobEntity h : allHmsJobs) {
            String status = h.getStatus() != null ? h.getStatus().toUpperCase() : "UNKNOWN";
            if ("ACTIVE".equalsIgnoreCase(status) || "BOOTSTRAPPING".equalsIgnoreCase(status)) activeJobsCount++;
            if ("PAUSED".equalsIgnoreCase(status)) frozenJobsCount++;
            if ("ERROR".equalsIgnoreCase(status)) failedJobsCount++;

            long eventLag = h.getEventLag() != null ? h.getEventLag() : 0L;
            totalUnreplicatedEvents += eventLag;

            Optional<HmsReplicationJobEntity> reverseHms = allHmsJobs.stream()
                    .filter(rh -> !rh.getId().equals(h.getId())
                            && matchesCluster(rh.getSourceClusterId(), h.getTargetClusterId())
                            && matchesCluster(rh.getTargetClusterId(), h.getSourceClusterId())
                            && Objects.equals(rh.getSourceDbName(), h.getTargetDbName()))
                    .findFirst();

            hmsRoutes.add(new DrStatusResponse.DrRouteItem(
                    h.getId(),
                    "HMS",
                    "HMS: " + h.getSourceDbName() + " ➔ " + h.getTargetDbName(),
                    h.getSourceClusterId(),
                    h.getTargetClusterId(),
                    h.getSourceDbName(),
                    h.getTargetDbName(),
                    status,
                    true,
                    "CDC Event Stream",
                    eventLag,
                    reverseHms.isPresent(),
                    reverseHms.map(HmsReplicationJobEntity::getId).orElse(null)
            ));
        }

        // Определение активного направления
        String activeSrcDc = "dc1";
        String activeDstDc = "dc2";
        long dc2SourceCount = allJobs.stream()
                .filter(j -> matchesDc(j.getSourceClusterId(), "dc2") && ("RUNNING".equalsIgnoreCase(j.getStatus()) || "QUEUED".equalsIgnoreCase(j.getStatus())))
                .count();
        if (dc2SourceCount > 0) {
            activeSrcDc = "dc2";
            activeDstDc = "dc1";
        }

        DrStatusResponse.DrSummary summary = new DrStatusResponse.DrSummary(
                activeSrcDc,
                activeDstDc,
                primaryOnline,
                standbyOnline,
                activeJobsCount,
                frozenJobsCount,
                failedJobsCount,
                totalUnreplicatedBytes,
                totalUnreplicatedEvents
        );

        return new DrStatusResponse(dcStatuses, clusterStatuses, summary, hdfsRoutes, hmsRoutes);
    }

    /**
     * Экстренная остановка (Kill-Switch) всех репликаций из указанного кластера-источника.
     * Защищает целевой ЦОД от Split-Brain и блокирует очереди.
     */
    @Transactional
    public DrActionResponse emergencyStop(DrEmergencyStopRequest req, String username) {
        String clusterId = req.clusterId();
        String operator = username != null ? username : "operator";
        String note = req.reason() != null && !req.reason().isBlank() ? ": " + req.reason() : "";
        Instant now = Instant.now();

        log.warn("[Disaster Recovery] АВАРИЙНЫЙ ОСТАНОВ (Kill-Switch) для кластера '{}' оператором '{}'{}",
                clusterId, operator, note);

        int stoppedHdfs = 0;
        List<JobEntity> jobs = jobRepository.findAll();
        for (JobEntity j : jobs) {
            if (matchesCluster(j.getSourceClusterId(), clusterId)) {
                String status = j.getStatus() != null ? j.getStatus().toUpperCase() : "";
                if ("RUNNING".equals(status) || "QUEUED".equals(status) || "ANALYZING".equals(status) || "STREAMING".equals(status)) {
                    j.setStatus("STOPPED");
                    j.setScheduled(false); // Отключаем планировщик
                    j.setCompletedAt(now);
                    j.setMessage("Экстренная остановка (Kill-Switch) оператором " + operator + note);

                    if (j.getActiveRunId() != null) {
                        jobRunRepository.findById(j.getActiveRunId()).ifPresent(run -> {
                            run.setStatus("STOPPED");
                            run.setCompletedAt(now);
                            run.setMessage("Остановлено аварийным Kill-Switch");
                            run.calculateMetrics();
                            jobRunRepository.save(run);
                        });
                        j.setActiveRunId(null);
                    }
                    jobRepository.save(j);
                    stoppedHdfs++;
                } else if (j.isScheduled()) {
                    // Если задача ждет по расписанию, снимаем расписание
                    j.setScheduled(false);
                    j.setMessage("Расписание деактивировано аварийным Kill-Switch");
                    jobRepository.save(j);
                    stoppedHdfs++;
                }
            }
        }

        int stoppedHms = 0;
        List<HmsReplicationJobEntity> hmsJobs = hmsReplicationJobRepository.findAll();
        for (HmsReplicationJobEntity h : hmsJobs) {
            if (matchesCluster(h.getSourceClusterId(), clusterId)) {
                if (!"PAUSED".equalsIgnoreCase(h.getStatus())) {
                    h.setStatus("PAUSED");
                    h.setMessage("Приостановлено аварийным Kill-Switch оператором " + operator + note);
                    hmsReplicationJobRepository.save(h);
                    stoppedHms++;
                }
            }
        }

        // Сетевое ограждение (Fencing): блокировка лимита канала шейпера
        if (Boolean.TRUE.equals(req.fenceNetwork())) {
            for (var dc : topologyRegistry.getDatacenters()) {
                if (!matchesDc(dc.getId(), clusterId)) {
                    topologyRegistry.setDcLimit(clusterId, dc.getId(), 0L);
                    log.warn("[Disaster Recovery Fencing] Канал полосы {} ➔ {} перекрыт (0 МБ/с)", clusterId, dc.getId());
                }
            }
        }

        String msg = String.format("Аварийный останов выполнен: остановлено %d HDFS задач и %d HMS задач для кластера '%s'",
                stoppedHdfs, stoppedHms, clusterId);
        return DrActionResponse.stopSuccess(msg, stoppedHdfs, stoppedHms);
    }

    /**
     * Создание и запуск обратных правил репликации (Reverse Replication) из target в source.
     */
    @Transactional
    public DrActionResponse reverseReplication(DrReverseRequest req, String username) {
        String fromClusterId = req.fromClusterId();
        String toClusterId = req.toClusterId();
        String operator = username != null ? username : "dr_operator";

        log.info("[Disaster Recovery] Запуск инверсии репликации (Reverse Replication): {} ➔ {} оператором '{}'",
                fromClusterId, toClusterId, operator);

        int reversedHdfs = 0;
        int reversedHms = 0;
        List<String> createdIds = new ArrayList<>();
        Instant now = Instant.now();

        // 1. Инверсия задач HDFS
        if (Boolean.TRUE.equals(req.includeHdfs())) {
            List<JobEntity> existingJobs = jobRepository.findAll();
            // Ищем задачи, которые изначально шли toClusterId ➔ fromClusterId (прямые)
            List<JobEntity> directJobs = existingJobs.stream()
                    .filter(j -> j.getParentJobId() == null
                            && matchesCluster(j.getSourceClusterId(), toClusterId)
                            && matchesCluster(j.getTargetClusterId(), fromClusterId))
                    .toList();

            for (JobEntity direct : directJobs) {
                // Проверяем, существует ли уже обратная задача
                boolean alreadyExists = existingJobs.stream().anyMatch(ej ->
                        matchesCluster(ej.getSourceClusterId(), fromClusterId)
                                && matchesCluster(ej.getTargetClusterId(), toClusterId)
                                && isSameOrFlippedPath(ej.getSourcePath(), direct.getTargetPath())
                );

                if (!alreadyExists) {
                    JobEntity revJob = new JobEntity();
                    revJob.setId("rev-" + UUID.randomUUID().toString().substring(0, 8));
                    revJob.setSourcePath(direct.getTargetPath());
                    revJob.setTargetPath(direct.getSourcePath());
                    revJob.setSourceClusterId(fromClusterId);
                    revJob.setTargetClusterId(toClusterId);
                    revJob.setTotalBytes(direct.getTotalBytes());
                    revJob.setCopiedBytes(0L);
                    revJob.setStatus(Boolean.TRUE.equals(req.autoStart()) ? "QUEUED" : "SCHEDULED");
                    revJob.setScheduled(direct.isScheduled());
                    revJob.setCronExpression(direct.getCronExpression());
                    revJob.setExecutionPrincipal(direct.getExecutionPrincipal());
                    revJob.setRunAsServiceAccount(direct.isRunAsServiceAccount());
                    revJob.setCreatedBy(operator);
                    revJob.setMessage("Обратная репликация (Reverse Failback) из " + fromClusterId + " в " + toClusterId);
                    revJob.setCreatedAt(now);
                    revJob.setUpdatedAt(now);
                    jobRepository.save(revJob);

                    // Создаем запись начального прогона
                    JobRunEntity run = new JobRunEntity();
                    run.setId(UUID.randomUUID().toString());
                    run.setJobId(revJob.getId());
                    run.setRunNumber(1);
                    run.setTriggerType("MANUAL");
                    run.setStatus(revJob.getStatus());
                    run.setTotalBytes(revJob.getTotalBytes());
                    run.setCopiedBytes(0L);
                    run.setStartedAt(now);
                    run.setMessage("Запуск обратной репликации дельты");
                    run.setTriggeredBy(operator);
                    run.setCreatedAt(now);
                    jobRunRepository.save(run);
                    revJob.setActiveRunId(run.getId());
                    jobRepository.save(revJob);

                    createdIds.add(revJob.getId());
                    reversedHdfs++;
                }
            }
        }

        // 2. Инверсия задач HMS
        if (Boolean.TRUE.equals(req.includeHms())) {
            List<HmsReplicationJobEntity> existingHms = hmsReplicationJobRepository.findAll();
            List<HmsReplicationJobEntity> directHms = existingHms.stream()
                    .filter(h -> matchesCluster(h.getSourceClusterId(), toClusterId)
                            && matchesCluster(h.getTargetClusterId(), fromClusterId))
                    .toList();

            for (HmsReplicationJobEntity direct : directHms) {
                boolean alreadyExists = existingHms.stream().anyMatch(eh ->
                        matchesCluster(eh.getSourceClusterId(), fromClusterId)
                                && matchesCluster(eh.getTargetClusterId(), toClusterId)
                                && Objects.equals(eh.getSourceDbName(), direct.getTargetDbName())
                );

                if (!alreadyExists) {
                    HmsReplicationJobEntity createdHms = hmsCoordinatorService.createAndStartReplication(
                            fromClusterId,
                            toClusterId,
                            direct.getTargetDbName(),
                            direct.getSourceDbName(),
                            direct.getTableIncludePattern(),
                            operator,
                            direct.isDropExtraneousTables(),
                            direct.isDropExtraneousPartitions(),
                            direct.getExecutionPrincipal()
                    );
                    createdIds.add("hms-" + createdHms.getId());
                    reversedHms++;
                }
            }
        }

        // Снятие сетевого ограждения (Fencing): возвращаем дефолтный лимит 100 МБ/с для нового направления
        topologyRegistry.setDcLimit(fromClusterId, toClusterId, 100L * 1024 * 1024);

        String msg = String.format("Обратная репликация настроена (%s ➔ %s): создано %d HDFS задач и %d HMS задач",
                fromClusterId, toClusterId, reversedHdfs, reversedHms);
        return DrActionResponse.reverseSuccess(msg, reversedHdfs, reversedHms, createdIds);
    }

    /**
     * Разворот одной точечной HDFS задачи.
     */
    @Transactional
    public DrActionResponse reverseSingleHdfsJob(String jobId, String username) {
        JobEntity direct = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Задача с ID " + jobId + " не найдена"));

        String fromCluster = direct.getTargetClusterId();
        String toCluster = direct.getSourceClusterId();
        String operator = username != null ? username : "dr_operator";
        Instant now = Instant.now();

        JobEntity revJob = new JobEntity();
        revJob.setId("rev-" + UUID.randomUUID().toString().substring(0, 8));
        revJob.setSourcePath(direct.getTargetPath());
        revJob.setTargetPath(direct.getSourcePath());
        revJob.setSourceClusterId(fromCluster);
        revJob.setTargetClusterId(toCluster);
        revJob.setTotalBytes(direct.getTotalBytes());
        revJob.setCopiedBytes(0L);
        revJob.setStatus("QUEUED");
        revJob.setScheduled(direct.isScheduled());
        revJob.setCronExpression(direct.getCronExpression());
        revJob.setExecutionPrincipal(direct.getExecutionPrincipal());
        revJob.setRunAsServiceAccount(direct.isRunAsServiceAccount());
        revJob.setCreatedBy(operator);
        revJob.setMessage("Обратная репликация для задачи " + direct.getId());
        revJob.setCreatedAt(now);
        revJob.setUpdatedAt(now);
        jobRepository.save(revJob);

        JobRunEntity run = new JobRunEntity();
        run.setId(UUID.randomUUID().toString());
        run.setJobId(revJob.getId());
        run.setRunNumber(1);
        run.setTriggerType("MANUAL");
        run.setStatus("QUEUED");
        run.setTotalBytes(revJob.getTotalBytes());
        run.setCopiedBytes(0L);
        run.setStartedAt(now);
        run.setMessage("Запуск обратной репликации дельты");
        run.setTriggeredBy(operator);
        run.setCreatedAt(now);
        jobRunRepository.save(run);
        revJob.setActiveRunId(run.getId());
        jobRepository.save(revJob);

        return DrActionResponse.reverseSuccess("Обратная задача создана: " + revJob.getId(), 1, 0, List.of(revJob.getId()));
    }

    private static boolean matchesCluster(String c1, String c2) {
        if (c1 == null || c2 == null) return true;
        if (c1.equalsIgnoreCase(c2)) return true;
        String c1n = c1.toLowerCase().replace("-", "").replace("_", "");
        String c2n = c2.toLowerCase().replace("-", "").replace("_", "");
        return c1n.equals(c2n) || c1n.contains(c2n) || c2n.contains(c1n);
    }

    private static boolean matchesDc(String clusterOrDc, String dc) {
        if (clusterOrDc == null || dc == null) return false;
        String cNorm = clusterOrDc.toLowerCase();
        String dNorm = dc.toLowerCase();
        return cNorm.contains(dNorm) || dNorm.contains(cNorm);
    }

    private static boolean isSameOrFlippedPath(String p1, String p2) {
        if (p1 == null || p2 == null) return false;
        return p1.equalsIgnoreCase(p2) || p1.endsWith(p2) || p2.endsWith(p1);
    }
}
