package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping({"/api/v1/hms/jobs", "/hms/jobs"})
public class HmsReplicationController {

    private final HmsReplicationJobRepository jobRepository;
    private final HmsEventLogRepository eventLogRepository;
    private final HmsCoordinatorService coordinatorService;
    private final JobService jobService;

    public HmsReplicationController(
            HmsReplicationJobRepository jobRepository,
            HmsEventLogRepository eventLogRepository,
            HmsCoordinatorService coordinatorService,
            JobService jobService
    ) {
        this.jobRepository = jobRepository;
        this.eventLogRepository = eventLogRepository;
        this.coordinatorService = coordinatorService;
        this.jobService = jobService;
    }

    private UserSession getSession(Authentication auth) {
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            return tokenAuth.getUserSession();
        }
        return null;
    }

    private String getUsername(Authentication auth) {
        UserSession session = getSession(auth);
        return session != null ? session.username() : "system";
    }

    /** Признак сессии, выставленной фильтром аутентификации агента. */
    private static final String AGENT_AUTH_METHOD = "agent-secret";

    /**
     * Проверка права на чтение деталей задачи (события CDC, подзадачи, DLQ).
     *
     * <p>Детали раскрывают структуру данных источника: имена баз и таблиц, пути HDFS,
     * идентификаторы агентов. Без проверки владельца любой аутентифицированный пользователь
     * мог перечислить их по идентификатору чужой задачи (BOLA/IDOR).
     */
    private void checkHmsJobReadAccess(String jobId, Authentication auth) {
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "Требуется аутентификация"
            );
        }
        if (session.isAdmin() || AGENT_AUTH_METHOD.equalsIgnoreCase(session.authMethod())) {
            return;
        }
        var opt = jobRepository.findById(jobId);
        if (opt.isEmpty()) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Задача репликации не найдена: " + jobId
            );
        }
        String createdBy = opt.get().getCreatedBy();
        if (createdBy == null || createdBy.equalsIgnoreCase(session.username())) {
            return;
        }
        throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Недостаточно прав для просмотра задачи репликации другого пользователя"
        );
    }

    public record CreateHmsJobRequest(
            String source_cluster_id,
            String target_cluster_id,
            String source_db,
            String target_db,
            String table_pattern,
            Boolean drop_extraneous_tables,
            Boolean drop_extraneous_partitions,
            String execution_principal,
            Boolean enable_hdfs_streaming
    ) {
        public CreateHmsJobRequest(
                String source_cluster_id,
                String target_cluster_id,
                String source_db,
                String target_db,
                String table_pattern,
                Boolean drop_extraneous_tables,
                Boolean drop_extraneous_partitions,
                String execution_principal
        ) {
            this(source_cluster_id, target_cluster_id, source_db, target_db, table_pattern,
                    drop_extraneous_tables, drop_extraneous_partitions, execution_principal, false);
        }
    }

    @PostMapping
    public ResponseEntity<HmsReplicationJobEntity> createJob(
            @RequestBody CreateHmsJobRequest req,
            Authentication auth
    ) {
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.UNAUTHORIZED, "Требуется аутентификация для создания задачи HMS репликации"
            );
        }
        if (session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Пользователь с правами только для чтения (READER) не имеет права создавать задачи"
            );
        }

        String username = session.username();
        String executionPrincipal;

        if (session.isAdmin() && req.execution_principal() != null && !req.execution_principal().isBlank()) {
            executionPrincipal = req.execution_principal().trim();
        } else {
            executionPrincipal = session.username();
        }

        if (executionPrincipal != null && !executionPrincipal.contains("@")) {
            executionPrincipal = executionPrincipal + "@REALM.LOCAL";
        }

        boolean dropTables = req.drop_extraneous_tables() != null && req.drop_extraneous_tables();
        boolean dropParts = req.drop_extraneous_partitions() != null && req.drop_extraneous_partitions();
        boolean enableStreaming = Boolean.TRUE.equals(req.enable_hdfs_streaming());
        HmsReplicationJobEntity created = coordinatorService.createAndStartReplication(
                req.source_cluster_id(),
                req.target_cluster_id(),
                req.source_db(),
                req.target_db(),
                req.table_pattern(),
                username,
                dropTables,
                dropParts,
                executionPrincipal,
                enableStreaming
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping
    public ResponseEntity<List<HmsReplicationJobEntity>> listJobs(Authentication auth) {
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "Требуется аутентификация"
            );
        }
        List<HmsReplicationJobEntity> jobs = jobRepository.findAll();
        // Непривилегированный пользователь видит только свои задачи: список раскрывает
        // имена баз, таблиц и кластеров источника.
        if (!session.isAdmin() && !AGENT_AUTH_METHOD.equalsIgnoreCase(session.authMethod())) {
            jobs = jobs.stream()
                    .filter(job -> job.getCreatedBy() == null
                            || job.getCreatedBy().equalsIgnoreCase(session.username()))
                    .toList();
        }
        coordinatorService.enrichAllWithSubtasksMetrics(jobs);
        return ResponseEntity.ok(jobs);
    }

    @GetMapping("/{id}")
    public ResponseEntity<HmsReplicationJobEntity> getJob(@PathVariable String id, Authentication auth) {
        checkHmsJobReadAccess(id, auth);
        return jobRepository.findById(id)
                .map(job -> {
                    coordinatorService.enrichWithSubtasksMetrics(job);
                    return ResponseEntity.ok(job);
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/sync")
    public ResponseEntity<Map<String, Object>> triggerSync(@PathVariable String id) {
        return ResponseEntity.ok(Map.of("success", true, "message", "Потоковая репликация CDC выполняется агентом"));
    }

    @GetMapping("/{id}/events")
    public ResponseEntity<List<HmsEventLogEntity>> getEvents(
            @PathVariable String id,
            @RequestParam(defaultValue = "100") int limit,
            Authentication auth
    ) {
        checkHmsJobReadAccess(id, auth);
        return ResponseEntity.ok(
                eventLogRepository.findByHmsJobIdOrderByCreatedAtDesc(id, PageRequest.of(0, Math.min(limit, 500)))
        );
    }

    @GetMapping("/{id}/subtasks")
    public ResponseEntity<List<JobResponse>> getSubtasks(@PathVariable String id, Authentication auth) {
        checkHmsJobReadAccess(id, auth);
        return ResponseEntity.ok(jobService.listSubjobsByParentId(id));
    }

    private void checkHmsJobAccess(String jobId, Authentication auth) {
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.UNAUTHORIZED, "Требуется аутентификация"
            );
        }
        var opt = jobRepository.findById(jobId);
        if (opt.isEmpty()) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.NOT_FOUND, "Задача репликации не найдена: " + jobId
            );
        }
        if (session.isAdmin()) {
            return; // Администратор может управлять любыми существующими задачами
        }
        if (session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Пользователь с ролью только для чтения (READER) не имеет права изменять задачи"
            );
        }
        String createdBy = opt.get().getCreatedBy();
        if (createdBy != null && createdBy.equalsIgnoreCase(session.username())) {
            return;
        }
        throw new org.springframework.web.server.ResponseStatusException(
            HttpStatus.FORBIDDEN, "Недостаточно прав для управления задачей репликации другого пользователя"
        );
    }

    @PostMapping("/{id}/pause")
    public ResponseEntity<HmsReplicationJobEntity> pauseJob(@PathVariable String id, Authentication auth) {
        checkHmsJobAccess(id, auth);
        return jobRepository.findById(id)
                .map(job -> {
                    job.transitionTo(org.apache.hadoop.explorer.replicator.model.HmsJobStatus.PAUSED);
                    jobRepository.save(job);
                    coordinatorService.pauseCompanionJob(job);
                    return ResponseEntity.ok(job);
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/resume")
    public ResponseEntity<HmsReplicationJobEntity> resumeJob(@PathVariable String id, Authentication auth) {
        checkHmsJobAccess(id, auth);
        return jobRepository.findById(id)
                .map(job -> {
                    job.transitionTo(org.apache.hadoop.explorer.replicator.model.HmsJobStatus.ACTIVE);
                    jobRepository.save(job);
                    coordinatorService.resumeCompanionJob(job);
                    return ResponseEntity.ok(job);
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/rebootstrap")
    public ResponseEntity<HmsReplicationJobEntity> rebootstrap(@PathVariable String id, Authentication auth) {
        checkHmsJobAccess(id, auth);
        coordinatorService.rebootstrap(id);
        return jobRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteJob(@PathVariable String id, Authentication auth) {
        checkHmsJobAccess(id, auth);
        boolean deleted = coordinatorService.deleteReplicationJob(id);
        if (deleted) {
            return ResponseEntity.ok(Map.of("success", true, "message", "Задача репликации HMS успешно удалена"));
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/{id}/dlq")
    public ResponseEntity<List<org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsDeadLetterEventEntity>> getDlqEvents(
            @PathVariable String id,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "100") int limit,
            Authentication auth
    ) {
        checkHmsJobReadAccess(id, auth);
        return ResponseEntity.ok(coordinatorService.getDeadLetterEvents(id, status, limit));
    }

    @PostMapping("/{id}/dlq/{dlqId}/skip")
    public ResponseEntity<Map<String, Object>> skipDlqEvent(
            @PathVariable String id,
            @PathVariable String dlqId,
            Authentication auth
    ) {
        checkHmsJobAccess(id, auth);
        boolean skipped = coordinatorService.skipDeadLetterEvent(id, dlqId);
        if (skipped) {
            return ResponseEntity.ok(Map.of("success", true, "message", "Событие DLQ помечено как IGNORED"));
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("/{id}/dlq/{dlqId}/replay")
    public ResponseEntity<Map<String, Object>> replayDlqEvent(
            @PathVariable String id,
            @PathVariable String dlqId,
            Authentication auth
    ) {
        checkHmsJobAccess(id, auth);
        boolean replayed = coordinatorService.replayDeadLetterEvent(id, dlqId);
        if (replayed) {
            return ResponseEntity.ok(Map.of("success", true, "message", "Событие DLQ отправлено на повтор (REPLAYED)"));
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/{id}/drift")
    public ResponseEntity<List<org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport>> getSchemaDrift(
            @PathVariable String id,
            Authentication auth
    ) {
        checkHmsJobAccess(id, auth);
        return ResponseEntity.ok(coordinatorService.scanSchemaDrift(id));
    }
}
