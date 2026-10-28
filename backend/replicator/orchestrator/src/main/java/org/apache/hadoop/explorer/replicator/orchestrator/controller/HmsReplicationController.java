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

    /** Статусы заданий метастора: по ним считаются чипы интерфейса. */
    private static final List<String> HMS_JOB_STATUSES =
            List.of("QUEUED", "WAITING_FOR_AGENTS", "BOOTSTRAPPING", "ACTIVE", "PAUSED", "ERROR",
                    "FAILED", "STOPPED");

    private static final int DEFAULT_JOBS_PAGE_SIZE = 100;
    private static final int MAX_JOBS_PAGE_SIZE = 1000;

    private final HmsReplicationJobRepository jobRepository;
    /** Суммы по отобранным заданиям: считает база, не контроллер. */
    private final org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsJobListingAggregates
            listingAggregates;
    private final HmsEventLogRepository eventLogRepository;
    private final HmsCoordinatorService coordinatorService;
    private final JobService jobService;

    public HmsReplicationController(
            HmsReplicationJobRepository jobRepository,
            HmsEventLogRepository eventLogRepository,
            HmsCoordinatorService coordinatorService,
            JobService jobService
    ) {
        this(jobRepository, eventLogRepository, coordinatorService, jobService, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public HmsReplicationController(
            HmsReplicationJobRepository jobRepository,
            HmsEventLogRepository eventLogRepository,
            HmsCoordinatorService coordinatorService,
            JobService jobService,
            org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsJobListingAggregates listingAggregates
    ) {
        this.jobRepository = jobRepository;
        this.eventLogRepository = eventLogRepository;
        this.coordinatorService = coordinatorService;
        this.jobService = jobService;
        this.listingAggregates = listingAggregates;
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

    /**
     * Список заданий метастора страницами.
     *
     * <p>Прежде отдавался {@code findAll()}: все задания разом, а чужие вычеркивались уже в
     * памяти. Порядок задан явно — без него страницы нестабильны и запись может попасть на две
     * сразу или не попасть никуда.
     */
    @GetMapping
    public ResponseEntity<org.springframework.data.domain.Page<HmsReplicationJobEntity>> listJobs(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String author,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            Authentication auth) {
        UserSession session = requireSession(auth);

        int pageNumber = (page != null && page >= 0) ? page : 0;
        int pageSize = (size != null && size > 0) ? Math.min(size, MAX_JOBS_PAGE_SIZE) : DEFAULT_JOBS_PAGE_SIZE;
        var pageable = PageRequest.of(pageNumber, pageSize,
                org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "createdAt"));

        var spec = org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsJobListingSpecifications
                .forListing(normalizeUpper(status), normalizeLower(search), normalizeLower(author),
                        ownerFilter(session));
        var jobs = jobRepository.findAll(spec, pageable);
        coordinatorService.enrichAllWithSubtasksMetrics(jobs.getContent());
        return ResponseEntity.ok(jobs);
    }

    /**
     * Кого пользователю видно: {@code null} — все, иначе только свои задания.
     *
     * <p>Список раскрывает имена баз, таблиц и кластеров источника, поэтому условие уходит в
     * запрос, а не отсекает записи после выборки.
     */
    private String ownerFilter(UserSession session) {
        boolean seesEverything = session.isAdmin() || AGENT_AUTH_METHOD.equalsIgnoreCase(session.authMethod());
        return seesEverything ? null : normalizeLower(session.username());
    }

    private UserSession requireSession(Authentication auth) {
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "Требуется аутентификация"
            );
        }
        return session;
    }

    private static String normalizeLower(String value) {
        return value == null || value.isBlank() ? null : value.trim().toLowerCase();
    }

    private static String normalizeUpper(String value) {
        return value == null || value.isBlank() || "ALL".equalsIgnoreCase(value.trim())
                ? null : value.trim().toUpperCase();
    }

    /**
     * Сводка по всему набору заданий: счетчики статусов и суммы перенесенного.
     *
     * <p>Отдельно от страницы, потому что считать это по странице — показывать счет по сотне
     * записей вместо счета по всем заданиям пользователя.
     */
    @GetMapping("/summary")
    public ResponseEntity<org.apache.hadoop.explorer.replicator.orchestrator.dto.HmsJobListingSummary>
            jobsSummary(
                    @RequestParam(required = false) String search,
                    @RequestParam(required = false) String author,
                    Authentication auth) {
        UserSession session = requireSession(auth);
        String owner = ownerFilter(session);

        // Условие то же, что у страницы, но без статуса: счетчики как раз и показывают,
        // сколько заданий в каждом статусе при текущем поиске
        var base = org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsJobListingSpecifications
                .forListing(null, normalizeLower(search), normalizeLower(author), owner);

        java.util.Map<String, Long> byStatus = new java.util.LinkedHashMap<>();
        for (String status : HMS_JOB_STATUSES) {
            byStatus.put(status, jobRepository.count(base.and(
                    org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsJobListingSpecifications
                            .withStatus(status))));
        }

        var totals = listingAggregates != null
                ? listingAggregates.sumReplicated(base)
                : new org.apache.hadoop.explorer.replicator.orchestrator.repository
                        .HmsJobListingAggregates.Totals(0L, 0L);

        return ResponseEntity.ok(new org.apache.hadoop.explorer.replicator.orchestrator.dto
                .HmsJobListingSummary(jobRepository.count(base), byStatus,
                totals.replicatedTables(), totals.replicatedPartitions(),
                jobRepository.listAuthors(owner)));
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

    /**
     * Фактическое состояние потоковой синхронизации CDC задачи.
     *
     * <p>Эндпоинт ничего не запускает: NOTIFICATION_LOG опрашивает сам агент своим циклом с
     * настроенным интервалом, а оркестратор командного канала к агенту не имеет. Прежний ответ
     * «success: true» утверждал обратное и выдавался без проверки доступа и существования
     * задачи — то есть на любой идентификатор. Теперь возвращается то, что оркестратор знает:
     * статус задачи, позиция в журнале событий, отставание и назначенный агент.
     */
    @PostMapping("/{id}/sync")
    public ResponseEntity<Map<String, Object>> triggerSync(@PathVariable String id, Authentication auth) {
        checkHmsJobAccess(id, auth);
        HmsReplicationJobEntity job = jobRepository.findById(id).orElseThrow(() ->
                new org.springframework.web.server.ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Задача репликации не найдена: " + id));

        Map<String, Object> state = new java.util.LinkedHashMap<>();
        state.put("triggered", false);
        state.put("status", job.getStatus());
        state.put("last_processed_event_id", job.getLastProcessedEventId());
        state.put("event_lag", job.getEventLag());
        state.put("assigned_agent_id", job.getAssignedAgentId());
        state.put("message", "Опрос NOTIFICATION_LOG выполняет агент своим циклом; "
                + "здесь показано состояние, известное оркестратору");
        return ResponseEntity.ok(state);
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

    /**
     * Подзадачи HMS-задачи постранично: у таблицы в 200k партиций столько же подзадач.
     * Общее число — в заголовке {@code X-Total-Count}, тело остается списком.
     */
    @GetMapping("/{id}/subtasks")
    public ResponseEntity<List<JobResponse>> getSubtasks(@PathVariable String id,
                                                         @RequestParam(defaultValue = "0") int page,
                                                         @RequestParam(defaultValue = "200") int size,
                                                         Authentication auth) {
        checkHmsJobReadAccess(id, auth);
        var result = jobService.listSubjobsByParentId(id, PageRequest.of(Math.max(0, page),
                Math.min(Math.max(1, size), 1000), org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "createdAt")));
        return ResponseEntity.ok()
                .header("X-Total-Count", String.valueOf(result.getTotalElements()))
                .body(result.getContent());
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
                    if (!job.pause()) {
                        throw new org.springframework.web.server.ResponseStatusException(
                                org.springframework.http.HttpStatus.CONFLICT,
                                "Задачу в статусе " + job.getStatus() + " нельзя поставить на паузу");
                    }
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
                    if (!job.resume()) {
                        throw new org.springframework.web.server.ResponseStatusException(
                                org.springframework.http.HttpStatus.CONFLICT,
                                "Задача в статусе " + job.getStatus() + " не на паузе");
                    }
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
        // Отчет о дрейфе только читает журнал задачи: права на запись для него не нужны
        checkHmsJobReadAccess(id, auth);
        return ResponseEntity.ok(coordinatorService.scanSchemaDrift(id));
    }
}
