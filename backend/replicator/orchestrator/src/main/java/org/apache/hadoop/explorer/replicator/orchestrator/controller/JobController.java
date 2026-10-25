package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.model.UpdateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobRunEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping({"/api/v1/jobs", "/jobs"})
public class JobController {

    /** Размер страницы списка заданий по умолчанию и жесткий максимум. */
    private static final int DEFAULT_JOBS_PAGE_SIZE = 100;
    private static final int MAX_JOBS_PAGE_SIZE = 1000;

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    private UserSession getSession(Authentication auth) {
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            return tokenAuth.getUserSession();
        }
        return null;
    }

    private void checkJobAccess(String jobId, Authentication auth) {
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.UNAUTHORIZED, "Требуется аутентификация"
            );
        }
        if (session.isAdmin() || (session.groups() != null && session.groups().contains("replicator-agents"))) {
            return; // Администраторы и доверенные демоны-агенты могут управлять задачами
        }
        if (session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Пользователь с ролью только для чтения (READER) не имеет права изменять задачи"
            );
        }
        // WRITER может управлять своими задачами или задачами, владельцем которых он является
        var opt = jobService.getJobEntity(jobId);
        if (opt.isPresent()) {
            String createdBy = opt.get().getCreatedBy();
            if (createdBy != null && (createdBy.equalsIgnoreCase(session.username())
                    || JobService.matchesJobOwner(opt.get(), session.username()))) {
                return;
            }
        }
        throw new org.springframework.web.server.ResponseStatusException(
            HttpStatus.FORBIDDEN, "Недостаточно прав для управления задачей другого пользователя"
        );
    }

    @PostMapping
    public ResponseEntity<JobResponse> createJob(@Valid @RequestBody CreateJobRequest req, Authentication auth) {
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.UNAUTHORIZED, "Требуется аутентификация для создания задачи репликации"
            );
        }
        if (session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Пользователь с правами только для чтения (READER) не имеет права создавать задачи"
            );
        }

        String username = session.username();
        String executionPrincipal;

        if (session.isAdmin()) {
            if (req.executionPrincipal() != null && !req.executionPrincipal().isBlank()) {
                executionPrincipal = req.executionPrincipal().trim();
            } else {
                executionPrincipal = session.username();
            }
        } else {
            executionPrincipal = session.username();
        }
        if (executionPrincipal != null && !executionPrincipal.contains("@")) {
            executionPrincipal = executionPrincipal + "@REALM.LOCAL";
        }
        req = new CreateJobRequest(
            req.sourcePath(),
            req.targetPath(),
            req.sourceClusterId(),
            req.targetClusterId(),
            req.totalBytes(),
            executionPrincipal,
            session.isAdmin() && req.runAsServiceAccount() != null ? req.runAsServiceAccount() : false,
            req.isScheduled(),
            req.cronExpression(),
            req.historyRetentionRuns(),
            req.jobType(),
            req.parentJobId()
        );

        JobResponse created = jobService.createJob(req, username);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping
    public ResponseEntity<org.springframework.data.domain.Page<JobResponse>> listJobs(
        @RequestParam(required = false) String status,
        @RequestParam(required = false) String author,
        @RequestParam(required = false) String search,
        @RequestParam(required = false, name = "include_subjobs", defaultValue = "false") boolean includeSubjobs,
        @RequestParam(required = false) Integer page,
        @RequestParam(required = false) Integer size,
        @RequestHeader(required = false, name = "X-Agent-Secret") String agentSecret,
        Authentication auth
    ) {
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : null;
        boolean isAdmin = session != null && session.isAdmin();
        boolean isReader = session != null && session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER;
        boolean effectiveIncludeSubjobs = includeSubjobs || (agentSecret != null && !agentSecret.isBlank());

        // Обязательная пагинация: с подзадачами метастора список неограничен, и выборка целиком
        // упиралась в heap оркестратора. Порядок задан явно, иначе страницы нестабильны.
        int pageNumber = (page != null && page >= 0) ? page : 0;
        int pageSize = (size != null && size > 0) ? Math.min(size, MAX_JOBS_PAGE_SIZE) : DEFAULT_JOBS_PAGE_SIZE;
        var pageable = org.springframework.data.domain.PageRequest.of(pageNumber, pageSize,
                org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "createdAt"));

        return ResponseEntity.ok(jobService.listJobs(status, author, search, username, isAdmin, isReader,
                effectiveIncludeSubjobs, pageable));
    }

    /**
     * Сводка по списку заданий: счетчики статусов и авторы для фильтров интерфейса.
     *
     * <p>Нужна вместе со страничной выборкой: считать это в интерфейсе по загруженной странице
     * значит показывать счет по сотне записей вместо счета по заданиям.
     */
    @GetMapping("/summary")
    public ResponseEntity<org.apache.hadoop.explorer.replicator.orchestrator.dto.JobListingSummary> jobsSummary(
        @RequestParam(required = false) String author,
        @RequestParam(required = false) String search,
        @RequestParam(required = false, name = "include_subjobs", defaultValue = "false") boolean includeSubjobs,
        @RequestHeader(required = false, name = "X-Agent-Secret") String agentSecret,
        Authentication auth
    ) {
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : null;
        boolean isAdmin = session != null && session.isAdmin();
        boolean isReader = session != null && session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER;
        boolean effectiveIncludeSubjobs = includeSubjobs || (agentSecret != null && !agentSecret.isBlank());

        return ResponseEntity.ok(jobService.summarizeJobs(author, search, username, isAdmin, isReader,
                effectiveIncludeSubjobs));
    }

    private void checkJobReadAccess(String jobId, Authentication auth, String agentSecret) {
        if (agentSecret != null && !agentSecret.isBlank()) {
            return;
        }
        UserSession session = getSession(auth);
        if (session == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.UNAUTHORIZED, "Требуется аутентификация для просмотра задачи"
            );
        }
        if (session.isAdmin() || session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER
                || (session.groups() != null && session.groups().contains("replicator-agents"))) {
            return;
        }
        var opt = jobService.getJobEntity(jobId);
        if (opt.isPresent()) {
            String createdBy = opt.get().getCreatedBy();
            if (createdBy != null && (createdBy.equalsIgnoreCase(session.username())
                    || JobService.matchesJobOwner(opt.get(), session.username()))) {
                return;
            }
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Недостаточно прав для просмотра задачи другого пользователя"
            );
        }
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobResponse> getJob(
        @PathVariable String jobId,
        @RequestHeader(required = false, name = "X-Agent-Secret") String agentSecret,
        Authentication auth
    ) {
        checkJobReadAccess(jobId, auth, agentSecret);
        return jobService.getJob(jobId)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/{jobId}")
    public ResponseEntity<JobResponse> updateJob(
        @PathVariable String jobId,
        @Valid @RequestBody CreateJobRequest req,
        Authentication auth
    ) {
        checkJobAccess(jobId, auth);
        UserSession session = getSession(auth);
        if (session != null && !session.isAdmin()) {
            var existing = jobService.getJobEntity(jobId);
            String existingPrincipal = existing.map(org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity::getExecutionPrincipal).orElse(session.username() + "@REALM.LOCAL");
            boolean safeRunAsSa = existing.map(org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity::isRunAsServiceAccount).orElse(false);
            req = new CreateJobRequest(
                req.sourcePath(),
                req.targetPath(),
                req.sourceClusterId(),
                req.targetClusterId(),
                req.totalBytes(),
                existingPrincipal,
                safeRunAsSa,
                req.isScheduled(),
                req.cronExpression(),
                req.historyRetentionRuns(),
                req.jobType(),
                req.parentJobId()
            );
        } else if (req.executionPrincipal() != null && !req.executionPrincipal().isBlank()) {
            String ep = req.executionPrincipal().trim();
            if (!ep.contains("@")) {
                ep = ep + "@REALM.LOCAL";
            }
            boolean runAsSa = session != null && session.isAdmin() && req.runAsServiceAccount() != null ? req.runAsServiceAccount() : false;
            req = new CreateJobRequest(
                req.sourcePath(),
                req.targetPath(),
                req.sourceClusterId(),
                req.targetClusterId(),
                req.totalBytes(),
                ep,
                runAsSa,
                req.isScheduled(),
                req.cronExpression(),
                req.historyRetentionRuns(),
                req.jobType(),
                req.parentJobId()
            );
        }
        return jobService.updateJob(jobId, req)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PatchMapping("/{jobId}")
    public ResponseEntity<JobResponse> updateJobProgress(
        @PathVariable String jobId,
        @RequestBody UpdateJobRequest req,
        Authentication auth
    ) {
        checkJobAccess(jobId, auth);
        return jobService.updateJobProgress(jobId, req)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{jobId}/start")
    public ResponseEntity<JobResponse> startJob(@PathVariable String jobId, Authentication auth) {
        checkJobAccess(jobId, auth);
        return jobService.startJob(jobId)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{jobId}/stop")
    public ResponseEntity<JobResponse> stopJob(@PathVariable String jobId, Authentication auth) {
        checkJobAccess(jobId, auth);
        return jobService.stopJob(jobId)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{jobId}/cancel")
    public ResponseEntity<JobResponse> cancelJob(@PathVariable String jobId, Authentication auth) {
        checkJobAccess(jobId, auth);
        return jobService.cancelJob(jobId)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{jobId}")
    public ResponseEntity<Map<String, Object>> deleteJob(@PathVariable String jobId, Authentication auth) {
        checkJobAccess(jobId, auth);
        boolean deleted = jobService.deleteJob(jobId);
        if (deleted) {
            return ResponseEntity.ok(Map.of("success", true, "message", "Задача удалена"));
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/{jobId}/runs")
    public ResponseEntity<List<JobRunEntity>> getJobRuns(
        @PathVariable String jobId,
        @RequestHeader(required = false, name = "X-Agent-Secret") String agentSecret,
        Authentication auth
    ) {
        checkJobReadAccess(jobId, auth, agentSecret);
        return ResponseEntity.ok(jobService.getJobRuns(jobId));
    }
}
