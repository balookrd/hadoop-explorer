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
            return; // Системный вызов / фоновый агент
        }
        if (session.isAdmin()) {
            return; // Администратор может управлять любыми задачами
        }
        if (session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Пользователь с ролью только для чтения (READER) не имеет права изменять задачи"
            );
        }
        // WRITER может управлять своими задачами, задачами группы инженеров или системными
        var opt = jobService.getJobEntity(jobId);
        if (opt.isPresent()) {
            String createdBy = opt.get().getCreatedBy();
            if (createdBy != null && (createdBy.equalsIgnoreCase(session.username())
                    || "system_operator".equalsIgnoreCase(createdBy)
                    || "demo-admin".equalsIgnoreCase(createdBy)
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
        if (session != null && !session.isAdmin() && session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER) {
            throw new org.springframework.web.server.ResponseStatusException(
                HttpStatus.FORBIDDEN, "Пользователь с правами только для чтения (READER) не имеет права создавать задачи"
            );
        }

        String username = session != null ? session.username() : "writer_user";
        String executionPrincipal = req.executionPrincipal();

        if (session != null) {
            if (session.isAdmin()) {
                if (executionPrincipal != null && !executionPrincipal.isBlank()) {
                    executionPrincipal = executionPrincipal.trim();
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
                req.runAsServiceAccount() != null ? req.runAsServiceAccount() : false,
                req.isScheduled(),
                req.cronExpression(),
                req.historyRetentionRuns(),
                req.jobType(),
                req.parentJobId()
            );
        } else if (req.executionPrincipal() != null && !req.executionPrincipal().isBlank()) {
            String p = req.executionPrincipal().trim();
            int at = p.indexOf('@');
            username = (at > 0) ? p.substring(0, at) : p;
        }

        JobResponse created = jobService.createJob(req, username);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping
    public ResponseEntity<List<JobResponse>> listJobs(
        @RequestParam(required = false) String status,
        @RequestParam(required = false, name = "include_subjobs", defaultValue = "false") boolean includeSubjobs,
        @RequestHeader(required = false, name = "X-Agent-Secret") String agentSecret,
        Authentication auth
    ) {
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : null;
        boolean isAdmin = session == null || session.isAdmin();
        boolean isReader = session != null && session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER;
        boolean effectiveIncludeSubjobs = includeSubjobs || (agentSecret != null && !agentSecret.isBlank());
        return ResponseEntity.ok(jobService.listJobs(status, username, isAdmin, isReader, effectiveIncludeSubjobs));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobResponse> getJob(@PathVariable String jobId) {
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
            req = new CreateJobRequest(
                req.sourcePath(),
                req.targetPath(),
                req.sourceClusterId(),
                req.targetClusterId(),
                req.totalBytes(),
                existingPrincipal,
                req.runAsServiceAccount(),
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
            req = new CreateJobRequest(
                req.sourcePath(),
                req.targetPath(),
                req.sourceClusterId(),
                req.targetClusterId(),
                req.totalBytes(),
                ep,
                req.runAsServiceAccount(),
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
        @RequestBody UpdateJobRequest req
    ) {
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
    public ResponseEntity<List<JobRunEntity>> getJobRuns(@PathVariable String jobId) {
        return ResponseEntity.ok(jobService.getJobRuns(jobId));
    }
}
