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

    private String getUsername(Authentication auth) {
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            UserSession session = tokenAuth.getUserSession();
            if (session != null) return session.username();
        }
        return "system_operator";
    }

    public record CreateHmsJobRequest(
            String source_cluster_id,
            String target_cluster_id,
            String source_db,
            String target_db,
            String table_pattern,
            Boolean drop_extraneous_tables,
            Boolean drop_extraneous_partitions
    ) {}

    @PostMapping
    public ResponseEntity<HmsReplicationJobEntity> createJob(
            @RequestBody CreateHmsJobRequest req,
            Authentication auth
    ) {
        boolean dropTables = req.drop_extraneous_tables() != null && req.drop_extraneous_tables();
        boolean dropParts = req.drop_extraneous_partitions() != null && req.drop_extraneous_partitions();
        HmsReplicationJobEntity created = coordinatorService.createAndStartReplication(
                req.source_cluster_id(),
                req.target_cluster_id(),
                req.source_db(),
                req.target_db(),
                req.table_pattern(),
                getUsername(auth),
                dropTables,
                dropParts
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping
    public ResponseEntity<List<HmsReplicationJobEntity>> listJobs() {
        return ResponseEntity.ok(jobRepository.findAll());
    }

    @GetMapping("/pending")
    public ResponseEntity<List<org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto>> getPendingJobs(
            @RequestParam(required = false) String clusterId,
            @RequestParam(required = false) String agentId
    ) {
        return ResponseEntity.ok(coordinatorService.getPendingJobsForCluster(clusterId, agentId));
    }

    @PostMapping("/{id}/progress")
    public ResponseEntity<Map<String, Object>> updateProgress(
            @PathVariable String id,
            @RequestBody org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest report
    ) {
        coordinatorService.updateJobProgress(id, report);
        return ResponseEntity.ok(Map.of("success", true));
    }

    @GetMapping("/{id}")
    public ResponseEntity<HmsReplicationJobEntity> getJob(@PathVariable String id) {
        return jobRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/sync")
    public ResponseEntity<Map<String, Object>> triggerSync(@PathVariable String id) {
        return ResponseEntity.ok(Map.of("success", true, "message", "Потоковая репликация CDC выполняется агентом"));
    }

    @GetMapping("/{id}/events")
    public ResponseEntity<List<HmsEventLogEntity>> getEvents(
            @PathVariable String id,
            @RequestParam(defaultValue = "100") int limit
    ) {
        return ResponseEntity.ok(
                eventLogRepository.findByHmsJobIdOrderByCreatedAtDesc(id, PageRequest.of(0, Math.min(limit, 500)))
        );
    }

    @GetMapping("/{id}/subtasks")
    public ResponseEntity<List<JobResponse>> getSubtasks(@PathVariable String id) {
        return ResponseEntity.ok(jobService.listSubjobsByParentId(id));
    }

    @PostMapping("/{id}/pause")
    public ResponseEntity<HmsReplicationJobEntity> pauseJob(@PathVariable String id) {
        return jobRepository.findById(id)
                .map(job -> {
                    job.setStatus("PAUSED");
                    jobRepository.save(job);
                    return ResponseEntity.ok(job);
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/resume")
    public ResponseEntity<HmsReplicationJobEntity> resumeJob(@PathVariable String id) {
        return jobRepository.findById(id)
                .map(job -> {
                    job.setStatus("ACTIVE");
                    jobRepository.save(job);
                    return ResponseEntity.ok(job);
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/rebootstrap")
    public ResponseEntity<HmsReplicationJobEntity> rebootstrap(@PathVariable String id) {
        coordinatorService.rebootstrap(id);
        return jobRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteJob(@PathVariable String id) {
        boolean deleted = coordinatorService.deleteReplicationJob(id);
        if (deleted) {
            return ResponseEntity.ok(Map.of("success", true, "message", "Задача репликации HMS успешно удалена"));
        }
        return ResponseEntity.notFound().build();
    }
}
