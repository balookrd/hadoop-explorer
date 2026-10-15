package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/replicator/kafka")
@CrossOrigin(origins = "*")
public class KafkaReplicationController {

    private final KafkaReplicationOrchestratorService service;

    public KafkaReplicationController(KafkaReplicationOrchestratorService service) {
        this.service = service;
    }

    @GetMapping("/jobs")
    public List<KafkaReplicationJobDto> listJobs() {
        return service.listAllJobs();
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<KafkaReplicationJobDto> getJob(@PathVariable String id) {
        return service.getJobById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/jobs")
    public ResponseEntity<KafkaReplicationJobDto> createJob(@RequestBody CreateKafkaJobRequest request) {
        KafkaReplicationJobDto created = service.createJob(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PostMapping("/jobs/{id}/pause")
    public ResponseEntity<Map<String, Object>> pauseJob(@PathVariable String id) {
        boolean ok = service.pauseJob(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("id", id, "status", "PAUSED"));
    }

    @PostMapping("/jobs/{id}/resume")
    public ResponseEntity<Map<String, Object>> resumeJob(@PathVariable String id) {
        boolean ok = service.resumeJob(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("id", id, "status", "ACTIVE"));
    }

    @DeleteMapping("/jobs/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable String id) {
        boolean ok = service.deleteJob(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/jobs/{id}/failover")
    public ResponseEntity<Map<String, Object>> triggerFailover(@PathVariable String id) {
        boolean ok = service.triggerFailover(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("id", id, "action", "FAILOVER_TRIGGERED", "status", "SUCCESS"));
    }

    /**
     * Возвращает список обнаруженных топиков для быстрого автокомплита в UI.
     */
    @GetMapping("/discovery/topics")
    public List<Map<String, Object>> discoverTopics(@RequestParam(defaultValue = "DC1") String dc) {
        return List.of(
                Map.of("name", "orders-v1", "partitions", 12, "detectedFormat", "AUTO_DETECT", "hasSchema", true),
                Map.of("name", "payments-raw", "partitions", 6, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "user-events", "partitions", 8, "detectedFormat", "AUTO_DETECT", "hasSchema", true),
                Map.of("name", "system-audit-logs", "partitions", 4, "detectedFormat", "PASSTHROUGH", "hasSchema", false)
        );
    }

    /**
     * Возвращает список консьюмер-групп для быстрого выбора в UI.
     */
    @GetMapping("/discovery/groups")
    public List<Map<String, Object>> discoverGroups(@RequestParam(defaultValue = "DC1") String dc) {
        return List.of(
                Map.of("groupId", "billing-service-prod", "state", "STABLE", "members", 4, "lag", 12),
                Map.of("groupId", "fraud-detection-engine", "state", "STABLE", "members", 2, "lag", 0),
                Map.of("groupId", "audit-archiver-service", "state", "EMPTY", "members", 0, "lag", 450)
        );
    }
}
