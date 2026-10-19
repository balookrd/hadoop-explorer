package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/replicator/kafka")
public class KafkaReplicationController {

    private final KafkaReplicationOrchestratorService service;
    private final org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry topologyRegistry;

    @org.springframework.beans.factory.annotation.Autowired
    public KafkaReplicationController(KafkaReplicationOrchestratorService service,
                                     @org.springframework.beans.factory.annotation.Autowired(required = false)
                                     org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry topologyRegistry) {
        this.service = service;
        this.topologyRegistry = topologyRegistry;
    }

    public KafkaReplicationController(KafkaReplicationOrchestratorService service) {
        this(service, null);
    }

    private UserSession getSession() {
        Authentication auth = org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            return tokenAuth.getUserSession();
        }
        return null;
    }

    private void requireAuthentication() {
        UserSession session = getSession();
        if (session == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Требуется аутентификация");
        }
    }

    private void requireWriterOrAdmin() {
        requireAuthentication();
        UserSession session = getSession();
        if (session.systemRole() == Role.READER) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Пользователь с ролью READER не имеет права модифицировать задачи Kafka");
        }
    }

    @GetMapping("/jobs")
    public List<KafkaReplicationJobDto> listJobs() {
        requireAuthentication();
        return service.listAllJobs();
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<KafkaReplicationJobDto> getJob(@PathVariable String id) {
        requireAuthentication();
        return service.getJobById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/jobs/active")
    public List<KafkaReplicationJobDto> getActiveJobs(@RequestParam String dcId) {
        requireAuthentication();
        return service.getActiveJobsForDc(dcId);
    }

    @PostMapping("/jobs")
    public ResponseEntity<KafkaReplicationJobDto> createJob(@jakarta.validation.Valid @RequestBody CreateKafkaJobRequest request) {
        requireWriterOrAdmin();
        KafkaReplicationJobDto created = service.createJob(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PostMapping("/jobs/{id}/pause")
    public ResponseEntity<Map<String, Object>> pauseJob(@PathVariable String id) {
        requireWriterOrAdmin();
        boolean ok = service.pauseJob(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("id", id, "status", "PAUSED"));
    }

    @PostMapping("/jobs/{id}/resume")
    public ResponseEntity<Map<String, Object>> resumeJob(@PathVariable String id) {
        requireWriterOrAdmin();
        boolean ok = service.resumeJob(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("id", id, "status", "ACTIVE"));
    }

    @DeleteMapping("/jobs/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable String id) {
        requireWriterOrAdmin();
        boolean ok = service.deleteJob(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/jobs/{id}/failover")
    public ResponseEntity<Map<String, Object>> triggerFailover(@PathVariable String id) {
        requireWriterOrAdmin();
        boolean ok = service.triggerFailover(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("id", id, "action", "FAILOVER_TRIGGERED", "status", "SUCCESS"));
    }

    @PostMapping("/jobs/{id}/provision-topics")
    public ResponseEntity<org.apache.hadoop.explorer.replicator.orchestrator.kafka.TopicProvisioningResult> provisionTopics(@PathVariable String id) {
        requireWriterOrAdmin();
        var result = service.provisionTopicsForJob(id);
        return ResponseEntity.ok(result);
    }

    /**
     * Возвращает список активных адресов агентов-приемников в целевом кластере/ДЦ для балансировки нагрузки.
     */
    @GetMapping("/agents/receivers")
    public List<String> getReceiverEndpoints(@RequestParam(defaultValue = "DC2") String clusterId) {
        requireAuthentication();
        return service.getLiveReceiverEndpoints(clusterId);
    }

    /**
     * Возвращает список сконфигурированных кластеров Kafka / ЦОД для выбора в UI.
     */
    @GetMapping("/discovery/clusters")
    public List<Map<String, Object>> discoverClusters() {
        requireAuthentication();
        UserSession session = getSession();
        boolean isAdmin = session != null && session.systemRole() == Role.ADMIN;

        if (topologyRegistry == null) {
            return java.util.Collections.emptyList();
        }

        List<Map<String, Object>> list = new java.util.ArrayList<>();
        for (var c : topologyRegistry.getClusters()) {
            var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
            if (cfg == null) continue;
            String keytabDisplay = isAdmin && cfg.getKeytabPath() != null
                    ? cfg.getKeytabPath()
                    : "[PROTECTED]";

            Map<String, Object> map = new java.util.LinkedHashMap<>();
            map.put("id", cfg.getId());
            map.put("name", cfg.getName() != null ? cfg.getName() : cfg.getId());
            map.put("dcId", cfg.getDcId() != null ? cfg.getDcId() : "");
            map.put("location", cfg.getDcId() != null ? cfg.getDcId() : "");
            map.put("bootstrapServers", cfg.getKafkaBootstrapServers() != null ? cfg.getKafkaBootstrapServers() : "");
            map.put("srUrl", cfg.getSchemaRegistryUrl() != null ? cfg.getSchemaRegistryUrl() : "");
            map.put("srType", cfg.getSchemaRegistryType() != null ? cfg.getSchemaRegistryType() : "CONFLUENT");
            map.put("kerberosEnabled", cfg.isKerberosEnabled());
            map.put("principal", cfg.getPrincipal() != null ? cfg.getPrincipal() : "");
            map.put("keytabPath", keytabDisplay);
            list.add(map);
        }
        return list;
    }

    /**
     * Возвращает список обнаруженных топиков для быстрого автокомплита в UI.
     */
    @GetMapping("/discovery/topics")
    public List<Map<String, Object>> discoverTopics(
            @RequestParam(required = false) String dc,
            @RequestParam(required = false) String clusterId,
            @RequestParam(required = false) String bootstrapServers,
            @RequestParam(required = false) String search) {
        requireAuthentication();
        String effectiveBootstrap = resolveBootstrap(dc, clusterId, bootstrapServers);
        List<Map<String, Object>> all = service.discoverTopics(effectiveBootstrap, false, null, null, null);
        if (search == null || search.isBlank()) {
            return all;
        }
        String q = search.toLowerCase();
        return all.stream().filter(t -> ((String) t.get("name")).toLowerCase().contains(q)).toList();
    }

    public List<Map<String, Object>> discoverTopics(String dc, String search) {
        return discoverTopics(dc, null, null, search);
    }

    /**
     * Возвращает список консьюмер-групп для быстрого выбора в UI.
     */
    @GetMapping("/discovery/groups")
    public List<Map<String, Object>> discoverGroups(
            @RequestParam(required = false) String dc,
            @RequestParam(required = false) String clusterId,
            @RequestParam(required = false) String bootstrapServers,
            @RequestParam(required = false) String search) {
        requireAuthentication();
        String effectiveBootstrap = resolveBootstrap(dc, clusterId, bootstrapServers);
        List<Map<String, Object>> all = service.discoverConsumerGroups(effectiveBootstrap, false, null, null, null);
        if (search == null || search.isBlank()) {
            return all;
        }
        String q = search.toLowerCase();
        return all.stream().filter(g -> ((String) g.get("groupId")).toLowerCase().contains(q)).toList();
    }

    public List<Map<String, Object>> discoverGroups(String dc, String search) {
        return discoverGroups(dc, null, null, search);
    }

    private String resolveBootstrap(String dc, String clusterId, String bootstrapServers) {
        if (bootstrapServers != null && !bootstrapServers.isBlank()) {
            return bootstrapServers;
        }
        if (topologyRegistry == null) {
            return null;
        }
        if (clusterId != null && !clusterId.isBlank()) {
            var cfg = topologyRegistry.getCluster(clusterId).orElse(null);
            if (cfg != null && cfg.getKafkaBootstrapServers() != null && !cfg.getKafkaBootstrapServers().isBlank()) {
                return cfg.getKafkaBootstrapServers();
            }
        }
        if (dc != null && !dc.isBlank()) {
            for (var c : topologyRegistry.getClusters()) {
                var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
                if (cfg != null && dc.equalsIgnoreCase(cfg.getDcId()) && cfg.getKafkaBootstrapServers() != null && !cfg.getKafkaBootstrapServers().isBlank()) {
                    return cfg.getKafkaBootstrapServers();
                }
            }
        }
        return null;
    }
}

