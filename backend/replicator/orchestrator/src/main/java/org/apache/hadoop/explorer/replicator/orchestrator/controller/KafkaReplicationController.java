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

    public KafkaReplicationController(KafkaReplicationOrchestratorService service) {
        this.service = service;
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
        String keytabDisplay = isAdmin ? "/etc/security/keytabs/kafka-replicator.keytab" : "[PROTECTED]";

        return List.of(
                Map.of(
                        "id", "kafka-cluster-1",
                        "name", "Kafka DC1 Primary (Москва)",
                        "dcId", "dc1",
                        "location", "ЦОД-1 Москва (Сколково)",
                        "bootstrapServers", "kafka-cluster-1.demo-platform-net:9092",
                        "srUrl", "http://schema-registry-1:8081",
                        "srType", "HORTONWORKS",
                        "kerberosEnabled", true,
                        "principal", "kafka-replicator@COMPANY.LOCAL",
                        "keytabPath", keytabDisplay
                ),
                Map.of(
                        "id", "kafka-cluster-2",
                        "name", "Kafka DC2 Disaster Recovery (СПб)",
                        "dcId", "dc2",
                        "location", "ЦОД-2 Санкт-Петербург (Пулково)",
                        "bootstrapServers", "kafka-cluster-2.demo-platform-net:9092",
                        "srUrl", "http://schema-registry-2:8082",
                        "srType", "CONFLUENT",
                        "kerberosEnabled", true,
                        "principal", "kafka-replicator@COMPANY.LOCAL",
                        "keytabPath", keytabDisplay
                )
        );
    }

    /**
     * Возвращает список обнаруженных топиков для быстрого автокомплита в UI.
     */
    @GetMapping("/discovery/topics")
    public List<Map<String, Object>> discoverTopics(
            @RequestParam(required = false) String dc,
            @RequestParam(required = false) String search) {
        requireAuthentication();
        List<Map<String, Object>> all = List.of(
                // Orders domain
                Map.of("name", "orders-v1", "partitions", 12, "detectedFormat", "AUTO_DETECT", "hasSchema", true),
                Map.of("name", "orders-avro", "partitions", 6, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                Map.of("name", "orders-dlq", "partitions", 3, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "orders-checkout-events", "partitions", 8, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                // Payments domain
                Map.of("name", "payments-raw", "partitions", 6, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "payments-clearing-avro", "partitions", 12, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                Map.of("name", "payments-fraud-alerts", "partitions", 4, "detectedFormat", "AUTO_DETECT", "hasSchema", true),
                // Billing domain
                Map.of("name", "billing-invoices", "partitions", 8, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                Map.of("name", "billing-subscription-events", "partitions", 4, "detectedFormat", "AUTO_DETECT", "hasSchema", true),
                Map.of("name", "billing-audit-trail", "partitions", 2, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                // Telemetry & Metrics
                Map.of("name", "telemetry-stream", "partitions", 16, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "telemetry-iot-sensors", "partitions", 32, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "metrics-raw", "partitions", 16, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "metrics-aggregated-1m", "partitions", 8, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                // Users & Customers
                Map.of("name", "user-events", "partitions", 8, "detectedFormat", "AUTO_DETECT", "hasSchema", true),
                Map.of("name", "user-cache-sync", "partitions", 4, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "customer-notifications", "partitions", 6, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                Map.of("name", "customer-profiles-cdc", "partitions", 12, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                // Inventory & Supply Chain
                Map.of("name", "inventory-stock-updates", "partitions", 8, "detectedFormat", "AUTO_DETECT", "hasSchema", true),
                Map.of("name", "inventory-warehouse-events", "partitions", 6, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                // CDC & DB streams
                Map.of("name", "cdc-postgres-accounts", "partitions", 12, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                Map.of("name", "cdc-mysql-merchants", "partitions", 8, "detectedFormat", "SCHEMA_REGISTRY", "hasSchema", true),
                // System & Audit
                Map.of("name", "system-audit-logs", "partitions", 4, "detectedFormat", "PASSTHROUGH", "hasSchema", false),
                Map.of("name", "system-health-heartbeats", "partitions", 2, "detectedFormat", "PASSTHROUGH", "hasSchema", false)
        );
        if (search == null || search.isBlank()) {
            return all;
        }
        String q = search.toLowerCase();
        return all.stream().filter(t -> ((String) t.get("name")).toLowerCase().contains(q)).toList();
    }

    /**
     * Возвращает список консьюмер-групп для быстрого выбора в UI.
     */
    @GetMapping("/discovery/groups")
    public List<Map<String, Object>> discoverGroups(
            @RequestParam(required = false) String dc,
            @RequestParam(required = false) String search) {
        requireAuthentication();
        List<Map<String, Object>> all = List.of(
                // Billing groups
                Map.of("groupId", "billing-service-prod", "state", "STABLE", "members", 4, "lag", 12),
                Map.of("groupId", "billing-invoicing-worker", "state", "STABLE", "members", 2, "lag", 5),
                Map.of("groupId", "billing-archiver-cron", "state", "EMPTY", "members", 0, "lag", 340),
                // Orders & Fraud
                Map.of("groupId", "orders-fulfillment-engine", "state", "STABLE", "members", 8, "lag", 3),
                Map.of("groupId", "fraud-detection-engine", "state", "STABLE", "members", 6, "lag", 0),
                Map.of("groupId", "fraud-alerts-telegram-bot", "state", "STABLE", "members", 1, "lag", 1),
                // Analytics & ML
                Map.of("groupId", "analytics-stream-processor", "state", "STABLE", "members", 6, "lag", 8),
                Map.of("groupId", "analytics-clickhouse-sink", "state", "STABLE", "members", 12, "lag", 25),
                Map.of("groupId", "ml-feature-store-loader", "state", "STABLE", "members", 3, "lag", 0),
                Map.of("groupId", "ml-realtime-scoring-worker", "state", "STABLE", "members", 4, "lag", 2),
                // Notifications & Dispatchers
                Map.of("groupId", "notification-dispatcher", "state", "STABLE", "members", 5, "lag", 19),
                Map.of("groupId", "push-notification-gateway", "state", "STABLE", "members", 4, "lag", 7),
                // Search & Indexing
                Map.of("groupId", "search-indexing-pipeline", "state", "STABLE", "members", 8, "lag", 2),
                Map.of("groupId", "elasticsearch-cdc-sync", "state", "STABLE", "members", 4, "lag", 11),
                // Legacy & Maintenance
                Map.of("groupId", "audit-archiver-service", "state", "EMPTY", "members", 0, "lag", 450),
                Map.of("groupId", "legacy-reports-exporter", "state", "EMPTY", "members", 0, "lag", 1250),
                Map.of("groupId", "gdpr-compliance-scanner", "state", "EMPTY", "members", 0, "lag", 0),
                Map.of("groupId", "dev-test-consumer-group", "state", "EMPTY", "members", 0, "lag", 88)
        );
        if (search == null || search.isBlank()) {
            return all;
        }
        String q = search.toLowerCase();
        return all.stream().filter(g -> ((String) g.get("groupId")).toLowerCase().contains(q)).toList();
    }
}

