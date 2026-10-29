package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.security.PrincipalSafety;
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
        try {
            request.setStartPosition(KafkaReplicationOrchestratorService.normalizeStartPosition(request.getStartPosition()));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        UserSession session = getSession();
        boolean isAdmin = session != null && session.systemRole() == Role.ADMIN;
        if (!isAdmin) {
            // [SEC-05] Запрет задания произвольных путей keytab для не-администраторов. Путь
            // keytab этого ЦОД из топологии произвольным не является: интерфейс подставляет его
            // сам, и отказ на нем закрывал создание задач всем, кроме администраторов
            if (isCustomKeytab(request.getSourceKeytabPath(), request.getSourceDc())
                    || isCustomKeytab(request.getTargetKeytabPath(), request.getTargetDc())) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                        "Задание путей к файлам keytab разрешено исключительно администраторам платформы");
            }
            if (request.getSourcePrincipal() != null && !request.getSourcePrincipal().isBlank()) {
                PrincipalSafety.validate(request.getSourcePrincipal());
            }
            if (request.getTargetPrincipal() != null && !request.getTargetPrincipal().isBlank()) {
                PrincipalSafety.validate(request.getTargetPrincipal());
            }
            applyTopologyDefaults(request, true);
        } else {
            // Администратор может задать учетную запись явно; если не задал, берется учетная
            // запись кластера из топологии. Без этого задача на керберизованном кластере
            // уходила в провижининг без кредов, и AdminClient молча шел к брокерам без SASL.
            applyTopologyDefaults(request, false);
        }
        KafkaReplicationJobDto created = service.createJob(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    /**
     * Пересечение задач — 409 с причиной в теле: стандартный ответ Spring причину не включает,
     * и оператор видел только «Conflict» без имени мешающей задачи.
     */
    @ExceptionHandler(KafkaReplicationOrchestratorService.JobOverlapException.class)
    public ResponseEntity<Map<String, Object>> handleJobOverlap(KafkaReplicationOrchestratorService.JobOverlapException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("status", 409, "message", e.getMessage()));
    }

    @ExceptionHandler(KafkaReplicationOrchestratorService.JobStateException.class)
    public ResponseEntity<Map<String, Object>> handleJobState(KafkaReplicationOrchestratorService.JobStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("status", 409, "message", e.getMessage()));
    }

    @PutMapping("/jobs/{id}")
    public ResponseEntity<KafkaReplicationJobDto> updateJob(@PathVariable String id,
                                                            @RequestBody org.apache.hadoop.explorer.replicator.orchestrator.dto.UpdateKafkaJobRequest request) {
        requireWriterOrAdmin();
        try {
            return service.updateJob(id, request).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @PostMapping("/jobs/{id}/reset-offsets")
    public ResponseEntity<Map<String, Object>> resetOffsets(@PathVariable String id,
                                                            @RequestBody(required = false) Map<String, String> body) {
        requireWriterOrAdmin();
        String position = body != null ? body.get("position") : null;
        if (position == null || position.isBlank()) {
            // Без явной позиции сброс пропустил бы данные (LATEST) или перечитал бы retention
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "position обязателен: LATEST или EARLIEST");
        }
        try {
            return service.resetOffsets(id, position)
                    .map(groups -> ResponseEntity.ok(Map.<String, Object>of("id", id, "position",
                            KafkaReplicationOrchestratorService.normalizeStartPosition(position), "groups", groups)))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
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
        return ResponseEntity.ok(Map.of("id", id, "action", "FAILOVER_TRIGGERED", "status", "SUCCESS",
                "job_status", "PAUSED"));
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
    public List<String> getReceiverEndpoints(@RequestParam String clusterId) {
        requireAuthentication();
        if (clusterId == null || clusterId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Параметр clusterId обязателен");
        }
        return service.getLiveReceiverEndpoints(clusterId.trim());
    }

    /**
     * Возвращает список активных адресов агентов-отправителей в кластере/ДЦ источника для горизонтального шардирования.
     */
    @GetMapping("/agents/senders")
    public List<String> getSenderEndpoints(@RequestParam String clusterId) {
        requireAuthentication();
        if (clusterId == null || clusterId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Параметр clusterId обязателен");
        }
        return service.getLiveSenderEndpoints(clusterId.trim());
    }

    /**
     * Детальная информация об агентах-отправителях источника (включая заданные при старте лимиты агентов).
     */
    @GetMapping("/agents/senders/details")
    public List<org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaAgentEndpointDto> getLiveSenderAgentDetails(@RequestParam String clusterId) {
        requireAuthentication();
        if (clusterId == null || clusterId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Параметр clusterId обязателен");
        }
        return service.getLiveSenderAgentDetails(clusterId.trim());
    }

    /**
     * Автоматический расчет QoS параметров репликации (согласование WAN HTB квоты и заданных лимитов агентов).
     */
    @GetMapping("/qos-calculation")
    public Map<String, Object> calculateQos(
            @RequestParam String sourceDc,
            @RequestParam String targetDc) {
        requireAuthentication();
        if (sourceDc == null || sourceDc.isBlank() || targetDc == null || targetDc.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Параметры sourceDc и targetDc обязательны");
        }

        Double wanQuotaMb = null;
        if (topologyRegistry != null) {
            var dcLimit = topologyRegistry.getConfiguredDcLimit(sourceDc.trim(), targetDc.trim());
            if (dcLimit.isPresent()) {
                wanQuotaMb = Math.round((dcLimit.get() * 0.40) / (1024.0 * 1024.0) * 10.0) / 10.0;
            }
        }

        var senders = service.getLiveSenderAgentDetails(sourceDc.trim());
        int sendersCount = senders.size();
        Double sendersCapacityMb = null;
        // Емкость известна, только если лимит задан у каждого отправителя
        if (!senders.isEmpty() && senders.stream()
                .allMatch(a -> a.getMaxBandwidthMbS() != null && a.getMaxBandwidthMbS() > 0)) {
            sendersCapacityMb = senders.stream()
                    .mapToDouble(a -> a.getMaxBandwidthMbS())
                    .sum();
        }

        double effectiveBandwidthMb;
        String bottleneck;
        if (wanQuotaMb != null && sendersCapacityMb != null) {
            if (wanQuotaMb < sendersCapacityMb) {
                effectiveBandwidthMb = wanQuotaMb;
                bottleneck = "WAN_HTB";
            } else {
                effectiveBandwidthMb = sendersCapacityMb;
                bottleneck = "AGENT_LIMIT";
            }
        } else if (wanQuotaMb != null) {
            effectiveBandwidthMb = wanQuotaMb;
            bottleneck = "WAN_HTB";
        } else if (sendersCapacityMb != null) {
            effectiveBandwidthMb = sendersCapacityMb;
            bottleneck = "AGENT_LIMIT";
        } else {
            effectiveBandwidthMb = 100.0;
            bottleneck = "WAN_HTB";
        }

        int targetTotalThreads;
        if (effectiveBandwidthMb <= 50.0) targetTotalThreads = 8;
        else if (effectiveBandwidthMb <= 100.0) targetTotalThreads = 16;
        else if (effectiveBandwidthMb <= 300.0) targetTotalThreads = 24;
        else if (effectiveBandwidthMb <= 800.0) targetTotalThreads = 32;
        else if (effectiveBandwidthMb <= 2000.0) targetTotalThreads = 64;
        else targetTotalThreads = Math.max(64, (int) Math.round(effectiveBandwidthMb / 30.0));

        int threadsPerAgent = Math.min(32, Math.max(1, (int) Math.ceil((double) targetTotalThreads / Math.max(1, sendersCount))));

        int batchSize;
        if (effectiveBandwidthMb <= 20.0) batchSize = 1000;
        else if (effectiveBandwidthMb <= 80.0) batchSize = 2000;
        else if (effectiveBandwidthMb <= 200.0) batchSize = 5000;
        else if (effectiveBandwidthMb <= 800.0) batchSize = 10000;
        else if (effectiveBandwidthMb <= 1500.0) batchSize = 15000;
        else if (effectiveBandwidthMb <= 3000.0) batchSize = 20000;
        else if (effectiveBandwidthMb <= 6000.0) batchSize = 35000;
        else batchSize = 50000;

        int offsetSyncSec;
        if (effectiveBandwidthMb >= 1500.0) offsetSyncSec = 1;
        else if (effectiveBandwidthMb >= 500.0) offsetSyncSec = 2;
        else if (effectiveBandwidthMb >= 100.0) offsetSyncSec = 3;
        else if (effectiveBandwidthMb <= 20.0) offsetSyncSec = 10;
        else offsetSyncSec = 5;

        Map<String, Object> res = new java.util.LinkedHashMap<>();
        res.put("wanQuotaMb", wanQuotaMb);
        res.put("sendersCapacityMb", sendersCapacityMb);
        res.put("effectiveBandwidthMb", effectiveBandwidthMb);
        res.put("bottleneck", bottleneck);
        res.put("sendersCount", sendersCount);
        res.put("threadsPerAgent", threadsPerAgent);
        res.put("totalThreads", threadsPerAgent * Math.max(1, sendersCount));
        res.put("batchSize", batchSize);
        res.put("offsetSyncSec", offsetSyncSec);
        return res;
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
            // Защита соединения с брокерами — из топологии; путь и пароль truststore не отдаются
            String protocol;
            try {
                protocol = org.apache.hadoop.explorer.replicator.orchestrator.kafka.DefaultKafkaAdminClientFactory
                        .securityProtocol(cfg, cfg.isKerberosEnabled());
            } catch (IllegalArgumentException e) {
                protocol = "INVALID: " + cfg.getKafkaSecurityProtocol();
            }
            map.put("securityProtocol", protocol);
            map.put("serviceName", !isBlank(cfg.getKafkaServiceName()) ? cfg.getKafkaServiceName() : "kafka");
            map.put("truststoreConfigured", !isBlank(cfg.getKafkaTruststoreLocation()));
            list.add(map);
        }
        // Реестр хранит кластеры в хеш-таблице, а форма берет первый кластер источником по
        // умолчанию: без сортировки направление новой задачи менялось от запуска к запуску
        list.sort(java.util.Comparator.comparing(m -> String.valueOf(m.get("id"))));
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
        DiscoveryTarget target = resolveCluster(dc, clusterId, bootstrapServers);
        List<Map<String, Object>> all = service.discoverTopics(target.bootstrap(),
                target.kerberosEnabled(), target.principal(), target.keytabPath(), null);
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
        DiscoveryTarget target = resolveCluster(dc, clusterId, bootstrapServers);
        List<Map<String, Object>> all = service.discoverConsumerGroups(target.bootstrap(),
                target.kerberosEnabled(), target.principal(), target.keytabPath(), null);
        if (search == null || search.isBlank()) {
            return all;
        }
        String q = search.toLowerCase();
        return all.stream().filter(g -> ((String) g.get("groupId")).toLowerCase().contains(q)).toList();
    }

    public List<Map<String, Object>> discoverGroups(String dc, String search) {
        return discoverGroups(dc, null, null, search);
    }

    /**
     * Учетная запись Kerberos кластеров задачи из топологии.
     *
     * @param override заменить переданные значения (не-администратор не вправе задавать свои);
     *                 иначе заполняются только незаданные principal и keytab
     */
    private void applyTopologyDefaults(CreateKafkaJobRequest request, boolean override) {
        if (topologyRegistry == null) return;
        for (var c : topologyRegistry.getClusters()) {
            var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
            if (cfg == null) continue;
            if (request.getSourceDc() != null && request.getSourceDc().equalsIgnoreCase(cfg.getDcId())) {
                if (cfg.isKerberosEnabled()) {
                    request.setSourceKerberosEnabled(true);
                    if (override || isBlank(request.getSourcePrincipal())) {
                        request.setSourcePrincipal(cfg.getPrincipal());
                    }
                    if (override || isBlank(request.getSourceKeytabPath())) {
                        request.setSourceKeytabPath(cfg.getKeytabPath());
                    }
                    if (!isBlank(cfg.getKafkaServiceName())
                            && (override || isBlank(request.getSourceServiceName()) || "kafka".equals(request.getSourceServiceName()))) {
                        request.setSourceServiceName(cfg.getKafkaServiceName());
                    }
                }
            }
            if (request.getTargetDc() != null && request.getTargetDc().equalsIgnoreCase(cfg.getDcId())) {
                if (cfg.isKerberosEnabled()) {
                    request.setTargetKerberosEnabled(true);
                    if (override || isBlank(request.getTargetPrincipal())) {
                        request.setTargetPrincipal(cfg.getPrincipal());
                    }
                    if (override || isBlank(request.getTargetKeytabPath())) {
                        request.setTargetKeytabPath(cfg.getKeytabPath());
                    }
                    if (!isBlank(cfg.getKafkaServiceName())
                            && (override || isBlank(request.getTargetServiceName()) || "kafka".equals(request.getTargetServiceName()))) {
                        request.setTargetServiceName(cfg.getKafkaServiceName());
                    }
                }
            }
        }
    }

    /** Путь keytab задан и отличается от keytab ЦОД в топологии. */
    private boolean isCustomKeytab(String keytabPath, String dc) {
        if (isBlank(keytabPath)) {
            return false;
        }
        if (topologyRegistry != null && dc != null) {
            for (var c : topologyRegistry.getClusters()) {
                var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
                if (cfg != null && dc.equalsIgnoreCase(cfg.getDcId())
                        && keytabPath.trim().equals(cfg.getKeytabPath() != null ? cfg.getKeytabPath().trim() : null)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private boolean isKnownBootstrap(String bootstrap) {
        if (bootstrap == null || bootstrap.isBlank() || topologyRegistry == null) {
            return false;
        }
        String trimmed = bootstrap.trim();
        for (var c : topologyRegistry.getClusters()) {
            var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
            if (cfg != null && trimmed.equalsIgnoreCase(cfg.getKafkaBootstrapServers())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Кластер, к которому обращается discovery: адрес брокеров и учетная запись Kerberos.
     *
     * <p>Discovery ходил к брокерам без SASL, и на керберизованном кластере AdminClient
     * получал отказ: список топиков в форме оставался пустым, и задачу нельзя было создать.
     * Учетная запись берется из топологии того же кластера, что и адрес брокеров.
     */
    private record DiscoveryTarget(String bootstrap, boolean kerberosEnabled,
                                   String principal, String keytabPath) {
        static DiscoveryTarget of(String bootstrap,
                                  org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties.ClusterConfig cfg) {
            if (cfg == null || !cfg.isKerberosEnabled()) {
                return new DiscoveryTarget(bootstrap, false, null, null);
            }
            return new DiscoveryTarget(bootstrap, true, cfg.getPrincipal(), cfg.getKeytabPath());
        }
    }

    private DiscoveryTarget resolveCluster(String dc, String clusterId, String bootstrapServers) {
        if (bootstrapServers != null && !bootstrapServers.isBlank()) {
            String trimmed = bootstrapServers.trim();
            if (trimmed.contains("169.254.") || trimmed.toLowerCase().contains("metadata")) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Заблокирована попытка обращения к сервису метаданных или link-local");
            }
            UserSession session = getSession();
            boolean isAdmin = session != null && session.systemRole() == Role.ADMIN;
            if (!isAdmin && topologyRegistry != null && !isKnownBootstrap(trimmed)) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                        "Указание произвольного bootstrap-сервера разрешено только администраторам платформы");
            }
            // Известный адрес получает учетную запись своего кластера, произвольный — без SASL
            if (topologyRegistry != null) {
                for (var c : topologyRegistry.getClusters()) {
                    var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
                    if (cfg != null && trimmed.equalsIgnoreCase(cfg.getKafkaBootstrapServers())) {
                        return DiscoveryTarget.of(trimmed, cfg);
                    }
                }
            }
            return new DiscoveryTarget(trimmed, false, null, null);
        }
        if (topologyRegistry == null) {
            return new DiscoveryTarget(null, false, null, null);
        }
        if (clusterId != null && !clusterId.isBlank()) {
            var cfg = topologyRegistry.getCluster(clusterId).orElse(null);
            if (cfg != null && cfg.getKafkaBootstrapServers() != null && !cfg.getKafkaBootstrapServers().isBlank()) {
                return DiscoveryTarget.of(cfg.getKafkaBootstrapServers(), cfg);
            }
        }
        if (dc != null && !dc.isBlank()) {
            for (var c : topologyRegistry.getClusters()) {
                var cfg = topologyRegistry.getCluster(c.getId()).orElse(null);
                if (cfg != null && dc.equalsIgnoreCase(cfg.getDcId()) && cfg.getKafkaBootstrapServers() != null && !cfg.getKafkaBootstrapServers().isBlank()) {
                    return DiscoveryTarget.of(cfg.getKafkaBootstrapServers(), cfg);
                }
            }
        }
        return new DiscoveryTarget(null, false, null, null);
    }
}
