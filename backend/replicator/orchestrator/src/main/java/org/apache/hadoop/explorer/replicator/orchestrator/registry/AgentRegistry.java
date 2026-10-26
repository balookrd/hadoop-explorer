package org.apache.hadoop.explorer.replicator.orchestrator.registry;

import jakarta.annotation.PostConstruct;
import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.AgentEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.AgentRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class AgentRegistry {

    private static final Logger log = LoggerFactory.getLogger(AgentRegistry.class);

    public enum AgentStatus {
        ONLINE,
        SUSPECT,
        OFFLINE
    }

    public static class AgentEntry {
        private final String agentId;
        private final String clusterId;
        private final String host;
        private final int grpcPort;
        private final String grpcAddress;
        private String mode;
        private int activeTransfers;
        private Double maxBandwidthMbS;
        private volatile AgentStatus status;
        private volatile Instant lastHeartbeat;
        private long transferredBytes;
        private long transferErrors;
        private long compressionRawBytes = 0L;
        private long compressionCompressedBytes = 0L;
        private double compressionRatio = 1.0;
        private double chunkLatencyAvgMs = 0.0;
        private double chunkLatencyMaxMs = 0.0;
        private long chunkTransferCount = 0L;
        private int stagingActiveFiles = 0;
        private long stagingActiveBytes = 0L;
        private long stagingOrphansCleaned = 0L;

        public AgentEntry(String agentId, String clusterId, String host, int grpcPort, String grpcAddress, String mode, Double maxBandwidthMbS) {
            this.agentId = agentId;
            this.clusterId = clusterId;
            this.host = host;
            this.grpcPort = grpcPort;
            this.grpcAddress = grpcAddress;
            this.mode = mode != null ? mode : "all";
            this.activeTransfers = 0;
            this.maxBandwidthMbS = maxBandwidthMbS;
            this.status = AgentStatus.ONLINE;
            this.lastHeartbeat = Instant.now();
            this.transferredBytes = 0L;
            this.transferErrors = 0L;
            this.compressionRawBytes = 0L;
            this.compressionCompressedBytes = 0L;
            this.compressionRatio = 1.0;
            this.chunkLatencyAvgMs = 0.0;
            this.chunkLatencyMaxMs = 0.0;
            this.chunkTransferCount = 0L;
            this.stagingActiveFiles = 0;
            this.stagingActiveBytes = 0L;
            this.stagingOrphansCleaned = 0L;
        }

        public String getAgentId() { return agentId; }
        public String getClusterId() { return clusterId; }
        public String getHost() { return host; }
        public int getGrpcPort() { return grpcPort; }
        public String getGrpcAddress() { return grpcAddress; }
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
        public int getActiveTransfers() { return activeTransfers; }
        public void setActiveTransfers(int activeTransfers) { this.activeTransfers = activeTransfers; }
        public Double getMaxBandwidthMbS() { return maxBandwidthMbS; }
        public void setMaxBandwidthMbS(Double maxBandwidthMbS) { this.maxBandwidthMbS = maxBandwidthMbS; }
        public AgentStatus getStatus() { return status; }
        public void setStatus(AgentStatus status) { this.status = status; }
        public Instant getLastHeartbeat() { return lastHeartbeat; }
        public void setLastHeartbeat(Instant lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
        public long getTransferredBytes() { return transferredBytes; }
        public void setTransferredBytes(long transferredBytes) { this.transferredBytes = transferredBytes; }
        public long getTransferErrors() { return transferErrors; }
        public void setTransferErrors(long transferErrors) { this.transferErrors = transferErrors; }
        public long getCompressionRawBytes() { return compressionRawBytes; }
        public void setCompressionRawBytes(long compressionRawBytes) { this.compressionRawBytes = compressionRawBytes; }
        public long getCompressionCompressedBytes() { return compressionCompressedBytes; }
        public void setCompressionCompressedBytes(long compressionCompressedBytes) { this.compressionCompressedBytes = compressionCompressedBytes; }
        public double getCompressionRatio() { return compressionRatio; }
        public void setCompressionRatio(double compressionRatio) { this.compressionRatio = compressionRatio; }
        public double getChunkLatencyAvgMs() { return chunkLatencyAvgMs; }
        public void setChunkLatencyAvgMs(double chunkLatencyAvgMs) { this.chunkLatencyAvgMs = chunkLatencyAvgMs; }
        public double getChunkLatencyMaxMs() { return chunkLatencyMaxMs; }
        public void setChunkLatencyMaxMs(double chunkLatencyMaxMs) { this.chunkLatencyMaxMs = chunkLatencyMaxMs; }
        public long getChunkTransferCount() { return chunkTransferCount; }
        public void setChunkTransferCount(long chunkTransferCount) { this.chunkTransferCount = chunkTransferCount; }
        public int getStagingActiveFiles() { return stagingActiveFiles; }
        public void setStagingActiveFiles(int stagingActiveFiles) { this.stagingActiveFiles = stagingActiveFiles; }
        public long getStagingActiveBytes() { return stagingActiveBytes; }
        public void setStagingActiveBytes(long stagingActiveBytes) { this.stagingActiveBytes = stagingActiveBytes; }
        public long getStagingOrphansCleaned() { return stagingOrphansCleaned; }
        public void setStagingOrphansCleaned(long stagingOrphansCleaned) { this.stagingOrphansCleaned = stagingOrphansCleaned; }
    }

    private final ReplicatorProperties properties;
    private final TopologyRegistry topologyRegistry;
    private final AgentRepository agentRepository;
    private final Map<String, AgentEntry> agents = new ConcurrentHashMap<>();
    private final java.util.Set<String> dirtyAgentIds = ConcurrentHashMap.newKeySet();

    public AgentRegistry(ReplicatorProperties properties, TopologyRegistry topologyRegistry) {
        this(properties, topologyRegistry, null);
    }

    @Autowired
    public AgentRegistry(ReplicatorProperties properties,
                         TopologyRegistry topologyRegistry,
                         @Autowired(required = false) AgentRepository agentRepository) {
        this.properties = properties;
        this.topologyRegistry = topologyRegistry;
        this.agentRepository = agentRepository;
    }

    @PostConstruct
    public void initSecurityValidation() {
        String secret = properties.getAgentSecret();
        if (properties.isEnforceAgentSecret()) {
            if (secret == null || secret.isBlank() || secret.trim().length() < 16) {
                throw new IllegalStateException("В защищенном режиме (enforce-agent-secret=true) "
                        + "hadoop.replicator.agent-secret должен быть задан и содержать минимум 16 символов");
            }
            // Унаследованный ключ из репозитория известен публично, поэтому в защищенном
            // режиме он равнозначен отсутствию секрета.
            if (ReplicatorProperties.LEGACY_DEFAULT_AGENT_SECRET.equals(secret.trim())) {
                throw new IllegalStateException("hadoop.replicator.agent-secret равен публично известному "
                        + "значению из репозитория: задайте уникальный секрет через свойство "
                        + "hadoop.replicator.agent-secret или переменную окружения REPLICATOR_AGENT_SECRET");
            }
        } else if (secret == null || secret.isBlank()) {
            log.warn("[SECURITY ALERT] Секрет агента не задан, а enforce-agent-secret выключен: "
                    + "gRPC Control Plane будет отклонять вызовы агентов (Fail-Closed), "
                    + "а REST-аутентификация агентов недоступна");
        } else if (ReplicatorProperties.LEGACY_DEFAULT_AGENT_SECRET.equals(secret.trim())) {
            log.warn("[SECURITY ALERT] Активен публично известный секрет агента из репозитория. "
                    + "Задайте уникальный секрет через hadoop.replicator.agent-secret "
                    + "или REPLICATOR_AGENT_SECRET.");
        }
        loadAgentsFromDatabase();
    }

    private void loadAgentsFromDatabase() {
        if (agentRepository == null) return;
        try {
            List<AgentEntity> entities = agentRepository.findAll();
            for (AgentEntity entity : entities) {
                AgentEntry entry = fromEntity(entity);
                agents.put(entry.getAgentId(), entry);
                topologyRegistry.updateClusterGrpcAddress(entry.getClusterId(), entry.getGrpcAddress());
            }
            if (!entities.isEmpty()) {
                log.info("Успешно восстановлено {} агентов из БД в in-memory реестр", entities.size());
            }
        } catch (Exception e) {
            log.warn("Не удалось загрузить состояние агентов из БД при старте: {}", e.getMessage());
        }
    }

    public void clear() {
        dirtyAgentIds.clear();
        agents.clear();
    }

    public void validateGrpcAddress(String address) {
        if (address == null || address.isBlank()) return;
        String trimmed = address.trim();
        int lastColon = trimmed.lastIndexOf(':');
        if (lastColon <= 0 || lastColon == trimmed.length() - 1) {
            throw new IllegalArgumentException("gRPC адрес должен быть в формате host:port (получено: " + address + ")");
        }

        String hostPart = trimmed.substring(0, lastColon);
        String portPart = trimmed.substring(lastColon + 1);

        int port;
        try {
            port = Integer.parseInt(portPart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Некорректный порт: " + portPart);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Порт вне диапазона 1-65535: " + port);
        }

        String cleanHost = hostPart.startsWith("[") && hostPart.endsWith("]")
                ? hostPart.substring(1, hostPart.length() - 1)
                : hostPart;

        if (!cleanHost.matches("^[a-zA-Z0-9.\\-_:]+$")) {
            throw new IllegalArgumentException("Недопустимые символы в адресе хоста: " + cleanHost);
        }

        String hostLower = cleanHost.toLowerCase(Locale.ROOT);
        if (hostLower.contains("metadata")
                || hostLower.startsWith("169.254.")
                || hostLower.contains(".169.254.")
                || hostLower.startsWith("fe80:")
                || hostLower.equals("100.100.100.200")
                || hostLower.contains("::ffff:169.254.")) {
            throw new SecurityException("SSRF защита: запрещен анонс облачных эндпоинтов метаданных и link-local адресов (" + hostPart + ")");
        }

        if (!properties.isAllowLoopbackAgents()) {
            if (hostLower.equals("localhost")
                    || hostLower.equals("127.0.0.1")
                    || hostLower.equals("::1")
                    || hostLower.startsWith("127.")
                    || hostLower.equals("0.0.0.0")) {
                throw new SecurityException("SSRF защита: анонс Loopback адресов запрещен политикой allowLoopbackAgents=false (" + hostPart + ")");
            }
        }

        try {
            InetAddress inet = InetAddress.getByName(cleanHost);
            if (!properties.isAllowLoopbackAgents() && inet.isLoopbackAddress()) {
                throw new SecurityException("SSRF защита: анонс Loopback адресов запрещен политикой allowLoopbackAgents=false (" + hostPart + ")");
            }
            if (inet.isLinkLocalAddress()) {
                throw new SecurityException("SSRF защита: запрещен анонс Link-Local адресов (" + hostPart + ")");
            }
            if (inet.isMulticastAddress()) {
                throw new SecurityException("SSRF защита: запрещен анонс Multicast адресов (" + hostPart + ")");
            }
            if (inet.isAnyLocalAddress()) {
                throw new SecurityException("SSRF защита: запрещен анонс AnyLocal wildcard адресов (" + hostPart + ")");
            }
            byte[] raw = inet.getAddress();
            if (raw.length == 4 && (raw[0] & 0xFF) == 169 && (raw[1] & 0xFF) == 254) {
                throw new SecurityException("SSRF защита: запрещен анонс диапазона 169.254.0.0/16 (" + hostPart + ")");
            }
            if (raw.length == 16 && raw[10] == (byte) 0xFF && raw[11] == (byte) 0xFF
                    && (raw[12] & 0xFF) == 169 && (raw[13] & 0xFF) == 254) {
                throw new SecurityException("SSRF защита: запрещен анонс IPv4-mapped IPv6 диапазона 169.254.0.0/16 (" + hostPart + ")");
            }
        } catch (UnknownHostException ignored) {
            // Если хост неизвестен локальному DNS, проводим проверку синтаксиса FQDN
            if (cleanHost.startsWith(".") || cleanHost.endsWith(".") || cleanHost.contains("..")) {
                throw new SecurityException("Некорректный синтаксис неразрешенного FQDN хоста: " + cleanHost);
            }
            if (cleanHost.chars().allMatch(c -> Character.isDigit(c) || c == '.')) {
                throw new SecurityException("Недопустимый IP адрес хоста: " + cleanHost);
            }
        }
    }

    public void validateClusterAssignment(String agentId, String requestedClusterId) {
        if (agentId == null || agentId.isBlank()) {
            throw new IllegalArgumentException("agentId не может быть пустым");
        }
        String effectiveClusterId = requestedClusterId != null && !requestedClusterId.isBlank()
                ? requestedClusterId.trim()
                : "default";

        // Защита от Cluster Hijacking: запрет несанкционированной смены кластера для активного агента
        AgentEntry existing = agents.get(agentId);
        if (existing != null && !matchesCluster(existing.getClusterId(), effectiveClusterId)) {
            log.error("Обнаружена попытка смены кластера (Cluster Hijacking) для агента '{}': текущий кластер '{}', запрошенный '{}'",
                    agentId, existing.getClusterId(), effectiveClusterId);
            throw new SecurityException("Попытка смены кластера для агента '" + agentId +
                    "': агент уже зарегистрирован в кластере '" + existing.getClusterId() +
                    "', переключение на '" + effectiveClusterId + "' запрещено без явной дерегистрации");
        }

        // Проверка белого списка кластеров (enforce-cluster-whitelist)
        if (properties.isEnforceClusterWhitelist()) {
            List<org.apache.hadoop.explorer.replicator.model.ClusterDto> knownClusters = topologyRegistry.getClusters();
            if (!knownClusters.isEmpty()) {
                boolean match = knownClusters.stream().anyMatch(c -> matchesCluster(c.getId(), effectiveClusterId));
                if (!match) {
                    List<String> allowedIds = knownClusters.stream().map(org.apache.hadoop.explorer.replicator.model.ClusterDto::getId).toList();
                    log.error("Отклонена регистрация агента '{}' в неразрешенном кластере '{}'. Допустимые кластеры: {}",
                            agentId, effectiveClusterId, allowedIds);
                    throw new SecurityException("Кластер '" + effectiveClusterId +
                            "' не разрешен политикой enforce-cluster-whitelist. Допустимые кластеры: " + allowedIds);
                }
            }
        }
    }

    public boolean register(AgentRegisterRequest request, String secretHeader) {
        verifySecret(secretHeader);
        validateGrpcAddress(request.getGrpcAddress());

        String agentId = request.getAgentId();
        String clusterId = request.getClusterId() != null && !request.getClusterId().isBlank()
                ? request.getClusterId() : "default";

        validateClusterAssignment(agentId, clusterId);

        String host = "localhost";
        int port = 50051;
        String grpcAddress = request.getGrpcAddress();
        if (grpcAddress != null && grpcAddress.contains(":")) {
            int lastColon = grpcAddress.lastIndexOf(':');
            host = grpcAddress.substring(0, lastColon);
            try {
                port = Integer.parseInt(grpcAddress.substring(lastColon + 1));
            } catch (NumberFormatException ignored) {}
        } else {
            grpcAddress = host + ":" + port;
        }

        AgentEntry entry = new AgentEntry(
            agentId,
            clusterId,
            host,
            port,
            grpcAddress,
            request.getMode(),
            request.getMaxBandwidthMbS()
        );

        agents.put(agentId, entry);
        persistAgent(entry);
        topologyRegistry.updateClusterGrpcAddress(clusterId, grpcAddress);
        log.info("Agent registered: id={}, cluster={}, grpc={}, mode={}", agentId, clusterId, grpcAddress, entry.getMode());
        return true;
    }

    public boolean heartbeat(AgentHeartbeatRequest request, String secretHeader) {
        verifySecret(secretHeader);
        String agentId = request.getAgentId();
        if (agentId == null || agentId.isBlank()) {
            return false;
        }

        String clusterId = request.getClusterId() != null && !request.getClusterId().isBlank()
                ? request.getClusterId() : "default";

        validateClusterAssignment(agentId, clusterId);
        if (request.getGrpcAddress() != null && !request.getGrpcAddress().isBlank()) {
            validateGrpcAddress(request.getGrpcAddress());
        }

        AgentEntry entry = agents.get(agentId);
        boolean isNewAgent = (entry == null);
        String mode = (request.getMode() != null && !request.getMode().isBlank())
                ? request.getMode()
                : (agentId.toLowerCase().startsWith("streamer") ? "streamer" : "all");
        if (entry == null) {
            String host = "localhost";
            int port = 50051;
            String grpcAddress = request.getGrpcAddress();
            if (grpcAddress != null && grpcAddress.contains(":")) {
                int lastColon = grpcAddress.lastIndexOf(':');
                host = grpcAddress.substring(0, lastColon);
                try {
                    port = Integer.parseInt(grpcAddress.substring(lastColon + 1));
                } catch (NumberFormatException ignored) {}
            } else {
                grpcAddress = host + ":" + port;
            }
            entry = new AgentEntry(agentId, clusterId, host, port, grpcAddress, mode, null);
            agents.put(agentId, entry);
            topologyRegistry.updateClusterGrpcAddress(clusterId, grpcAddress);
            log.info("Agent auto-registered via heartbeat: id={}, cluster={}, grpc={}, mode={}", agentId, clusterId, grpcAddress, mode);
            persistAgent(entry);
        } else if (request.getMode() != null && !request.getMode().isBlank()) {
            entry.setMode(request.getMode());
        }

        entry.setLastHeartbeat(Instant.now());
        entry.setStatus(AgentStatus.ONLINE);
        entry.setActiveTransfers(request.getActiveTransfers());
        if (request.getTransferredBytes() > 0) {
            entry.setTransferredBytes(request.getTransferredBytes());
        }
        if (request.getTransferErrors() > 0) {
            entry.setTransferErrors(request.getTransferErrors());
        }
        if (request.getBandwidthLimitMbS() != null) {
            entry.setMaxBandwidthMbS(request.getBandwidthLimitMbS());
        }
        if (request.getCompressionRawBytes() > 0) {
            entry.setCompressionRawBytes(request.getCompressionRawBytes());
        }
        if (request.getCompressionCompressedBytes() > 0) {
            entry.setCompressionCompressedBytes(request.getCompressionCompressedBytes());
        }
        if (request.getCompressionRatio() != null && request.getCompressionRatio() > 0) {
            entry.setCompressionRatio(request.getCompressionRatio());
        }
        if (request.getChunkLatencyAvgMs() != null) {
            entry.setChunkLatencyAvgMs(request.getChunkLatencyAvgMs());
        }
        if (request.getChunkLatencyMaxMs() != null) {
            entry.setChunkLatencyMaxMs(request.getChunkLatencyMaxMs());
        }
        if (request.getChunkTransferCount() > 0) {
            entry.setChunkTransferCount(request.getChunkTransferCount());
        }
        entry.setStagingActiveFiles(request.getStagingActiveFiles());
        entry.setStagingActiveBytes(request.getStagingActiveBytes());
        if (request.getStagingOrphansCleaned() > 0) {
            entry.setStagingOrphansCleaned(request.getStagingOrphansCleaned());
        }

        // Write-Behind Cache: при повторном heartbeat от известного агента помечаем узел dirty
        // и сбрасываем батчем в фоне, предотвращая лавину синхронных транзакций к БД на 300+ агентах
        if (isNewAgent) {
            persistAgent(entry);
        } else {
            dirtyAgentIds.add(agentId);
        }
        return true;
    }

    public boolean unregister(String agentId, String secretHeader) {
        verifySecret(secretHeader);
        dirtyAgentIds.remove(agentId);
        AgentEntry entry = agents.remove(agentId);
        if (entry != null) {
            deleteAgentFromDb(agentId);
            log.info("Agent unregistered: id={}", agentId);
            return true;
        }
        return false;
    }

    /**
     * Фоновый сброс накопленных состояний агентов в БД (Write-Behind Cache).
     * Срабатывает раз в 10 секунд, заменяя сотни синхронных UPDATE на одиночный пакетный saveAll.
     */
    @org.springframework.scheduling.annotation.Scheduled(fixedDelayString = "${hadoop.replicator.agent-registry-flush-interval-ms:10000}")
    public int flushDirtyAgentsToDatabase() {
        if (agentRepository == null || dirtyAgentIds.isEmpty()) {
            return 0;
        }
        List<AgentEntity> toSave = new ArrayList<>();
        List<String> ids = new ArrayList<>(dirtyAgentIds);
        for (String id : ids) {
            dirtyAgentIds.remove(id);
            AgentEntry entry = agents.get(id);
            if (entry != null) {
                toSave.add(toEntity(entry));
            }
        }
        if (!toSave.isEmpty()) {
            try {
                agentRepository.saveAll(toSave);
                return toSave.size();
            } catch (Exception e) {
                log.warn("Ошибка фонового сброса dirty-агентов в БД: {}", e.getMessage());
            }
        }
        return 0;
    }

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        flushDirtyAgentsToDatabase();
    }

    public List<AgentEntry> getAgents() {
        refreshHealth();
        return new ArrayList<>(agents.values());
    }

    public List<AgentResponseDto> getAgentDtos() {
        refreshHealth();
        Instant now = Instant.now();
        List<AgentResponseDto> list = new ArrayList<>();
        for (AgentEntry a : agents.values()) {
            long ageSec = Math.max(0, now.getEpochSecond() - a.getLastHeartbeat().getEpochSecond());
            String statusStr = switch (a.getStatus()) {
                case ONLINE -> "online";
                case SUSPECT -> "stale";
                case OFFLINE -> "offline";
            };

            list.add(new AgentResponseDto(
                a.getAgentId(),
                a.getClusterId(),
                a.getGrpcAddress(),
                statusStr,
                a.getMode(),
                a.getActiveTransfers(),
                a.getMaxBandwidthMbS(),
                ageSec,
                a.getLastHeartbeat().toString(),
                a.getTransferredBytes(),
                a.getTransferErrors(),
                a.getCompressionRawBytes(),
                a.getCompressionCompressedBytes(),
                a.getCompressionRatio(),
                a.getChunkLatencyAvgMs(),
                a.getChunkLatencyMaxMs(),
                a.getChunkTransferCount(),
                a.getStagingActiveFiles(),
                a.getStagingActiveBytes(),
                a.getStagingOrphansCleaned()
            ));
        }
        return list;
    }

    public Optional<AgentEntry> getLiveAgentForCluster(String clusterId) {
        return agents.values().stream()
            .filter(a -> matchesCluster(a.getClusterId(), clusterId) && isLive(a))
            .findAny();
    }

    public Optional<AgentEntry> getLiveHmsAgentForCluster(String clusterId) {
        return agents.values().stream()
            .filter(a -> matchesCluster(a.getClusterId(), clusterId)
                    && isLive(a)
                    && ("all".equalsIgnoreCase(a.getMode()) || "hms".equalsIgnoreCase(a.getMode())))
            .findAny();
    }

    /**
     * Поиск активного агента-стримера HDFS Inotify для кластера.
     */
    public Optional<AgentEntry> getLiveStreamerForCluster(String clusterId) {
        return agents.values().stream()
            .filter(a -> matchesCluster(a.getClusterId(), clusterId)
                    && isLive(a)
                    && "streamer".equalsIgnoreCase(a.getMode()))
            .findAny();
    }

    /**
     * Список всех активных агентов-стримеров HDFS Inotify для кластера.
     */
    public List<AgentEntry> getLiveStreamersForCluster(String clusterId) {
        return agents.values().stream()
            .filter(a -> matchesCluster(a.getClusterId(), clusterId)
                    && isLive(a)
                    && "streamer".equalsIgnoreCase(a.getMode()))
            .toList();
    }

    /**
     * Количество активных онлайн-стримеров HDFS Inotify для кластера.
     */
    public int countLiveStreamers(String clusterId) {
        return getLiveStreamersForCluster(clusterId).size();
    }

    /**
     * Проверка наличия хотя бы одного активного зарегистрированного Inotify-стримера.
     */
    public boolean hasLiveStreamerForCluster(String clusterId) {
        return getLiveStreamerForCluster(clusterId).isPresent();
    }

    /**
     * Поиск наименее загруженного агента в целевом кластере для балансировки входящего трафика.
     */
    public Optional<AgentEntry> getLeastLoadedAgentForCluster(String clusterId) {
        return agents.values().stream()
            .filter(a -> a.getClusterId().equalsIgnoreCase(clusterId)
                    && isLive(a)
                    && ("all".equalsIgnoreCase(a.getMode()) || "receiver".equalsIgnoreCase(a.getMode())))
            .min(Comparator.comparingInt(AgentEntry::getActiveTransfers));
    }

    /**
     * Возвращает список всех активных агентов-приемников для кластера Kafka репликации.
     */
    public List<AgentEntry> getLiveKafkaReceiversForCluster(String clusterId) {
        return agents.values().stream()
            .filter(a -> matchesCluster(a.getClusterId(), clusterId)
                    && isLive(a)
                    && ("all".equalsIgnoreCase(a.getMode())
                        || "kafka".equalsIgnoreCase(a.getMode())
                        || "receiver".equalsIgnoreCase(a.getMode())
                        || "dual".equalsIgnoreCase(a.getMode())))
            .toList();
    }

    /**
     * Возвращает список всех активных агентов-отправителей для кластера Kafka репликации.
     */
    public List<AgentEntry> getLiveKafkaSendersForCluster(String clusterId) {
        return agents.values().stream()
            .filter(a -> matchesCluster(a.getClusterId(), clusterId)
                    && isLive(a)
                    && ("all".equalsIgnoreCase(a.getMode())
                        || "kafka".equalsIgnoreCase(a.getMode())
                        || "sender".equalsIgnoreCase(a.getMode())
                        || "dual".equalsIgnoreCase(a.getMode())))
            .toList();
    }

    public Optional<AgentEntry> getAgent(String agentId) {
        if (agentId == null) return Optional.empty();
        return Optional.ofNullable(agents.get(agentId));
    }

    public boolean hasOtherOnlineAgentsForCluster(String clusterId, String excludeAgentId) {
        return agents.values().stream()
                .anyMatch(a -> matchesCluster(a.getClusterId(), clusterId)
                        && isLive(a)
                        && !a.getAgentId().equalsIgnoreCase(excludeAgentId));
    }

    private boolean matchesCluster(String agentCluster, String targetCluster) {
        if (agentCluster == null || targetCluster == null) return false;
        if (agentCluster.equalsIgnoreCase(targetCluster)) return true;
        String aNorm = agentCluster.toLowerCase().replace("-", "").replace("_", "");
        String tNorm = targetCluster.toLowerCase().replace("-", "").replace("_", "");
        return aNorm.equals(tNorm);
    }

    /**
     * Проверка секрета агента.
     *
     * <p>Вызывается всегда, а не по условию {@code enforce || secretHeader != null}: с таким
     * условием достаточно было не прислать заголовок, чтобы проверка не выполнилась вовсе, и при
     * выключенном {@code enforce-agent-secret} — значении по умолчанию — заданный секрет защищал
     * только тех, кто его присылал. Кто угодно мог зарегистрировать агента, принять на себя его
     * задачи и слать heartbeat за него.
     *
     * <p>Контур без заданного секрета остается открытым: это режим разработки, и он обозначен
     * предупреждением при старте.
     */
    public void verifySecret(String secretHeader) {
        String expected = properties.getAgentSecret();
        if (properties.isEnforceAgentSecret() && (expected == null || expected.isBlank() || expected.trim().length() < 16)) {
            throw new SecurityException("В защищенном режиме секрет агента не сконфигурирован должным образом (минимум 16 символов)");
        }
        if (expected == null || expected.isBlank()) {
            if (properties.isEnforceAgentSecret()) {
                throw new SecurityException("Секрет агента обязателен в защищенном режиме, но не сконфигурирован на сервере");
            }
            return;
        }
        if (secretHeader == null) {
            throw new SecurityException("Отсутствует X-Agent-Secret токен агента");
        }
        byte[] expectedBytes = expected.trim().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] actualBytes = secretHeader.trim().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (!java.security.MessageDigest.isEqual(expectedBytes, actualBytes)) {
            throw new SecurityException("Неверный X-Agent-Secret токен агента");
        }
    }

    /**
     * Живость агента по времени последнего heartbeat, без записи в общее состояние.
     *
     * <p>Горячие пути (забор задач, выбор приемника) вызывались десятки тысяч раз за секунду,
     * и каждый из них прогонял {@code refreshHealth()} — полный проход по всем агентам с
     * мутацией их статусов. Проверка на лету стоит одно сравнение и ничего не пишет.
     */
    /**
     * Живость агента с опорой на БД.
     *
     * <p>Реестр локален для узла: heartbeat приходит на тот узел, который выбрал
     * балансировщик. Планировщик failover может выполняться на другом узле, где записи об
     * агенте в памяти нет вовсе, — и тогда живой агент считался брошенным, а его активные
     * задачи массово переназначались с дублированием передач. Свежая локальная запись
     * имеет приоритет (БД отстает на интервал write-behind), иначе решение принимается
     * по времени heartbeat из БД.
     */
    public boolean isAgentLive(String agentId) {
        if (agentId == null) return false;
        if (isLive(agents.get(agentId))) {
            return true;
        }
        return isLiveInDatabase(agentId);
    }

    private boolean isLiveInDatabase(String agentId) {
        if (agentRepository == null) {
            return false;
        }
        try {
            return agentRepository.findById(agentId)
                    .map(AgentEntity::getLastHeartbeat)
                    .map(this::isHeartbeatFresh)
                    .orElse(false);
        } catch (Exception e) {
            log.warn("Не удалось прочитать heartbeat агента '{}' из БД: {}", agentId, e.getMessage());
            return false;
        }
    }

    private boolean isHeartbeatFresh(Instant lastHeartbeat) {
        if (lastHeartbeat == null) {
            return false;
        }
        long elapsed = Instant.now().getEpochSecond() - lastHeartbeat.getEpochSecond();
        return elapsed < properties.getAgentOfflineTimeoutSeconds();
    }

    private boolean isLive(AgentEntry agent) {
        if (agent == null || agent.getLastHeartbeat() == null) {
            return false;
        }
        long elapsed = Instant.now().getEpochSecond() - agent.getLastHeartbeat().getEpochSecond();
        return elapsed < properties.getAgentOfflineTimeoutSeconds();
    }

    /**
     * Периодическое обновление статусов в реестре и в БД.
     *
     * <p>Статус нужен интерфейсу и отчетам; горячие пути используют {@link #isLive(AgentEntry)}.
     */
    @org.springframework.scheduling.annotation.Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void scheduledHealthRefresh() {
        refreshHealth();
    }

    void refreshHealth() {
        Instant now = Instant.now();
        int suspectTimeout = properties.getAgentHeartbeatTimeoutSeconds();
        int offlineTimeout = properties.getAgentOfflineTimeoutSeconds();

        for (AgentEntry agent : agents.values()) {
            long elapsed = now.getEpochSecond() - agent.getLastHeartbeat().getEpochSecond();
            if (elapsed >= offlineTimeout) {
                agent.setStatus(AgentStatus.OFFLINE);
            } else if (elapsed >= suspectTimeout) {
                agent.setStatus(AgentStatus.SUSPECT);
            } else {
                agent.setStatus(AgentStatus.ONLINE);
            }
        }
    }

    private void persistAgent(AgentEntry entry) {
        if (agentRepository == null || entry == null) return;
        try {
            AgentEntity entity = toEntity(entry);
            agentRepository.save(entity);
        } catch (Exception e) {
            log.warn("Не удалось сохранить агента '{}' в БД: {}", entry.getAgentId(), e.getMessage());
        }
    }

    private void deleteAgentFromDb(String agentId) {
        if (agentRepository == null || agentId == null) return;
        try {
            agentRepository.deleteById(agentId);
        } catch (Exception e) {
            log.warn("Не удалось удалить агента '{}' из БД: {}", agentId, e.getMessage());
        }
    }

    private AgentEntity toEntity(AgentEntry entry) {
        AgentEntity entity = new AgentEntity();
        entity.setAgentId(entry.getAgentId());
        entity.setClusterId(entry.getClusterId());
        entity.setHost(entry.getHost());
        entity.setGrpcPort(entry.getGrpcPort());
        entity.setGrpcAddress(entry.getGrpcAddress());
        entity.setMode(entry.getMode());
        entity.setActiveTransfers(entry.getActiveTransfers());
        entity.setMaxBandwidthMbS(entry.getMaxBandwidthMbS());
        entity.setStatus(entry.getStatus() != null ? entry.getStatus().name() : "ONLINE");
        entity.setLastHeartbeat(entry.getLastHeartbeat());
        entity.setTransferredBytes(entry.getTransferredBytes());
        entity.setTransferErrors(entry.getTransferErrors());
        entity.setCompressionRawBytes(entry.getCompressionRawBytes());
        entity.setCompressionCompressedBytes(entry.getCompressionCompressedBytes());
        entity.setCompressionRatio(entry.getCompressionRatio());
        entity.setChunkLatencyAvgMs(entry.getChunkLatencyAvgMs());
        entity.setChunkLatencyMaxMs(entry.getChunkLatencyMaxMs());
        entity.setChunkTransferCount(entry.getChunkTransferCount());
        entity.setStagingActiveFiles(entry.getStagingActiveFiles());
        entity.setStagingActiveBytes(entry.getStagingActiveBytes());
        entity.setStagingOrphansCleaned(entry.getStagingOrphansCleaned());
        entity.setUpdatedAt(Instant.now());
        return entity;
    }

    private AgentEntry fromEntity(AgentEntity entity) {
        AgentEntry entry = new AgentEntry(
                entity.getAgentId(),
                entity.getClusterId(),
                entity.getHost(),
                entity.getGrpcPort(),
                entity.getGrpcAddress(),
                entity.getMode(),
                entity.getMaxBandwidthMbS()
        );
        entry.setActiveTransfers(entity.getActiveTransfers());
        if (entity.getStatus() != null) {
            try {
                entry.setStatus(AgentStatus.valueOf(entity.getStatus()));
            } catch (IllegalArgumentException ignored) {
                entry.setStatus(AgentStatus.ONLINE);
            }
        }
        if (entity.getLastHeartbeat() != null) {
            entry.setLastHeartbeat(entity.getLastHeartbeat());
        }
        entry.setTransferredBytes(entity.getTransferredBytes());
        entry.setTransferErrors(entity.getTransferErrors());
        entry.setCompressionRawBytes(entity.getCompressionRawBytes());
        entry.setCompressionCompressedBytes(entity.getCompressionCompressedBytes());
        entry.setCompressionRatio(entity.getCompressionRatio());
        entry.setChunkLatencyAvgMs(entity.getChunkLatencyAvgMs());
        entry.setChunkLatencyMaxMs(entity.getChunkLatencyMaxMs());
        entry.setChunkTransferCount(entity.getChunkTransferCount());
        entry.setStagingActiveFiles(entity.getStagingActiveFiles());
        entry.setStagingActiveBytes(entity.getStagingActiveBytes());
        entry.setStagingOrphansCleaned(entity.getStagingOrphansCleaned());
        return entry;
    }
}
