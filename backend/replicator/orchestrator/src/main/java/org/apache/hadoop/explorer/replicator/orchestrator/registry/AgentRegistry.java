package org.apache.hadoop.explorer.replicator.orchestrator.registry;

import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
        private AgentStatus status;
        private Instant lastHeartbeat;

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
    }

    private final ReplicatorProperties properties;
    private final TopologyRegistry topologyRegistry;
    private final Map<String, AgentEntry> agents = new ConcurrentHashMap<>();

    public AgentRegistry(ReplicatorProperties properties, TopologyRegistry topologyRegistry) {
        this.properties = properties;
        this.topologyRegistry = topologyRegistry;
    }

    public void validateGrpcAddress(String address) {
        if (address == null || address.isBlank()) return;
        String trimmed = address.trim();
        String[] parts = trimmed.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("gRPC адрес должен быть в формате host:port (получено: " + address + ")");
        }
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Некорректный порт: " + parts[1]);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Порт вне диапазона 1-65535: " + port);
        }

        String hostLower = parts[0].toLowerCase();
        if (hostLower.startsWith("169.254.") || hostLower.startsWith("fe80:") || hostLower.contains("metadata")) {
            throw new SecurityException("SSRF защита: запрещен анонс облачных эндпоинтов метаданных (" + parts[0] + ")");
        }
    }

    public boolean register(AgentRegisterRequest request, String secretHeader) {
        verifySecret(secretHeader);
        validateGrpcAddress(request.getGrpcAddress());

        String agentId = request.getAgentId();
        String clusterId = request.getClusterId() != null ? request.getClusterId() : "default";

        String host = "localhost";
        int port = 50051;
        String grpcAddress = request.getGrpcAddress();
        if (grpcAddress != null && grpcAddress.contains(":")) {
            String[] parts = grpcAddress.split(":");
            host = parts[0];
            port = Integer.parseInt(parts[1]);
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

        AgentEntry entry = agents.get(agentId);
        if (entry == null) {
            String clusterId = request.getClusterId() != null ? request.getClusterId() : "default";
            String host = "localhost";
            int port = 50051;
            String grpcAddress = request.getGrpcAddress();
            if (grpcAddress != null && grpcAddress.contains(":")) {
                String[] parts = grpcAddress.split(":");
                host = parts[0];
                try {
                    port = Integer.parseInt(parts[1]);
                } catch (NumberFormatException ignored) {}
            } else {
                grpcAddress = host + ":" + port;
            }
            entry = new AgentEntry(agentId, clusterId, host, port, grpcAddress, "all", null);
            agents.put(agentId, entry);
            topologyRegistry.updateClusterGrpcAddress(clusterId, grpcAddress);
            log.info("Agent auto-registered via heartbeat: id={}, cluster={}, grpc={}", agentId, clusterId, grpcAddress);
        }

        entry.setLastHeartbeat(Instant.now());
        entry.setStatus(AgentStatus.ONLINE);
        entry.setActiveTransfers(request.getActiveTransfers());
        return true;
    }

    public boolean unregister(String agentId, String secretHeader) {
        verifySecret(secretHeader);
        AgentEntry entry = agents.remove(agentId);
        if (entry != null) {
            log.info("Agent unregistered: id={}", agentId);
            return true;
        }
        return false;
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
                a.getLastHeartbeat().toString()
            ));
        }
        return list;
    }

    public Optional<AgentEntry> getLiveAgentForCluster(String clusterId) {
        refreshHealth();
        return agents.values().stream()
            .filter(a -> a.getClusterId().equalsIgnoreCase(clusterId) && a.getStatus() == AgentStatus.ONLINE)
            .findAny();
    }

    public void verifySecret(String secretHeader) {
        String expected = properties.getAgentSecret();
        if (expected == null || expected.isBlank()) return;
        if (secretHeader == null || !expected.equals(secretHeader.trim())) {
            throw new SecurityException("Неверный X-Agent-Secret токен агента");
        }
    }

    private void refreshHealth() {
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
}
