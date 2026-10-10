package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.model.ClusterDto;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.TopologyResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class TopologyController {

    private final TopologyRegistry topologyRegistry;
    private final TokenBucketThrottler throttler;
    private final ReplicatorProperties properties;

    @org.springframework.beans.factory.annotation.Autowired
    public TopologyController(TopologyRegistry topologyRegistry, TokenBucketThrottler throttler,
                              ReplicatorProperties properties) {
        this.topologyRegistry = topologyRegistry;
        this.throttler = throttler;
        this.properties = properties != null ? properties : new ReplicatorProperties();
    }

    public TopologyController(TopologyRegistry topologyRegistry, TokenBucketThrottler throttler) {
        this(topologyRegistry, throttler, new ReplicatorProperties());
    }

    @GetMapping("/api/v1/clusters")
    public ResponseEntity<List<ClusterDto>> getClusters() {
        return ResponseEntity.ok(topologyRegistry.getClusters());
    }

    @GetMapping("/api/v1/datacenters")
    public ResponseEntity<List<ReplicatorProperties.DatacenterConfig>> getDatacenters() {
        return ResponseEntity.ok(topologyRegistry.getDatacenters());
    }

    @GetMapping("/api/v1/topology")
    public ResponseEntity<TopologyResponse> getTopology() {
        double globalLimitBytes = throttler.getGlobalLimit();
        double globalLimitMb = Math.round((globalLimitBytes / (1024.0 * 1024.0)) * 100.0) / 100.0;
        boolean streamingEnabled = properties.getStreaming() != null && properties.getStreaming().isEnabled();
        TopologyResponse response = new TopologyResponse(
            topologyRegistry.getDatacenters(),
            topologyRegistry.getClusters(),
            globalLimitBytes,
            globalLimitMb,
            topologyRegistry.getDcLimitsList(),
            topologyRegistry.getHdfsLimitsList(),
            streamingEnabled
        );
        return ResponseEntity.ok(response);
    }

    private void requireAdmin(org.springframework.security.core.Authentication auth) {
        if (auth instanceof org.apache.hadoop.explorer.common.security.CommonAuthenticationToken tokenAuth) {
            org.apache.hadoop.explorer.common.model.UserSession session = tokenAuth.getUserSession();
            if (session != null && session.isAdmin()) {
                return;
            }
        }
        throw new org.springframework.web.server.ResponseStatusException(
            org.springframework.http.HttpStatus.FORBIDDEN, "Только администратор платформы имеет право изменять сетевые лимиты топологии"
        );
    }

    @PostMapping("/api/v1/limits/global")
    public ResponseEntity<Map<String, Object>> setGlobalLimit(
        @RequestBody Map<String, Object> body,
        org.springframework.security.core.Authentication auth
    ) {
        requireAdmin(auth);
        Number limit = (Number) body.get("limit_bytes_per_sec");
        if (limit == null && body.containsKey("limit_mb_per_sec")) {
            Number mb = (Number) body.get("limit_mb_per_sec");
            if (mb != null) {
                limit = (long) (mb.doubleValue() * 1024 * 1024);
            }
        }
        if (limit != null) {
            throttler.setGlobalLimit(limit.longValue());
            return ResponseEntity.ok(Map.of("status", "updated", "limit_bytes_per_sec", limit.longValue()));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "limit_bytes_per_sec is required"));
    }

    @PostMapping("/api/v1/limits/dc-dc")
    public ResponseEntity<Map<String, Object>> setDcDcLimit(
        @RequestBody Map<String, Object> body,
        org.springframework.security.core.Authentication auth
    ) {
        requireAdmin(auth);
        String srcDc = (String) body.get("source_dc");
        String dstDc = (String) body.get("target_dc");
        Number limit = (Number) body.get("limit_bytes_per_sec");
        if (limit == null && body.containsKey("limit_mb_per_sec")) {
            Number mb = (Number) body.get("limit_mb_per_sec");
            if (mb != null) {
                limit = (long) (mb.doubleValue() * 1024 * 1024);
            }
        }

        if (srcDc != null && dstDc != null && limit != null) {
            topologyRegistry.setDcLimit(srcDc, dstDc, limit.longValue());
            return ResponseEntity.ok(Map.of("status", "updated", "channel", srcDc + "->" + dstDc));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "source_dc, target_dc and limit_bytes_per_sec/limit_mb_per_sec are required"));
    }

    @PostMapping("/api/v1/limits/hdfs-hdfs")
    public ResponseEntity<Map<String, Object>> setHdfsHdfsLimit(
        @RequestBody Map<String, Object> body,
        org.springframework.security.core.Authentication auth
    ) {
        requireAdmin(auth);
        String srcCluster = (String) body.get("source_cluster");
        String dstCluster = (String) body.get("target_cluster");
        Number limit = (Number) body.get("limit_bytes_per_sec");
        if (limit == null && body.containsKey("limit_mb_per_sec")) {
            Number mb = (Number) body.get("limit_mb_per_sec");
            if (mb != null) {
                limit = (long) (mb.doubleValue() * 1024 * 1024);
            }
        }

        if (srcCluster != null && dstCluster != null && limit != null) {
            topologyRegistry.setHdfsLimit(srcCluster, dstCluster, limit.longValue());
            return ResponseEntity.ok(Map.of("status", "updated", "channel", srcCluster + "->" + dstCluster));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "source_cluster, target_cluster and limit_bytes_per_sec/limit_mb_per_sec are required"));
    }
}
