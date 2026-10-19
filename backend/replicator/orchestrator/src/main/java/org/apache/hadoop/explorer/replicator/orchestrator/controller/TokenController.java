package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestController
public class TokenController {

    private final TokenBucketThrottler throttler;
    private final org.apache.hadoop.explorer.replicator.orchestrator.metrics.ReplicatorMetricsService metricsService;
    private final org.apache.hadoop.explorer.replicator.orchestrator.service.OrchestratorClusterCoordinator coordinator;

    public TokenController(TokenBucketThrottler throttler,
                           org.apache.hadoop.explorer.replicator.orchestrator.metrics.ReplicatorMetricsService metricsService) {
        this(throttler, metricsService, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public TokenController(
            TokenBucketThrottler throttler,
            org.apache.hadoop.explorer.replicator.orchestrator.metrics.ReplicatorMetricsService metricsService,
            @org.springframework.beans.factory.annotation.Autowired(required = false)
            org.apache.hadoop.explorer.replicator.orchestrator.service.OrchestratorClusterCoordinator coordinator
    ) {
        this.throttler = throttler;
        this.metricsService = metricsService;
        this.coordinator = coordinator;
    }

    private void requireAdmin(Authentication auth) {
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            UserSession session = tokenAuth.getUserSession();
            if (session != null && session.isAdmin()) {
                return;
            }
        }
        throw new ResponseStatusException(
            HttpStatus.FORBIDDEN,
            "Изменение глобального лимита пропускной способности доступно только Администратору (ADMIN)"
        );
    }

    @GetMapping("/api/v1/tokens/limit")
    public ResponseEntity<Map<String, Object>> getLimit() {
        double configured = throttler.getGlobalConfiguredLimit();
        double effective = throttler.getGlobalLimit();
        return ResponseEntity.ok(Map.of(
            "global_limit_bytes_per_sec", configured > 0 ? configured : effective,
            "global_limit_mb_s", Math.round(((configured > 0 ? configured : effective) / (1024.0 * 1024.0)) * 10.0) / 10.0,
            "effective_limit_bytes_per_sec", effective,
            "active_orchestrator_nodes", throttler.getActiveNodeCount()
        ));
    }

    @PutMapping("/api/v1/tokens/limit")
    public ResponseEntity<Map<String, Object>> updateLimit(
        @RequestBody Map<String, Object> body,
        Authentication auth
    ) {
        requireAdmin(auth);
        Number limit = (Number) body.get("global_limit_bytes_per_sec");
        if (limit != null) {
            if (coordinator != null) {
                coordinator.persistAndApplyLimit("global", limit.longValue());
            } else {
                throttler.setGlobalLimit(limit.longValue());
            }
            return ResponseEntity.ok(Map.of(
                "status", "updated",
                "global_limit_bytes_per_sec", limit.longValue(),
                "active_orchestrator_nodes", throttler.getActiveNodeCount()
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "global_limit_bytes_per_sec required"));
    }
}
