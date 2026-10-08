package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

@RestController
public class HealthController {

    private final Instant startTime = Instant.now();

    @GetMapping({"/health", "/healthz"})
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
            "status", "UP",
            "service", "hadoop-grpc-replicator-orchestrator",
            "uptime_seconds", Instant.now().getEpochSecond() - startTime.getEpochSecond(),
            "timestamp", Instant.now().toString()
        ));
    }
}
