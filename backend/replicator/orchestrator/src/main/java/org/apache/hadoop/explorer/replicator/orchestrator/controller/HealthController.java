package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthController {

    private final Instant startTime = Instant.now();
    private final DataSource dataSource;

    public HealthController(@Autowired(required = false) DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @GetMapping({"/health", "/healthz", "/api/v1/health"})
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> body = new HashMap<>();
        body.put("service", "hadoop-grpc-replicator-orchestrator");
        body.put("uptime_seconds", Instant.now().getEpochSecond() - startTime.getEpochSecond());
        body.put("timestamp", Instant.now().toString());

        boolean dbOk = true;
        if (dataSource != null) {
            try (Connection conn = dataSource.getConnection()) {
                dbOk = conn.isValid(2);
            } catch (Exception e) {
                dbOk = false;
                body.put("db_error", e.getMessage());
            }
        }
        body.put("database", dbOk ? "UP" : "DOWN");

        if (dbOk) {
            body.put("status", "UP");
            return ResponseEntity.ok(body);
        } else {
            body.put("status", "DOWN");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
        }
    }
}
