package io.github.balookrd.pristan.replicator.orchestrator.controller;

import io.github.balookrd.pristan.replicator.orchestrator.dto.StreamingLeaseRenewResponse;
import io.github.balookrd.pristan.replicator.orchestrator.service.StreamingLeaseCoordinator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/streaming/lease")
public class StreamingLeaseController {

    private final StreamingLeaseCoordinator leaseCoordinator;

    public StreamingLeaseController(StreamingLeaseCoordinator leaseCoordinator) {
        this.leaseCoordinator = leaseCoordinator;
    }

    @GetMapping("/status")
    public ResponseEntity<StreamingLeaseRenewResponse> getStatus(@RequestParam String clusterId) {
        return leaseCoordinator.getLeaseStatus(clusterId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/all")
    public ResponseEntity<Map<String, StreamingLeaseRenewResponse>> getAllStatuses() {
        return ResponseEntity.ok(leaseCoordinator.getAllLeaseStatuses());
    }
}
