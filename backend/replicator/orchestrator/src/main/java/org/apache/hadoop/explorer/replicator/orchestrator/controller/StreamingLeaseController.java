package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.StreamingLeaseRenewResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.service.StreamingLeaseCoordinator;
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

    @PostMapping("/renew")
    public ResponseEntity<StreamingLeaseRenewResponse> renewLease(@RequestBody StreamingLeaseRenewRequest req) {
        return ResponseEntity.ok(leaseCoordinator.renewLease(req));
    }

    @GetMapping("/status")
    public ResponseEntity<StreamingLeaseRenewResponse> getStatus(@RequestParam(defaultValue = "dc1") String clusterId) {
        return leaseCoordinator.getLeaseStatus(clusterId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/all")
    public ResponseEntity<Map<String, StreamingLeaseRenewResponse>> getAllStatuses() {
        return ResponseEntity.ok(leaseCoordinator.getAllLeaseStatuses());
    }
}
