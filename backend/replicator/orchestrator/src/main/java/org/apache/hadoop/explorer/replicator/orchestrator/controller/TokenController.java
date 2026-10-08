package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.model.TokenRequest;
import org.apache.hadoop.explorer.replicator.model.TokenResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.throttler.TokenBucketThrottler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class TokenController {

    private final TokenBucketThrottler throttler;

    public TokenController(TokenBucketThrottler throttler) {
        this.throttler = throttler;
    }

    @PostMapping("/api/v1/tokens/request")
    public ResponseEntity<TokenResponse> requestTokens(@RequestBody TokenRequest req) {
        double waitSeconds = throttler.requestTokens(
            req.getRequestedBytes(),
            req.getSourceClusterId(),
            req.getTargetClusterId()
        );
        return ResponseEntity.ok(new TokenResponse(waitSeconds));
    }

    @GetMapping("/api/v1/tokens/limit")
    public ResponseEntity<Map<String, Object>> getLimit() {
        return ResponseEntity.ok(Map.of(
            "global_limit_bytes_per_sec", throttler.getGlobalLimit(),
            "global_limit_mb_s", Math.round((throttler.getGlobalLimit() / (1024.0 * 1024.0)) * 10.0) / 10.0
        ));
    }

    @PutMapping("/api/v1/tokens/limit")
    public ResponseEntity<Map<String, Object>> updateLimit(@RequestBody Map<String, Object> body) {
        Number limit = (Number) body.get("global_limit_bytes_per_sec");
        if (limit != null) {
            throttler.setGlobalLimit(limit.longValue());
            return ResponseEntity.ok(Map.of(
                "status", "updated",
                "global_limit_bytes_per_sec", limit.longValue()
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "global_limit_bytes_per_sec required"));
    }
}
