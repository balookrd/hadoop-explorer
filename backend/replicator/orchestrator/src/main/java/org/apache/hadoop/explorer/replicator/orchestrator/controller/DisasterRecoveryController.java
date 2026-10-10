package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrActionResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrReverseRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.service.DisasterRecoveryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping({"/api/v1/dr", "/dr"})
public class DisasterRecoveryController {

    private final DisasterRecoveryService drService;

    public DisasterRecoveryController(DisasterRecoveryService drService) {
        this.drService = drService;
    }

    private UserSession getSession(Authentication auth) {
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            return tokenAuth.getUserSession();
        }
        return null;
    }

    private void checkWriteAccess(Authentication auth) {
        UserSession session = getSession(auth);
        if (session != null && session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER) {
            throw new ResponseStatusException(
                HttpStatus.FORBIDDEN, "Пользователь с ролью только для чтения (READER) не имеет права управлять аварийным переключением"
            );
        }
    }

    @GetMapping("/status")
    public ResponseEntity<DrStatusResponse> getDrStatus() {
        return ResponseEntity.ok(drService.getDrStatus());
    }

    @PostMapping("/emergency-stop")
    public ResponseEntity<DrActionResponse> emergencyStop(
            @Valid @RequestBody DrEmergencyStopRequest req,
            Authentication auth
    ) {
        checkWriteAccess(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.emergencyStop(req, username));
    }

    @PostMapping("/reverse")
    public ResponseEntity<DrActionResponse> reverseReplication(
            @Valid @RequestBody DrReverseRequest req,
            Authentication auth
    ) {
        checkWriteAccess(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.reverseReplication(req, username));
    }

    @PostMapping("/jobs/{jobId}/reverse")
    public ResponseEntity<DrActionResponse> reverseSingleJob(
            @PathVariable String jobId,
            Authentication auth
    ) {
        checkWriteAccess(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.reverseSingleHdfsJob(jobId, username));
    }
}
