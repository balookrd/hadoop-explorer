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

    private void requireAdmin(Authentication auth) {
        UserSession session = getSession(auth);
        if (session != null && (session.isAdmin() || session.systemRole() == org.apache.hadoop.explorer.common.model.Role.ADMIN)) {
            return;
        }
        throw new ResponseStatusException(
            HttpStatus.FORBIDDEN, "Управление разделом Disaster Recovery доступно только Администратору платформы (ADMIN)"
        );
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
        requireAdmin(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.emergencyStop(req, username));
    }

    @PostMapping({"/emergency-stop/rollback", "/emergency-stop/cancel", "/restore"})
    public ResponseEntity<DrActionResponse> rollbackEmergencyStop(
            @RequestBody(required = false) org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyRollbackRequest req,
            Authentication auth
    ) {
        requireAdmin(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        var request = req != null ? req : new org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyRollbackRequest("dc1", true, true, true);
        return ResponseEntity.ok(drService.rollbackEmergencyStop(request, username));
    }

    @PostMapping("/reverse")
    public ResponseEntity<DrActionResponse> reverseReplication(
            @Valid @RequestBody DrReverseRequest req,
            Authentication auth
    ) {
        requireAdmin(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.reverseReplication(req, username));
    }

    @PostMapping("/jobs/{jobId}/reverse")
    public ResponseEntity<DrActionResponse> reverseSingleJob(
            @PathVariable String jobId,
            Authentication auth
    ) {
        requireAdmin(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.reverseSingleHdfsJob(jobId, username));
    }

    @PostMapping("/jobs/{jobId}/undo-reverse")
    public ResponseEntity<DrActionResponse> undoReverseJob(
            @PathVariable String jobId,
            Authentication auth
    ) {
        requireAdmin(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.undoReverse(jobId, username));
    }

    @DeleteMapping("/jobs/{jobId}/reverse")
    public ResponseEntity<DrActionResponse> deleteReverseJob(
            @PathVariable String jobId,
            Authentication auth
    ) {
        requireAdmin(auth);
        UserSession session = getSession(auth);
        String username = session != null ? session.username() : "system_operator";
        return ResponseEntity.ok(drService.undoReverse(jobId, username));
    }
}
