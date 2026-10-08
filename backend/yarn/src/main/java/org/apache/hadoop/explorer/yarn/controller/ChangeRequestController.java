package org.apache.hadoop.explorer.yarn.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.common.audit.Audited;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.yarn.model.*;
import org.apache.hadoop.explorer.yarn.service.ChangeRequestService;
import org.apache.hadoop.explorer.yarn.util.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/change-requests")
public class ChangeRequestController {

    private static final Logger log = LoggerFactory.getLogger(ChangeRequestController.class);

    private final ChangeRequestService changeRequestService;

    public ChangeRequestController(ChangeRequestService changeRequestService) {
        this.changeRequestService = changeRequestService;
    }

    @GetMapping
    public ResponseEntity<List<ChangeRequestSummary>> listChangeRequests(
            @RequestParam(required = false) String clusterId,
            @RequestParam(required = false, name = "status") String status,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        List<ChangeRequestSummary> list = changeRequestService.listChangeRequests(clusterId, status, user);
        return ResponseEntity.ok(list);
    }

    @GetMapping("/pending-count")
    public ResponseEntity<Map<String, Long>> getPendingCount(
            @RequestParam(required = false) String clusterId,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        Map<String, Long> count = changeRequestService.getPendingCount(clusterId, user);
        return ResponseEntity.ok(count);
    }

    @PostMapping
    @Audited(action = "CREATE_CHANGE_REQUEST", resource = "YARN")
    public ResponseEntity<ChangeRequestResponse> createChangeRequest(
            @Valid @RequestBody ChangeRequestCreate request,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ChangeRequestResponse created = changeRequestService.createChangeRequest(request, user);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ChangeRequestResponse> getChangeRequest(
            @PathVariable Long id,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ChangeRequestResponse resp = changeRequestService.getChangeRequest(id, user);
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/{id}/approve")
    @Audited(action = "APPROVE_CHANGE_REQUEST", resource = "YARN")
    public ResponseEntity<ChangeRequestResponse> approveChangeRequest(
            @PathVariable Long id,
            @RequestBody(required = false) ChangeRequestReview review,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        String comment = (review != null && review.getComment() != null) ? review.getComment() : "";
        ChangeRequestResponse approved = changeRequestService.approveChangeRequest(id, comment, user);
        return ResponseEntity.ok(approved);
    }

    @PostMapping("/{id}/reject")
    @Audited(action = "REJECT_CHANGE_REQUEST", resource = "YARN")
    public ResponseEntity<ChangeRequestResponse> rejectChangeRequest(
            @PathVariable Long id,
            @RequestBody(required = false) ChangeRequestReview review,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        String comment = (review != null && review.getComment() != null) ? review.getComment() : "";
        ChangeRequestResponse rejected = changeRequestService.rejectChangeRequest(id, comment, user);
        return ResponseEntity.ok(rejected);
    }

    @PostMapping("/{id}/cancel")
    @Audited(action = "CANCEL_CHANGE_REQUEST", resource = "YARN")
    public ResponseEntity<ChangeRequestResponse> cancelChangeRequest(
            @PathVariable Long id,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ChangeRequestResponse cancelled = changeRequestService.cancelChangeRequest(id, user);
        return ResponseEntity.ok(cancelled);
    }

    @GetMapping("/{id}/xml")
    public ResponseEntity<Map<String, Object>> previewChangeRequestXml(
            @PathVariable Long id,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        Map<String, Object> preview = changeRequestService.previewChangeRequestXml(id, user);
        return ResponseEntity.ok(preview);
    }

    @PostMapping("/{id}/deploy")
    @Audited(action = "DEPLOY_CHANGE_REQUEST", resource = "YARN")
    public ResponseEntity<DeployResponse> deployChangeRequest(
            @PathVariable Long id,
            @RequestParam(defaultValue = "true") boolean wait,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        DeployResponse resp = changeRequestService.deployChangeRequest(id, wait, user);
        return ResponseEntity.ok(resp);
    }
}
