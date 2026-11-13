package io.github.balookrd.pristan.iskra.controller;

import jakarta.validation.Valid;
import io.github.balookrd.pristan.iskra.config.SparkProperties;
import io.github.balookrd.pristan.iskra.dto.CreateSessionRequest;
import io.github.balookrd.pristan.iskra.dto.SessionResponse;
import io.github.balookrd.pristan.iskra.service.ClusterService;
import io.github.balookrd.pristan.iskra.service.SparkSessionManager;
import io.github.balookrd.pristan.iskra.util.SecurityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/sessions")
public class SessionController {

    private final SparkSessionManager sessionManager;
    private final ClusterService clusterService;

    public SessionController(SparkSessionManager sessionManager, ClusterService clusterService) {
        this.sessionManager = sessionManager;
        this.clusterService = clusterService;
    }

    @GetMapping
    public List<SessionResponse> listSessions(Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        return sessionManager.getActiveSessions(username);
    }

    @PostMapping
    public SessionResponse createSession(
            @Valid @RequestBody CreateSessionRequest request,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        List<String> groups = SecurityUtils.getGroups(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);

        SparkProperties.SparkClusterConfig cluster = clusterService.findCluster(request.clusterId())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Кластер не найден: " + request.clusterId()));

        if (!clusterService.hasAccess(username, groups, isAdmin, cluster)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Доступ к кластеру запрещен");
        }

        return sessionManager.createSession(cluster, username, request);
    }

    @GetMapping("/{sessionId}")
    public SessionResponse getSession(
            @PathVariable("sessionId") String sessionId,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        return sessionManager.getSession(sessionId, username)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Сессия не найдена: " + sessionId));
    }

    @DeleteMapping("/{sessionId}")
    public Map<String, Object> deleteSession(
            @PathVariable("sessionId") String sessionId,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);
        boolean success = sessionManager.stopSession(sessionId, username, isAdmin);
        if (!success) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Сессия не найдена или нет прав на удаление");
        }
        return Map.of("status", "ok", "message", "Сессия остановлена");
    }
}
