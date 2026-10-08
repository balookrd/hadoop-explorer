package org.apache.hadoop.explorer.sql.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.*;
import org.apache.hadoop.explorer.sql.model.SavedQuery;
import org.apache.hadoop.explorer.sql.repository.SavedQueryRepository;
import org.apache.hadoop.explorer.sql.service.ClusterService;
import org.apache.hadoop.explorer.sql.service.QueryManagerService;
import org.apache.hadoop.explorer.sql.util.SecurityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/queries")
public class QueryController {

    private final QueryManagerService queryManager;
    private final ClusterService clusterService;
    private final SavedQueryRepository savedQueryRepository;

    public QueryController(
            QueryManagerService queryManager,
            ClusterService clusterService,
            SavedQueryRepository savedQueryRepository
    ) {
        this.queryManager = queryManager;
        this.clusterService = clusterService;
        this.savedQueryRepository = savedQueryRepository;
    }

    @PostMapping("/execute")
    public ExecuteQueryResponse executeQuery(
            @Valid @RequestBody ExecuteQueryRequest request,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        List<String> groups = SecurityUtils.getGroups(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);

        SqlProperties.ClusterConfig cluster = clusterService.findCluster(request.clusterId())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Указанный кластер не найден: " + request.clusterId()));

        if (!clusterService.hasAccess(username, groups, isAdmin, cluster)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Доступ к данному кластеру запрещен ACL");
        }

        String queryId = queryManager.submitQuery(cluster, username, request.query());
        return new ExecuteQueryResponse(queryId, "QUEUED", "Запрос поставлен в очередь на исполнение");
    }

    @GetMapping("/queue")
    public List<QueryHistoryItem> getQueue(Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        return queryManager.getUserQueue(username);
    }

    @DeleteMapping("/queue/{queryId}")
    public Map<String, String> removeFromQueue(
            @PathVariable("queryId") String queryId,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);
        boolean cancelled = queryManager.cancelQuery(queryId, username, isAdmin);
        if (!cancelled) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Запрос не найден в очереди или нет прав на удаление");
        }
        return Map.of("status", "ok", "message", "Запрос остановлен и удален из очереди");
    }

    @GetMapping("/{queryId}/result")
    public CachedResultResponse getResult(
            @PathVariable("queryId") String queryId,
            @RequestParam(name = "offset", defaultValue = "0") int offset,
            @RequestParam(name = "limit", defaultValue = "500") int limit,
            Authentication authentication
    ) {
        return queryManager.getCachedResult(queryId, offset, limit)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Результат запроса не сохранен или был очищен"));
    }

    @GetMapping("/{queryId}/stream")
    public SseEmitter streamQueryStatus(
            @PathVariable("queryId") String queryId,
            Authentication authentication
    ) {
        return queryManager.registerQueryStream(queryId);
    }

    @PostMapping("/{queryId}/cancel")
    public Map<String, String> cancelQuery(
            @PathVariable("queryId") String queryId,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);
        boolean success = queryManager.cancelQuery(queryId, username, isAdmin);
        if (!success) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Запрос не найден среди активных");
        }
        return Map.of("status", "ok", "message", "Сигнал отмены отправлен в движок");
    }

    @GetMapping("/notifications/stream")
    public SseEmitter streamNotifications(Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        return queryManager.registerUserStream(username);
    }

    @GetMapping("/history")
    public List<QueryHistoryItem> getHistory(
            @RequestParam(name = "limit", defaultValue = "50") int limit,
            @RequestParam(name = "offset", defaultValue = "0") int offset,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        return queryManager.getUserHistory(username, offset, limit);
    }

    @PostMapping("/saved")
    public SavedQuery saveQuery(
            @Valid @RequestBody SaveQueryRequest request,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        SavedQuery sq = new SavedQuery();
        sq.setId(UUID.randomUUID().toString());
        sq.setUsername(username);
        sq.setTitle(request.title());
        sq.setDescription(request.description());
        sq.setClusterId(request.clusterId());
        sq.setQueryText(request.queryText());
        sq.setShared(request.shared());
        sq.setCreatedAt(Instant.now());
        sq.setUpdatedAt(Instant.now());
        return savedQueryRepository.save(sq);
    }

    @GetMapping("/saved")
    public List<SavedQuery> listSavedQueries(Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        return savedQueryRepository.findVisibleQueries(username);
    }
}
