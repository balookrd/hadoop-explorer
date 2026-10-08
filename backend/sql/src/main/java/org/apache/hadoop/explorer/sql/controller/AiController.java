package org.apache.hadoop.explorer.sql.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.*;
import org.apache.hadoop.explorer.sql.service.AiService;
import org.apache.hadoop.explorer.sql.service.ClusterService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/v1/ai")
public class AiController {

    private final AiService aiService;
    private final ClusterService clusterService;

    public AiController(AiService aiService, ClusterService clusterService) {
        this.aiService = aiService;
        this.clusterService = clusterService;
    }

    private String resolveDialect(String clusterId, String defaultDialect) {
        if (clusterId != null && !clusterId.isBlank()) {
            var c = clusterService.findCluster(clusterId);
            if (c.isPresent()) {
                return c.get().getType();
            }
        }
        return (defaultDialect != null && !defaultDialect.isBlank()) ? defaultDialect : "trino";
    }

    @GetMapping("/status")
    public AIStatusResponse getStatus() {
        return aiService.getStatus();
    }

    @PostMapping("/check")
    public AICheckResponse checkSql(@Valid @RequestBody CheckSqlRequest request) {
        if (request.sql() == null || request.sql().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "SQL запрос не может быть пустым");
        }
        String dialect = resolveDialect(request.clusterId(), request.dialect());
        return aiService.checkQuery(request.sql(), dialect, request.catalogContext());
    }

    @PostMapping("/explain")
    public AIExplainResponse explainSql(@Valid @RequestBody ExplainSqlRequest request) {
        if (request.sql() == null || request.sql().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "SQL запрос не может быть пустым");
        }
        String dialect = resolveDialect(request.clusterId(), request.dialect());
        return aiService.explainQuery(request.sql(), dialect);
    }

    @PostMapping("/optimize")
    public AIOptimizeResponse optimizeSql(@Valid @RequestBody OptimizeSqlRequest request) {
        if (request.sql() == null || request.sql().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "SQL запрос не может быть пустым");
        }
        String dialect = resolveDialect(request.clusterId(), request.dialect());
        return aiService.optimizeQuery(request.sql(), dialect, request.catalogContext());
    }

    @PostMapping("/fix")
    public AIFixResponse fixSql(@Valid @RequestBody FixSqlRequest request) {
        if (request.sql() == null || request.sql().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "SQL запрос не может быть пустым");
        }
        String dialect = resolveDialect(request.clusterId(), request.dialect());
        return aiService.fixQuery(request.sql(), dialect, request.errorMessage());
    }

    @PostMapping("/format")
    public AIFormatResponse formatSql(@Valid @RequestBody FormatSqlRequest request) {
        if (request.sql() == null || request.sql().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "SQL запрос не может быть пустым");
        }
        String dialect = resolveDialect(request.clusterId(), request.dialect());
        return aiService.formatSql(request.sql(), dialect);
    }

    @PostMapping("/generate")
    public AIGenerateResponse generateSql(@Valid @RequestBody GenerateSqlRequest request) {
        if (request.prompt() == null || request.prompt().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Описание запроса не может быть пустым");
        }
        String dialect = resolveDialect(request.clusterId(), request.dialect());
        return aiService.generateQuery(request.prompt(), dialect, request.catalogContext());
    }
}
