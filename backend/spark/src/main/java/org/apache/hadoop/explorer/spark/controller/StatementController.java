package org.apache.hadoop.explorer.spark.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.spark.dto.ExecuteCodeRequest;
import org.apache.hadoop.explorer.spark.dto.ExecuteCodeResponse;
import org.apache.hadoop.explorer.spark.dto.StatementResultResponse;
import org.apache.hadoop.explorer.spark.service.SparkSessionManager;
import org.apache.hadoop.explorer.spark.util.SecurityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/statements")
public class StatementController {

    private final SparkSessionManager sessionManager;

    public StatementController(SparkSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @PostMapping("/execute")
    public ExecuteCodeResponse executeCode(
            @Valid @RequestBody ExecuteCodeRequest request,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        try {
            String executionId = sessionManager.submitCode(request.sessionId(), request.code(), request.language(), username);
            return new ExecuteCodeResponse(executionId, "QUEUED", "Задача отправлена на исполнение в Spark");
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        } catch (SecurityException e) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, e.getMessage());
        }
    }

    @PostMapping("/{executionId}/cancel")
    public Map<String, String> cancelExecution(
            @PathVariable("executionId") String executionId,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);
        boolean success = sessionManager.cancelExecution(executionId, username, isAdmin);
        if (!success) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Задача не найдена");
        }
        return Map.of("status", "CANCELLED", "message", "Выполнение успешно остановлено");
    }

    @GetMapping("/{executionId}/stream")
    public SseEmitter streamExecution(
            @PathVariable("executionId") String executionId,
            Authentication authentication
    ) {
        return sessionManager.registerStream(executionId);
    }

    @GetMapping("/{executionId}/result")
    public StatementResultResponse getResult(
            @PathVariable("executionId") String executionId,
            @RequestParam(name = "offset", defaultValue = "0") int offset,
            @RequestParam(name = "limit", defaultValue = "500") int limit,
            Authentication authentication
    ) {
        return sessionManager.getResult(executionId, offset, limit)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Результат не найден: " + executionId));
    }
}
