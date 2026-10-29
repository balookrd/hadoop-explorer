package org.apache.hadoop.explorer.yarn.controller;

import org.apache.hadoop.explorer.yarn.service.YarnRmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Returns the reason of a rejected request as {@code {"detail": ..., "status": ...}}, the shape the
 * UI reads. Spring's default error body drops the reason, so the four-eyes rule or a validation
 * error used to reach the user as a bare "Forbidden" / "Bad Request".
 */
@RestControllerAdvice(basePackages = "org.apache.hadoop.explorer.yarn")
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> handleStatus(ResponseStatusException e) {
        String reason = e.getReason() != null ? e.getReason() : e.getStatusCode().toString();
        return body(HttpStatus.valueOf(e.getStatusCode().value()), reason);
    }

    @ExceptionHandler(YarnRmException.class)
    public ResponseEntity<Map<String, Object>> handleRm(YarnRmException e) {
        log.error("ResourceManager error: {}", e.getMessage());
        return body(HttpStatus.BAD_GATEWAY, e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException e) {
        String details = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return body(HttpStatus.BAD_REQUEST, "Некорректный запрос: " + details);
    }

    private static ResponseEntity<Map<String, Object>> body(HttpStatus status, String detail) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", status.value());
        m.put("detail", detail);
        return ResponseEntity.status(status).body(m);
    }
}
