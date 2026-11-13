package io.github.balookrd.pristan.tolmach.controller;

import io.github.balookrd.pristan.tolmach.service.engine.SqlEngineException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;
import java.util.Objects;

/** Catalog calls (/api/v1/catalog/**) reach the engine synchronously; its failure is an upstream error. */
@RestControllerAdvice
public class SqlEngineExceptionHandler {

    @ExceptionHandler(SqlEngineException.class)
    public ResponseEntity<Map<String, Object>> handle(SqlEngineException e) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(Map.of("detail", Objects.requireNonNullElse(e.getMessage(), "Ошибка SQL-движка"),
                        "status", HttpStatus.BAD_GATEWAY.value()));
    }
}
