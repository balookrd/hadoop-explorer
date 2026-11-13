package io.github.balookrd.pristan.spark.controller;

import io.github.balookrd.pristan.spark.service.engine.livy.InvalidSessionRequestException;
import io.github.balookrd.pristan.spark.service.engine.livy.LivyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** Engine failures in synchronous calls: Livy timeouts are 504, other Livy errors 502, a rejected session request 400. */
@RestControllerAdvice
public class SparkEngineExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(SparkEngineExceptionHandler.class);

    @ExceptionHandler(LivyException.class)
    public ResponseEntity<Map<String, Object>> livyFailure(LivyException e) {
        // The message carries no credentials (LivyClient strips them), so it is safe for both the log and the client
        log.warn("Ошибка Livy (HTTP {}): {}", e.getStatus(), e.getMessage());
        // A Livy timeout stays 504; any other Livy status (401 included: it is the service's credential problem,
        // and a 401 here would log the user out in the UI) is reported as 502
        return body(e.getStatus() == 504 ? HttpStatus.GATEWAY_TIMEOUT : HttpStatus.BAD_GATEWAY, e.getMessage());
    }

    @ExceptionHandler(InvalidSessionRequestException.class)
    public ResponseEntity<Map<String, Object>> invalidRequest(InvalidSessionRequestException e) {
        return body(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    private static ResponseEntity<Map<String, Object>> body(HttpStatus status, String detail) {
        return ResponseEntity.status(status).body(Map.of("detail", detail, "status", status.value()));
    }
}
