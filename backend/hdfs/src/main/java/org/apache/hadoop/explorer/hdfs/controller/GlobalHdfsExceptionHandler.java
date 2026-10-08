package org.apache.hadoop.explorer.hdfs.controller;

import org.apache.hadoop.explorer.hdfs.exception.HdfsErrorTranslator;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalHdfsExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalHdfsExceptionHandler.class);

    @ExceptionHandler(HdfsLocalizedException.class)
    public ResponseEntity<Map<String, Object>> handleLocalizedException(HdfsLocalizedException ex) {
        log.warn("HDFS operation exception [{}]: {}", ex.getErrorClass(), ex.getMessage());
        return ResponseEntity.status(ex.getStatus()).body(Map.of(
            "detail", ex.getMessage(),
            "error_class", ex.getErrorClass(),
            "status", ex.getStatus().value()
        ));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericException(Exception ex) {
        log.error("Unhandled exception in HDFS Explorer", ex);
        var translated = HdfsErrorTranslator.translate(ex);
        return ResponseEntity.status(translated.status()).body(Map.of(
            "detail", translated.userFriendlyMessage(),
            "error_class", translated.errorClass(),
            "status", translated.status().value()
        ));
    }
}
