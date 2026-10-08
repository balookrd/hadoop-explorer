package org.apache.hadoop.explorer.hdfs.exception;

import org.springframework.http.HttpStatus;

public class HdfsLocalizedException extends RuntimeException {

    private final HttpStatus status;
    private final String errorClass;

    public HdfsLocalizedException(String message, HttpStatus status, String errorClass) {
        super(message);
        this.status = status;
        this.errorClass = errorClass;
    }

    public HdfsLocalizedException(String message, HttpStatus status) {
        this(message, status, "HdfsError");
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getErrorClass() {
        return errorClass;
    }
}
