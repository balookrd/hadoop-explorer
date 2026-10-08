package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;

public record FileActionResponse(
    @JsonProperty("success") boolean success,
    @JsonProperty("message") String message,
    @JsonProperty("path") String path
) {
    public static FileActionResponse ok(String message, String path) {
        return new FileActionResponse(true, message, path);
    }

    public static FileActionResponse ok(String message) {
        return new FileActionResponse(true, message, null);
    }

    public static FileActionResponse fail(String message) {
        return new FileActionResponse(false, message, null);
    }
}
