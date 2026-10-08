package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum QueueType {
    FIXED("fixed"),
    ELASTIC("elastic");

    private final String value;

    QueueType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
