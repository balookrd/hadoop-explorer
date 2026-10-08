package org.apache.hadoop.explorer.common.model;

/**
 * Роли пользователей в платформе Hadoop Explorer.
 */
public enum Role {
    READER("reader"),
    WRITER("writer"),
    ADMIN("admin");

    private final String value;

    Role(String value) {
        this.value = value;
    }

    @com.fasterxml.jackson.annotation.JsonValue
    public String getValue() {
        return value;
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public static Role fromString(String text) {
        if (text == null) {
            return READER;
        }
        for (Role r : Role.values()) {
            if (r.value.equalsIgnoreCase(text) || r.name().equalsIgnoreCase(text)) {
                return r;
            }
        }
        return READER;
    }
}
