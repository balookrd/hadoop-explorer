package org.apache.hadoop.explorer.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record LoginResponse(
    @JsonProperty("success") boolean success,
    @JsonProperty("user") UserInfo user,
    @JsonProperty("message") String message
) {
    public static LoginResponse success(UserInfo user) {
        return new LoginResponse(true, user, "Login successful");
    }

    public static LoginResponse failure(String message) {
        return new LoginResponse(false, null, message);
    }
}
