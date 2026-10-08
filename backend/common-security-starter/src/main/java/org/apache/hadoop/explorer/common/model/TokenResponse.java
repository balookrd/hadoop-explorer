package org.apache.hadoop.explorer.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TokenResponse(
    @JsonProperty("access_token") String accessToken,
    @JsonProperty("token_type") String tokenType,
    @JsonProperty("user") UserSession user,
    @JsonProperty("success") boolean success,
    @JsonProperty("message") String message
) {
    public static TokenResponse of(String accessToken, UserSession user) {
        return new TokenResponse(accessToken, "bearer", user, true, "Авторизация успешна");
    }

    public static TokenResponse failure(String message) {
        return new TokenResponse(null, "bearer", null, false, message);
    }
}
