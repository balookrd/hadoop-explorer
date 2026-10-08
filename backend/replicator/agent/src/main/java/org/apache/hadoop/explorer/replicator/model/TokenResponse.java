package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Ответ Оркестратора с рассчитанным временем задержки перед отправкой чанка.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenResponse {

    @JsonProperty("wait_seconds")
    private double waitSeconds;

    public TokenResponse() {}

    public TokenResponse(double waitSeconds) {
        this.waitSeconds = waitSeconds;
    }

    public double getWaitSeconds() { return waitSeconds; }
    public void setWaitSeconds(double waitSeconds) { this.waitSeconds = waitSeconds; }
}
