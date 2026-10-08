package org.apache.hadoop.explorer.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record LoginRequest(
    @NotBlank(message = "Username cannot be blank")
    @JsonProperty("username")
    String username,

    @NotBlank(message = "Password cannot be blank")
    @JsonProperty("password")
    String password
) {}
