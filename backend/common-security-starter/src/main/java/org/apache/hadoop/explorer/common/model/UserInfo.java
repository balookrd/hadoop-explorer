package org.apache.hadoop.explorer.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

/**
 * Публичная информация о пользователе.
 */
public record UserInfo(
    @JsonProperty("username") String username,
    @JsonProperty("display_name") String displayName,
    @JsonProperty("email") String email,
    @JsonProperty("groups") List<String> groups,
    @JsonProperty("is_admin") boolean isAdmin
) {
    public UserInfo {
        if (groups == null) {
            groups = Collections.emptyList();
        }
    }
}
