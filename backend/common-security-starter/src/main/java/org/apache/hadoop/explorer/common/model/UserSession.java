package org.apache.hadoop.explorer.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Сессия аутентифицированного пользователя платформы.
 */
public record UserSession(
    @JsonProperty("username") String username,
    @JsonProperty("display_name") String displayName,
    @JsonProperty("email") String email,
    @JsonProperty("groups") List<String> groups,
    @JsonProperty("auth_method") String authMethod,
    @JsonProperty("is_admin") boolean isAdmin,
    @JsonProperty("system_role") Role systemRole
) implements Serializable {

    public UserSession {
        if (groups == null) {
            groups = Collections.emptyList();
        }
        if (displayName == null || displayName.isBlank()) {
            displayName = username;
        }
        if (authMethod == null) {
            authMethod = "ldap";
        }
        if (systemRole == null) {
            systemRole = isAdmin ? Role.ADMIN : Role.READER;
        }
    }

    public UserInfo toUserInfo() {
        return new UserInfo(username, displayName, email, groups, isAdmin);
    }
}
