package org.apache.hadoop.explorer.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

public record TokenPayload(
    @JsonProperty("sub") String sub,
    @JsonProperty("display_name") String displayName,
    @JsonProperty("email") String email,
    @JsonProperty("groups") List<String> groups,
    @JsonProperty("exp") long exp,
    @JsonProperty("iat") long iat,
    @JsonProperty("jti") String jti,
    @JsonProperty("auth_method") String authMethod,
    @JsonProperty("is_admin") boolean isAdmin,
    @JsonProperty("system_role") String systemRole
) {
    public TokenPayload {
        if (groups == null) {
            groups = Collections.emptyList();
        }
    }
}
