package org.apache.hadoop.explorer.yarn.util;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.util.List;

public final class SecurityUtils {

    private SecurityUtils() {}

    public static UserSession getUserSession(Authentication auth) {
        if (auth == null) {
            return new UserSession("anonymous", "Anonymous User", "anonymous@local", List.of(), "mock", false, Role.READER);
        }
        if (auth.getPrincipal() instanceof UserSession session) {
            return session;
        }
        String username = auth.getName();
        List<String> groups = auth.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .toList();

        return new UserSession(username, username, username + "@local", groups, "mock", false, Role.READER);
    }

    public static String getUsername(Authentication auth) {
        return getUserSession(auth).username();
    }

    public static List<String> getGroups(Authentication auth) {
        UserSession s = getUserSession(auth);
        return s.groups() != null ? s.groups() : List.of();
    }
}
