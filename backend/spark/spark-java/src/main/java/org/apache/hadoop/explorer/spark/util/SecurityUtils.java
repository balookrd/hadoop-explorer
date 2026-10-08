package org.apache.hadoop.explorer.spark.util;

import org.apache.hadoop.explorer.common.model.UserSession;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.util.List;

public final class SecurityUtils {

    private SecurityUtils() {}

    public static String getUsername(Authentication auth) {
        if (auth == null) return "anonymous";
        if (auth.getPrincipal() instanceof UserSession session) {
            return session.username();
        }
        return auth.getName();
    }

    public static List<String> getGroups(Authentication auth) {
        if (auth == null) return List.of();
        if (auth.getPrincipal() instanceof UserSession session) {
            return session.groups() != null ? session.groups() : List.of();
        }
        return auth.getAuthorities().stream().map(GrantedAuthority::getAuthority).toList();
    }

    public static boolean isAdmin(Authentication auth) {
        if (auth == null) return false;
        if (auth.getPrincipal() instanceof UserSession session) {
            return session.isAdmin();
        }
        return auth.getAuthorities().stream().anyMatch(a ->
                a.getAuthority().equalsIgnoreCase("ROLE_ADMIN") ||
                a.getAuthority().equalsIgnoreCase("admins") ||
                a.getAuthority().equalsIgnoreCase("admin")
        );
    }
}
