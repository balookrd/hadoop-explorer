package org.apache.hadoop.explorer.common.security;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.Role;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Централизованный резолвер системных ролей пользователей платформы Hadoop Explorer.
 */
public class RoleResolver {

    public static final Set<String> DEFAULT_ADMIN_GROUPS = Set.of(
        "hadoop-admins",
        "admins",
        "data-platform-admins",
        "platform-admins",
        "superusers"
    );

    public static final Set<String> DEFAULT_WRITER_GROUPS = Set.of(
        "data-engineers",
        "engineers",
        "spark-users",
        "etl-developers",
        "yarn-operators",
        "operators",
        "replicator-operators",
        "dev-leads"
    );

    private final CommonSecurityProperties properties;

    public RoleResolver(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    public record ResolvedRole(Role role, boolean isAdmin) {}

    public ResolvedRole resolve(String username, List<String> groups) {
        String uname = (username != null ? username : "").toLowerCase();
        Set<String> userGroups = new HashSet<>();
        if (groups != null) {
            for (String g : groups) {
                if (g != null) {
                    userGroups.add(g.toLowerCase());
                }
            }
        }

        Set<String> adminGroups = new HashSet<>(DEFAULT_ADMIN_GROUPS);
        Set<String> adminUsers = new HashSet<>();
        Set<String> writerGroups = new HashSet<>(DEFAULT_WRITER_GROUPS);
        Set<String> writerUsers = new HashSet<>();

        if (properties != null && properties.getAuth() != null) {
            var auth = properties.getAuth();
            if (auth.getAdminGroups() != null) {
                auth.getAdminGroups().forEach(g -> adminGroups.add(g.toLowerCase()));
            }
            if (auth.getAdminUsers() != null) {
                auth.getAdminUsers().forEach(u -> adminUsers.add(u.toLowerCase()));
            }
            if (auth.getWriterGroups() != null) {
                auth.getWriterGroups().forEach(g -> writerGroups.add(g.toLowerCase()));
            }
            if (auth.getWriterUsers() != null) {
                auth.getWriterUsers().forEach(u -> writerUsers.add(u.toLowerCase()));
            }
        }

        // 1. Проверка Администратора (ADM)
        boolean hasAdminGroup = !Collections.disjoint(userGroups, adminGroups);
        if (adminUsers.contains(uname) || hasAdminGroup || uname.startsWith("admin") || uname.equals("root")) {
            return new ResolvedRole(Role.ADMIN, true);
        }

        // 2. Проверка Писателя (Writer / RW)
        boolean hasWriterGroup = !Collections.disjoint(userGroups, writerGroups);
        if (writerUsers.contains(uname)
            || hasWriterGroup
            || uname.startsWith("writer")
            || uname.startsWith("engineer")
            || uname.equals("de_user")
            || uname.startsWith("de_")
            || uname.startsWith("data_engineer")
            || uname.startsWith("operator")) {
            return new ResolvedRole(Role.WRITER, false);
        }

        // 3. Читатель (Reader / RO) по умолчанию
        return new ResolvedRole(Role.READER, false);
    }
}
