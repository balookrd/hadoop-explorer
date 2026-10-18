package org.apache.hadoop.explorer.common.auth;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.RoleResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Провайдер mock-аутентификации для разработки и демо-стендов.
 * Категорически блокируется при debug: false.
 */
public class MockAuthService {

    private static final Logger log = LoggerFactory.getLogger(MockAuthService.class);

    private final CommonSecurityProperties properties;
    private final RoleResolver roleResolver;

    public MockAuthService(CommonSecurityProperties properties, RoleResolver roleResolver) {
        this.properties = properties;
        this.roleResolver = roleResolver;
    }

    public Optional<UserSession> authenticate(String username, String password) {
        if (!"mock".equalsIgnoreCase(properties.getAuth().getMode()) && !properties.isDebug()) {
            throw new SecurityException("Mock authentication is forbidden in non-debug mode (auth.mode="
                    + properties.getAuth().getMode() + ", debug=false)");
        }

        if (username == null || username.isBlank()) {
            return Optional.empty();
        }

        String uname = username.trim();
        List<CommonSecurityProperties.MockUser> configured = properties.getMockUsers();

        // 1. Проверка явно заданных пользователей в конфигурации
        if (configured != null) {
            for (CommonSecurityProperties.MockUser mu : configured) {
                if (mu.getUsername() != null && mu.getUsername().equalsIgnoreCase(uname)) {
                    if (mu.getPassword() != null && !mu.getPassword().equals(password)) {
                        return Optional.empty();
                    }
                    List<String> userGroups = mu.getGroups() != null ? new ArrayList<>(mu.getGroups()) : new ArrayList<>();
                    var resolved = roleResolver.resolve(uname, userGroups);
                    String dName = mu.getDisplayName() != null ? mu.getDisplayName() : (uname.substring(0, 1).toUpperCase() + uname.substring(1));
                    String mail = mu.getEmail() != null ? mu.getEmail() : (uname + "@example.com");
                    return Optional.of(new UserSession(
                        uname,
                        dName,
                        mail,
                        userGroups,
                        "mock",
                        resolved.isAdmin(),
                        resolved.role()
                    ));
                }
            }
        }

        // 2. Дефолтный fallback для стандартных учетных записей демо-режима разработки
        List<String> mockGroups = new ArrayList<>();
        String lower = uname.toLowerCase();
        if ("admin".equals(lower) || "root".equals(lower)) {
            mockGroups.addAll(RoleResolver.DEFAULT_ADMIN_GROUPS);
        } else if ("writer".equals(lower)) {
            mockGroups.addAll(RoleResolver.DEFAULT_WRITER_GROUPS);
        } else {
            mockGroups.add("users");
        }
        var resolved = roleResolver.resolve(uname, mockGroups);

        return Optional.of(new UserSession(
            uname,
            uname.substring(0, 1).toUpperCase() + uname.substring(1),
            uname + "@example.com",
            mockGroups,
            "mock",
            resolved.isAdmin(),
            resolved.role()
        ));
    }
}
