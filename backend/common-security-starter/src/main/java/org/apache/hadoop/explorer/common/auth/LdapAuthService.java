package org.apache.hadoop.explorer.common.auth;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.RoleResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;

/**
 * Аутентификация через LDAP / Active Directory с защитой от LDAP Injection (CWE-90).
 */
public class LdapAuthService {

    private static final Logger log = LoggerFactory.getLogger(LdapAuthService.class);

    private final CommonSecurityProperties properties;
    private final RoleResolver roleResolver;

    public LdapAuthService(CommonSecurityProperties properties, RoleResolver roleResolver) {
        this.properties = properties;
        this.roleResolver = roleResolver;
    }

    /**
     * Экранирование спецсимволов LDAP фильтра (RFC 4515) для предотвращения LDAP Injection.
     */
    public static String escapeLdapFilter(String input) {
        if (input == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char c : input.toCharArray()) {
            switch (c) {
                case '\\' -> sb.append("\\5c");
                case '*'  -> sb.append("\\2a");
                case '('  -> sb.append("\\28");
                case ')'  -> sb.append("\\29");
                case '\0' -> sb.append("\\00");
                default   -> sb.append(c);
            }
        }
        return sb.toString();
    }

    public Optional<UserSession> authenticate(String username, String password) {
        var ldap = properties.getLdap();
        if (!ldap.isEnabled()) {
            log.warn("LDAP authentication requested but LDAP is not enabled in configuration");
            return Optional.empty();
        }

        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            return Optional.empty();
        }

        String safeUsername = escapeLdapFilter(username.trim());

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, ldap.getUrl());
        env.put(Context.SECURITY_AUTHENTICATION, "simple");

        // 1. Поиск пользователя через сервисный аккаунт (Manager DN) либо анонимно
        if (ldap.getManagerDn() != null && !ldap.getManagerDn().isBlank()) {
            env.put(Context.SECURITY_PRINCIPAL, ldap.getManagerDn());
            env.put(Context.SECURITY_CREDENTIALS, ldap.getManagerPassword() != null ? ldap.getManagerPassword() : "");
        } else {
            env.put(Context.SECURITY_AUTHENTICATION, "none");
        }

        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(env);

            SearchControls controls = new SearchControls();
            controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            controls.setReturningAttributes(new String[]{"cn", "displayName", "mail", "memberOf"});

            String searchFilter = ldap.getUserSearchFilter().replace("{0}", safeUsername);
            String searchBase = ldap.getUserSearchBase() != null ? ldap.getUserSearchBase() : ldap.getBaseDn();

            NamingEnumeration<SearchResult> results = ctx.search(searchBase, searchFilter, controls);
            if (!results.hasMore()) {
                log.warn("LDAP user not found: {}", username);
                return Optional.empty();
            }

            SearchResult userEntry = results.next();
            String userDn = userEntry.getNameInNamespace();
            Attributes attrs = userEntry.getAttributes();

            // 2. Проверка пароля путем bind от имени найденного пользователя
            Hashtable<String, String> userEnv = new Hashtable<>(env);
            userEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
            userEnv.put(Context.SECURITY_PRINCIPAL, userDn);
            userEnv.put(Context.SECURITY_CREDENTIALS, password);

            DirContext userCtx = null;
            try {
                userCtx = new InitialDirContext(userEnv);
            } finally {
                if (userCtx != null) userCtx.close();
            }

            // 3. Извлечение атрибутов и групп
            String displayName = getAttributeValue(attrs, "displayName")
                .orElse(getAttributeValue(attrs, "cn").orElse(username));
            String email = getAttributeValue(attrs, "mail").orElse(null);

            List<String> groups = new ArrayList<>();
            Attribute memberOf = attrs.get("memberOf");
            if (memberOf != null) {
                NamingEnumeration<?> allGroups = memberOf.getAll();
                while (allGroups.hasMore()) {
                    String groupDn = allGroups.next().toString();
                    String groupName = extractCn(groupDn);
                    groups.add(groupName);
                }
            }

            var resolved = roleResolver.resolve(username, groups);

            return Optional.of(new UserSession(
                username,
                displayName,
                email,
                groups,
                "ldap",
                resolved.isAdmin(),
                resolved.role()
            ));

        } catch (Exception e) {
            log.error("LDAP authentication failed for user: {} - {}", username, e.getMessage());
            return Optional.empty();
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception ignored) {}
            }
        }
    }

    private Optional<String> getAttributeValue(Attributes attrs, String attrId) {
        try {
            Attribute attr = attrs.get(attrId);
            if (attr != null && attr.get() != null) {
                return Optional.of(attr.get().toString());
            }
        } catch (Exception ignored) {}
        return Optional.empty();
    }

    private String extractCn(String dn) {
        if (dn == null) return "";
        for (String part : dn.split(",")) {
            String trimmed = part.trim();
            if (trimmed.toUpperCase().startsWith("CN=")) {
                return trimmed.substring(3).trim();
            }
        }
        return dn;
    }
}
