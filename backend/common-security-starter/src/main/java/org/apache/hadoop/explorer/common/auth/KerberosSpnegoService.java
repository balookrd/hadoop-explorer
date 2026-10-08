package org.apache.hadoop.explorer.common.auth;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.RoleResolver;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Collections;
import java.util.Optional;

/**
 * Валидатор Kerberos SPNEGO билетов через нативный Java GSS-API (RFC 4559).
 */
public class KerberosSpnegoService {

    private static final Logger log = LoggerFactory.getLogger(KerberosSpnegoService.class);

    private final CommonSecurityProperties properties;
    private final RoleResolver roleResolver;

    public KerberosSpnegoService(CommonSecurityProperties properties, RoleResolver roleResolver) {
        this.properties = properties;
        this.roleResolver = roleResolver;
    }

    public Optional<UserSession> authenticate(HttpServletRequest request) {
        if (!properties.getKerberos().isEnabled()) {
            log.debug("Kerberos SPNEGO is disabled");
            return Optional.empty();
        }

        String authHeader = request.getHeader("Authorization");
        if (authHeader == null || !authHeader.regionMatches(true, 0, "Negotiate ", 0, 10)) {
            return Optional.empty();
        }

        String tokenStr = authHeader.substring(10).trim();
        if (tokenStr.isEmpty()) {
            return Optional.empty();
        }

        byte[] spnegoToken;
        try {
            spnegoToken = Base64.getDecoder().decode(tokenStr);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid Base64 SPNEGO token");
            return Optional.empty();
        }

        try {
            GSSManager manager = GSSManager.getInstance();

            // Создаем серверный контекст
            GSSContext context = manager.createContext((GSSCredential) null);
            byte[] outToken = context.acceptSecContext(spnegoToken, 0, spnegoToken.length);

            if (context.isEstablished()) {
                GSSName srcName = context.getSrcName();
                String fullPrincipal = srcName != null ? srcName.toString() : "unknown";
                log.info("Kerberos SPNEGO authenticated principal: {}", fullPrincipal);

                String username = fullPrincipal.contains("@") ? fullPrincipal.split("@")[0] : fullPrincipal;
                var resolved = roleResolver.resolve(username, Collections.emptyList());

                return Optional.of(new UserSession(
                    username,
                    username,
                    null,
                    Collections.emptyList(),
                    "kerberos",
                    resolved.isAdmin(),
                    resolved.role()
                ));
            }
        } catch (Exception e) {
            log.warn("Kerberos SPNEGO ticket validation failed: {}", e.getMessage());
        }

        return Optional.empty();
    }
}
