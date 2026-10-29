package org.apache.hadoop.explorer.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.Set;

/**
 * Fails startup on security settings that are unsafe outside development.
 *
 * <p>A misspelled key silently falls back to the defaults, and the defaults are mock
 * authentication (any password for well-known demo accounts) and a JWT secret that is
 * published in the source code. Refusing to start is the only way such a mistake is noticed
 * before production.
 */
public class SecurityConfigurationValidator implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfigurationValidator.class);

    // "hybrid" is used by the spark/sql stands: CompositeAuthService tries LDAP, Kerberos SSO stays available
    private static final Set<String> KNOWN_MODES = Set.of("mock", "ldap", "kerberos", "hybrid");

    private final CommonSecurityProperties properties;

    public SecurityConfigurationValidator(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        validate(properties);
    }

    public static void validate(CommonSecurityProperties properties) {
        String mode = properties.getAuth().getMode() == null ? "" : properties.getAuth().getMode().trim().toLowerCase();

        if (!KNOWN_MODES.contains(mode)) {
            throw new IllegalStateException("hadoop.security.auth.mode='" + properties.getAuth().getMode()
                    + "' is not supported; expected one of " + KNOWN_MODES);
        }

        if ("mock".equals(mode)) {
            if (!properties.isDebug()) {
                throw new IllegalStateException("hadoop.security.auth.mode=mock accepts demo accounts without a real "
                        + "identity check and is allowed only together with hadoop.security.debug=true. "
                        + "For production set hadoop.security.auth.mode to ldap or kerberos.");
            }
            log.warn("SECURITY: mock authentication is enabled (debug mode). Never use this configuration in production.");
            return;
        }

        String secret = properties.getJwt().getSecretKey();
        if (secret == null || secret.isBlank()
                || CommonSecurityProperties.JwtProperties.DEFAULT_SECRET_KEY.equals(secret)) {
            throw new IllegalStateException("hadoop.security.jwt.secret-key is not set: the built-in default is public "
                    + "and lets anyone forge session tokens. Provide a random secret of at least 32 characters.");
        }
        if (secret.length() < 32) {
            throw new IllegalStateException("hadoop.security.jwt.secret-key must be at least 32 characters long");
        }

        if ("ldap".equals(mode) && !properties.getLdap().isEnabled()) {
            throw new IllegalStateException("hadoop.security.auth.mode=ldap requires hadoop.security.ldap.enabled=true");
        }
    }
}
