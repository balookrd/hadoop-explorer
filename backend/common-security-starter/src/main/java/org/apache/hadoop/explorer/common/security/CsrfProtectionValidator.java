package org.apache.hadoop.explorer.common.security;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Валидатор защиты от CSRF (CWE-352) для запросов, аутентифицированных через Cookie.
 */
public class CsrfProtectionValidator {

    private static final Logger log = LoggerFactory.getLogger(CsrfProtectionValidator.class);
    private static final Set<String> STATE_CHANGING_METHODS = Set.of("POST", "PUT", "DELETE", "PATCH");

    private final CommonSecurityProperties properties;

    public CsrfProtectionValidator(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    public boolean isValid(HttpServletRequest request, boolean isCookieAuth) {
        if (!isCookieAuth) {
            return true;
        }

        String method = request.getMethod();
        if (method == null || !STATE_CHANGING_METHODS.contains(method.toUpperCase())) {
            return true;
        }

        // 1. Проверка заголовка Sec-Fetch-Site
        String secFetchSite = request.getHeader("Sec-Fetch-Site");
        if (secFetchSite != null && "cross-site".equalsIgnoreCase(secFetchSite.trim())) {
            log.warn("CSRF rejected: Sec-Fetch-Site is cross-site for URI: {}", request.getRequestURI());
            return false;
        }

        // 2. Проверка X-Requested-With: XMLHttpRequest
        String xRequestedWith = request.getHeader("X-Requested-With");
        if ("XMLHttpRequest".equalsIgnoreCase(xRequestedWith)) {
            return true;
        }

        List<String> allowedOrigins = properties.getCors().getAllowedOrigins();

        // 3. Проверка заголовка Origin
        String origin = request.getHeader("Origin");
        if (origin != null && isAllowedOrigin(origin, request, allowedOrigins)) {
            return true;
        }

        // 4. Проверка заголовка Referer
        String referer = request.getHeader("Referer");
        if (referer != null && isAllowedOrigin(referer, request, allowedOrigins)) {
            return true;
        }

        log.warn("CSRF rejected: state-changing cookie request with missing or untrusted Origin/Referer (URI: {})",
            request.getRequestURI());
        return false;
    }

    private boolean isAllowedOrigin(String urlStr, HttpServletRequest request, List<String> allowedCors) {
        if (urlStr == null || urlStr.isBlank()) {
            return false;
        }
        try {
            URI uri = URI.create(urlStr.trim());
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (scheme == null || host == null) {
                return false;
            }

            int port = uri.getPort();
            String targetOrigin = scheme.toLowerCase() + "://" + host.toLowerCase() + (port != -1 ? ":" + port : "");

            for (String allowed : allowedCors) {
                if ("*".equals(allowed)) {
                    return true;
                }
                try {
                    URI allowedUri = URI.create(allowed.trim());
                    String aHost = allowedUri.getHost();
                    if (aHost != null) {
                        String aScheme = allowedUri.getScheme() != null ? allowedUri.getScheme().toLowerCase() : "http";
                        int aPort = allowedUri.getPort();
                        String normalizedAllowed = aScheme + "://" + aHost.toLowerCase() + (aPort != -1 ? ":" + aPort : "");
                        if (normalizedAllowed.equalsIgnoreCase(targetOrigin)) {
                            return true;
                        }
                    } else if (allowed.trim().equalsIgnoreCase(targetOrigin)) {
                        return true;
                    }
                } catch (Exception ignored) {
                }
            }

            // Проверяем совпадение с текущим сервером (Same-Origin)
            String serverScheme = request.getScheme() != null ? request.getScheme().toLowerCase() : "http";
            String serverName = request.getServerName() != null ? request.getServerName().toLowerCase() : "localhost";
            int serverPort = request.getServerPort();
            boolean isDefaultPort = ("http".equals(serverScheme) && serverPort == 80) ||
                                    ("https".equals(serverScheme) && serverPort == 443);
            String currentOrigin = serverScheme + "://" + serverName + (!isDefaultPort ? ":" + serverPort : "");

            return currentOrigin.equalsIgnoreCase(targetOrigin);
        } catch (Exception e) {
            return false;
        }
    }
}
