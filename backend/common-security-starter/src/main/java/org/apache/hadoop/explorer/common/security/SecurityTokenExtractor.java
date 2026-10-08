package org.apache.hadoop.explorer.common.security;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;

import java.util.List;

/**
 * Извлекает токен авторизации из заголовка Authorization: Bearer либо из Cookie.
 */
public class SecurityTokenExtractor {

    private final CommonSecurityProperties properties;

    public SecurityTokenExtractor(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    public record ExtractedToken(String token, boolean isCookieAuth, String cookieName) {}

    public ExtractedToken extract(HttpServletRequest request) {
        if (request == null) {
            return new ExtractedToken(null, false, null);
        }

        // 1. Authorization: Bearer <token>
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String token = authHeader.substring(7).trim();
            if (!token.isEmpty()) {
                return new ExtractedToken(token, false, null);
            }
        }

        // 2. Cookie extraction
        Cookie[] cookies = request.getCookies();
        if (cookies != null && cookies.length > 0) {
            List<String> cookieNames = properties.getCookie().getNames();
            for (String configuredName : cookieNames) {
                for (Cookie cookie : cookies) {
                    if (configuredName.equalsIgnoreCase(cookie.getName())) {
                        String value = cookie.getValue();
                        if (value != null && !value.isBlank()) {
                            return new ExtractedToken(value.trim(), true, cookie.getName());
                        }
                    }
                }
            }
        }

        return new ExtractedToken(null, false, null);
    }
}
