package org.apache.hadoop.explorer.common.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hadoop.explorer.common.resilience.RateLimiterService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;

/**
 * Rejects API requests over the configured rate with HTTP 429 and {@code Retry-After}.
 *
 * <p>Login and SSO are keyed by client IP (LOGIN tier). Other API calls are keyed by the
 * authenticated user, anonymous ones by client IP (API tier). Registered after
 * springSecurityFilterChain, so the security context is already populated.
 */
public class RateLimitFilter extends OncePerRequestFilter {

    static final String LOGIN_PATH = "/api/v1/auth/login";
    static final String SSO_PATH = "/api/v1/auth/sso";

    private final RateLimiterService rateLimiter;
    private final ClientIpResolver clientIpResolver;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RateLimitFilter(RateLimiterService rateLimiter, ClientIpResolver clientIpResolver) {
        this.rateLimiter = rateLimiter;
        this.clientIpResolver = clientIpResolver;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
        throws ServletException, IOException {
        String path = pathWithinApplication(request);
        String clientIp = clientIpResolver.resolve(request);
        RateLimiterService.Decision decision = (LOGIN_PATH.equals(path) || SSO_PATH.equals(path))
            ? rateLimiter.tryConsume(RateLimiterService.Tier.LOGIN, "ip:" + clientIp)
            : rateLimiter.tryConsume(RateLimiterService.Tier.API, apiKey(clientIp));

        if (decision.allowed()) {
            filterChain.doFilter(request, response);
            return;
        }
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader(HttpHeaders.RETRY_AFTER, Long.toString(decision.retryAfterSeconds()));
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        objectMapper.writeValue(response.getOutputStream(), Map.of(
            "detail", "Слишком много запросов: повторите через " + decision.retryAfterSeconds() + " с",
            "status", 429
        ));
    }

    private static String apiKey(String clientIp) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.isAuthenticated() && !(auth instanceof AnonymousAuthenticationToken)) {
            return "user:" + auth.getName();
        }
        return "ip:" + clientIp;
    }

    private static String pathWithinApplication(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String contextPath = request.getContextPath();
        return contextPath != null && !contextPath.isEmpty() && uri.startsWith(contextPath)
            ? uri.substring(contextPath.length())
            : uri;
    }
}
