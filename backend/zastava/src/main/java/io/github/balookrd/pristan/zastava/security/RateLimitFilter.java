package io.github.balookrd.pristan.zastava.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import io.github.balookrd.pristan.zastava.resilience.RateLimiterService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.UrlPathHelper;

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

    private static final UrlPathHelper PATH_HELPER = new UrlPathHelper();

    private final RateLimiterService rateLimiter;
    private final ClientIpResolver clientIpResolver;
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** Null when nobody listens for blocked requests. */
    private final RateLimitBlockListener blockListener;

    public RateLimitFilter(RateLimiterService rateLimiter, ClientIpResolver clientIpResolver) {
        this(rateLimiter, clientIpResolver, null);
    }

    public RateLimitFilter(RateLimiterService rateLimiter, ClientIpResolver clientIpResolver,
                           RateLimitBlockListener blockListener) {
        this.rateLimiter = rateLimiter;
        this.clientIpResolver = clientIpResolver;
        this.blockListener = blockListener;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
        throws ServletException, IOException {
        String path = pathWithinApplication(request);
        String clientIp = clientIpResolver.resolve(request);
        RateLimiterService.Tier tier = (LOGIN_PATH.equals(path) || SSO_PATH.equals(path))
            ? RateLimiterService.Tier.LOGIN
            : RateLimiterService.Tier.API;
        RateLimiterService.Decision decision = rateLimiter.tryConsume(
            tier, tier == RateLimiterService.Tier.LOGIN ? "ip:" + clientIp : apiKey(clientIp));

        if (decision.allowed()) {
            filterChain.doFilter(request, response);
            return;
        }
        if (blockListener != null) {
            blockListener.onBlock(tier);
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
        // Decoded, so that /api/v1/auth/l%6Fgin (served by the login handler) is rate limited as login.
        return PATH_HELPER.getPathWithinApplication(request);
    }
}
