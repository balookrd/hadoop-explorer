package io.github.balookrd.pristan.replicator.orchestrator.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import io.github.balookrd.pristan.replicator.orchestrator.config.ReplicatorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;

/**
 * Lets Prometheus scrape GET /metrics with HTTP Basic credentials prometheus:&lt;metrics scrape token&gt;.
 * The credential is accepted on GET /metrics only and carries no role, so it cannot call the REST API.
 */
@Component
public class MetricsScrapeAuthenticationFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(MetricsScrapeAuthenticationFilter.class);

    public static final String SCRAPE_USERNAME = "prometheus";
    private static final String METRICS_PATH = "/metrics";
    private static final String BASIC_PREFIX = "Basic ";

    private final ReplicatorProperties properties;

    public MetricsScrapeAuthenticationFilter(ReplicatorProperties properties) {
        this.properties = properties;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !("GET".equals(request.getMethod()) && METRICS_PATH.equals(request.getRequestURI()));
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        String expectedToken = properties.getMetricsScrapeToken();
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);

        if (expectedToken != null && !expectedToken.isBlank()
                && header != null && header.regionMatches(true, 0, BASIC_PREFIX, 0, BASIC_PREFIX.length())
                && SecurityContextHolder.getContext().getAuthentication() == null) {
            if (matches(header.substring(BASIC_PREFIX.length()).trim(), expectedToken.trim())) {
                SecurityContextHolder.getContext().setAuthentication(
                        UsernamePasswordAuthenticationToken.authenticated(SCRAPE_USERNAME, null, List.of()));
            } else {
                // DEBUG, not WARN: /metrics is outside RateLimitFilter, so an anonymous client could flood the log
                log.debug("[MetricsAuth] Rejected scrape credentials for {}", request.getRequestURI());
            }
        }

        filterChain.doFilter(request, response);
    }

    private static boolean matches(String encodedCredentials, String expectedToken) {
        byte[] actual;
        try {
            actual = Base64.getDecoder().decode(encodedCredentials);
        } catch (IllegalArgumentException e) {
            return false;
        }
        byte[] expected = (SCRAPE_USERNAME + ":" + expectedToken).getBytes(StandardCharsets.UTF_8);
        // Constant-time comparison, same as AgentSecretAuthenticationFilter
        return MessageDigest.isEqual(expected, actual);
    }
}
