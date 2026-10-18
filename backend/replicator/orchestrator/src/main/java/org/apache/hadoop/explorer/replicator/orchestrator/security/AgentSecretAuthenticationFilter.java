package org.apache.hadoop.explorer.replicator.orchestrator.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/**
 * Фильтр аутентификации фоновых агентов репликации и внутренних сервисов платформы
 * по секретному токену в заголовке X-Agent-Secret.
 *
 * <p>Обеспечивает:
 * <ul>
 *   <li>Аутентификацию агентов при передаче сетевых запросов (Heartbeat, Claim Tasks, Complete Task и др.).</li>
 *   <li>Защиту от Timing Attack через constant-time сравнение хешей секретов (MessageDigest.isEqual).</li>
 *   <li>Наполнение Spring SecurityContext доверенной системной сессией агента с правами ADMIN / AGENT.</li>
 * </ul>
 */
@Component
public class AgentSecretAuthenticationFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(AgentSecretAuthenticationFilter.class);
    public static final String AGENT_SECRET_HEADER = "X-Agent-Secret";

    private final ReplicatorProperties properties;

    public AgentSecretAuthenticationFilter(ReplicatorProperties properties) {
        this.properties = properties;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        String secretHeader = request.getHeader(AGENT_SECRET_HEADER);
        String expectedSecret = properties.getAgentSecret();

        if (secretHeader != null && !secretHeader.isBlank()
                && expectedSecret != null && !expectedSecret.isBlank()) {

            byte[] expectedBytes = expectedSecret.trim().getBytes(StandardCharsets.UTF_8);
            byte[] actualBytes = secretHeader.trim().getBytes(StandardCharsets.UTF_8);

            // Защита от Timing Attack: постоянное время сравнения
            if (MessageDigest.isEqual(expectedBytes, actualBytes)) {
                if (SecurityContextHolder.getContext().getAuthentication() == null) {
                    UserSession agentSession = new UserSession(
                            "system_agent",
                            "Replicator Agent Daemon",
                            "agent@cluster.local",
                            List.of("replicator-agents"),
                            "agent-secret",
                            false,
                            Role.WRITER
                    );

                    CommonAuthenticationToken token = new CommonAuthenticationToken(agentSession);
                    SecurityContextHolder.getContext().setAuthentication(token);
                    log.debug("[AgentAuth] Успешная аутентификация агента по X-Agent-Secret для: {}", request.getRequestURI());
                }
            } else {
                log.warn("[AgentAuth Security Alert] Попытка обращения с неверным X-Agent-Secret токеном для: {}", request.getRequestURI());
            }
        }

        filterChain.doFilter(request, response);
    }
}
