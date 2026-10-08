package org.apache.hadoop.explorer.common.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.TokenPayload;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.session.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Центральный фильтр безопасности:
 * 1. Извлечение токена (Header / Cookie)
 * 2. CSRF-проверка
 * 3. Проверка отзыва токена
 * 4. Загрузка сессии (L1/L2)
 * 5. Наполнение Spring SecurityContext
 */
public class CommonAuthFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(CommonAuthFilter.class);

    private final SecurityTokenExtractor tokenExtractor;
    private final CsrfProtectionValidator csrfValidator;
    private final JwtTokenService jwtTokenService;
    private final SessionStore sessionStore;
    private final RoleResolver roleResolver;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CommonAuthFilter(
        SecurityTokenExtractor tokenExtractor,
        CsrfProtectionValidator csrfValidator,
        JwtTokenService jwtTokenService,
        SessionStore sessionStore,
        RoleResolver roleResolver
    ) {
        this.tokenExtractor = tokenExtractor;
        this.csrfValidator = csrfValidator;
        this.jwtTokenService = jwtTokenService;
        this.sessionStore = sessionStore;
        this.roleResolver = roleResolver;
    }

    @Override
    protected void doFilterInternal(
        HttpServletRequest request,
        HttpServletResponse response,
        FilterChain filterChain
    ) throws ServletException, IOException {

        var extracted = tokenExtractor.extract(request);

        // 1. CSRF валидация для Cookie-аутентификации
        if (!csrfValidator.isValid(request, extracted.isCookieAuth())) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding("UTF-8");
            Map<String, Object> error = Map.of(
                "detail", "CSRF protection: запрос отклонен политикой безопасности источника",
                "status", 403
            );
            objectMapper.writeValue(response.getOutputStream(), error);
            return;
        }

        // 2. Если токен отсутствует, продолжаем цепочку (анонимный доступ для /login, /public и т.п.)
        if (extracted.token() == null) {
            filterChain.doFilter(request, response);
            return;
        }

        String token = extracted.token();

        // 3. Проверка отзыва токена
        if (sessionStore.isTokenRevoked(token)) {
            log.debug("Token is revoked: {}", JwtTokenService.hashToken(token));
            filterChain.doFilter(request, response);
            return;
        }

        // 4. Попытка получить сессию из SessionStore
        Optional<UserSession> sessionOpt = sessionStore.getSession(token);

        if (sessionOpt.isPresent()) {
            UserSession session = sessionOpt.get();
            setAuthentication(session);
            filterChain.doFilter(request, response);
            return;
        }

        // 5. Fallback: декодирование JWT токена
        Optional<TokenPayload> payloadOpt = jwtTokenService.parseAndVerifyToken(token);
        if (payloadOpt.isPresent()) {
            TokenPayload payload = payloadOpt.get();

            // Проверка JTI на отзыв
            if (payload.jti() != null && sessionStore.isTokenRevoked(payload.jti())) {
                log.debug("Token JTI is revoked: {}", payload.jti());
                filterChain.doFilter(request, response);
                return;
            }

            var resolved = roleResolver.resolve(payload.sub(), payload.groups());
            Role role = resolved.isAdmin() ? Role.ADMIN : (
                resolved.role() != Role.READER ? resolved.role() : Role.fromString(payload.systemRole())
            );

            UserSession newSession = new UserSession(
                payload.sub(),
                payload.displayName(),
                payload.email(),
                payload.groups(),
                payload.authMethod(),
                resolved.isAdmin(),
                role
            );

            // Кэшируем восстановленную сессию
            sessionStore.saveSession(
                token,
                newSession,
                Instant.ofEpochSecond(payload.exp()),
                payload.jti()
            );

            setAuthentication(newSession);
        }

        filterChain.doFilter(request, response);
    }

    private void setAuthentication(UserSession session) {
        CommonAuthenticationToken auth = new CommonAuthenticationToken(session);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}
