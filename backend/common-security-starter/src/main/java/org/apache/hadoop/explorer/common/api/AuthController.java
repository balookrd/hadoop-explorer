package org.apache.hadoop.explorer.common.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import org.apache.hadoop.explorer.common.auth.AuthService;
import org.apache.hadoop.explorer.common.auth.UserLoggedOutEvent;
import org.apache.hadoop.explorer.common.model.LoginRequest;
import org.apache.hadoop.explorer.common.model.TokenResponse;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Стандартизированный REST-контроллер аутентификации для всех сервисов Hadoop Explorer.
 */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    private final AuthService authService;
    private final ApplicationEventPublisher eventPublisher;

    public AuthController(AuthService authService, ApplicationEventPublisher eventPublisher) {
        this.authService = authService;
        this.eventPublisher = eventPublisher;
    }

    @PostMapping("/login")
    public ResponseEntity<TokenResponse> login(
        @Valid @RequestBody LoginRequest request,
        HttpServletRequest httpRequest,
        HttpServletResponse httpResponse
    ) {
        TokenResponse response = authService.login(request, httpRequest, httpResponse);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/sso")
    public ResponseEntity<TokenResponse> sso(
        HttpServletRequest httpRequest,
        HttpServletResponse httpResponse
    ) {
        TokenResponse response = authService.sso(httpRequest, httpResponse);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/logout")
    public ResponseEntity<Map<String, Object>> logout(
        HttpServletRequest httpRequest,
        HttpServletResponse httpResponse
    ) {
        // Resolve the user before the token is revoked
        String username = authService.getCurrentUser(httpRequest).map(UserSession::username).orElse(null);
        authService.logout(httpRequest, httpResponse);
        if (username != null) {
            // Local publish: with a shared jdbc session store only this instance sees the event
            try {
                eventPublisher.publishEvent(new UserLoggedOutEvent(username));
            } catch (RuntimeException e) {
                // The token is already revoked: a failing listener must not turn logout into a 500
                log.warn("UserLoggedOutEvent listener failed for user '{}': {}", username, e.toString());
                log.debug("UserLoggedOutEvent listener failure", e);
            }
        }
        return ResponseEntity.ok(Map.of("success", true, "message", "Сессия завершена"));
    }

    @GetMapping("/me")
    public ResponseEntity<?> getCurrentUser(HttpServletRequest httpRequest) {
        return authService.getCurrentUser(httpRequest)
            .<ResponseEntity<?>>map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.of("detail", "Не авторизован", "status", 401)));
    }
}
