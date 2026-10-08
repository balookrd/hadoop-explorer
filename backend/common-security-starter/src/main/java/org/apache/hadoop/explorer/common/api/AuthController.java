package org.apache.hadoop.explorer.common.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import org.apache.hadoop.explorer.common.auth.AuthService;
import org.apache.hadoop.explorer.common.model.LoginRequest;
import org.apache.hadoop.explorer.common.model.TokenResponse;
import org.apache.hadoop.explorer.common.model.UserSession;
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

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
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
        authService.logout(httpRequest, httpResponse);
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
