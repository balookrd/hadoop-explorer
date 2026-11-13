package io.github.balookrd.pristan.zastava.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import io.github.balookrd.pristan.zastava.model.LoginRequest;
import io.github.balookrd.pristan.zastava.model.TokenResponse;
import io.github.balookrd.pristan.zastava.model.UserSession;

import java.util.Optional;

/**
 * Интерфейс централизованной службы аутентификации.
 */
public interface AuthService {

    /**
     * Аутентификация по логину и паролю.
     */
    TokenResponse login(LoginRequest request, HttpServletRequest httpRequest, HttpServletResponse httpResponse);

    /**
     * Аутентификация через Kerberos SPNEGO SSO.
     */
    TokenResponse sso(HttpServletRequest httpRequest, HttpServletResponse httpResponse);

    /**
     * Завершение сессии и отзыв токена.
     */
    void logout(HttpServletRequest httpRequest, HttpServletResponse httpResponse);

    /**
     * Получение текущего аутентифицированного пользователя из контекста.
     */
    Optional<UserSession> getCurrentUser(HttpServletRequest httpRequest);
}
