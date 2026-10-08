package org.apache.hadoop.explorer.common.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.access.AccessDeniedHandler;

import java.io.IOException;
import java.util.Map;

/**
 * Обработчики ошибок 401 Unauthorized и 403 Forbidden в формате JSON.
 */
public class CommonSecurityErrorHandlers implements AuthenticationEntryPoint, AccessDeniedHandler {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException)
        throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        Map<String, Object> error = Map.of(
            "detail", "Необходима авторизация: " + (authException != null ? authException.getMessage() : "Отсутствует валидный токен"),
            "status", 401
        );
        objectMapper.writeValue(response.getOutputStream(), error);
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, AccessDeniedException accessDeniedException)
        throws IOException {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        Map<String, Object> error = Map.of(
            "detail", "Доступ запрещен: " + (accessDeniedException != null ? accessDeniedException.getMessage() : "Недостаточно прав"),
            "status", 403
        );
        objectMapper.writeValue(response.getOutputStream(), error);
    }
}
