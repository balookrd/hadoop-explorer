package org.apache.hadoop.explorer.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.Cookie;
import org.apache.hadoop.explorer.common.model.LoginRequest;
import org.apache.hadoop.explorer.common.model.TokenResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TestApplication.class)
@AutoConfigureMockMvc
class CommonSecurityIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Сквозной интеграционный сценарий: Login -> Cookie -> Protected API -> /me -> Logout -> Revoke")
    void fullAuthLifecycleTest() throws Exception {
        // 1. Попытка доступа к защищенному эндпоинту без авторизации -> 401
        mockMvc.perform(get("/api/v1/test/protected"))
            .andExpect(status().isUnauthorized());

        // 2. Вход под mock-администратором
        LoginRequest loginRequest = new LoginRequest("admin", "any-password");
        MvcResult loginResult = mockMvc.perform(post("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(loginRequest)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.access_token").isNotEmpty())
            .andExpect(jsonPath("$.user.username").value("admin"))
            .andExpect(jsonPath("$.user.is_admin").value(true))
            .andExpect(jsonPath("$.user.system_role").value("admin"))
            .andReturn();

        // Проверяем установку защищенной HttpOnly Cookie
        Cookie authCookie = loginResult.getResponse().getCookie("access_token");
        assertNotNull(authCookie, "HttpOnly cookie 'access_token' должна присутствовать в ответе");
        assertTrue(authCookie.isHttpOnly(), "Cookie должна быть HttpOnly");

        TokenResponse tokenResp = objectMapper.readValue(loginResult.getResponse().getContentAsString(), TokenResponse.class);
        String token = tokenResp.accessToken();

        // 3. Вызов /me с передачей Cookie
        mockMvc.perform(get("/api/v1/auth/me")
                .cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.username").value("admin"))
            .andExpect(jsonPath("$.is_admin").value(true));

        // 4. Доступ к защищенному сервисному эндпоинту с Cookie
        mockMvc.perform(get("/api/v1/test/protected")
                .cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));

        // 5. Альтернативный доступ с заголовком Authorization: Bearer <token>
        mockMvc.perform(get("/api/v1/test/protected")
                .header("Authorization", "Bearer " + token))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));

        // 6. Logout -> отзыв токена и очистка Cookie
        MvcResult logoutResult = mockMvc.perform(post("/api/v1/auth/logout")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andReturn();

        Cookie clearedCookie = logoutResult.getResponse().getCookie("access_token");
        assertNotNull(clearedCookie);
        assertEquals(0, clearedCookie.getMaxAge(), "Cookie должна удаляться (Max-Age=0)");

        // 7. Повторный доступ с отозванным токеном/Cookie -> 401 Unauthorized
        mockMvc.perform(get("/api/v1/auth/me")
                .cookie(authCookie))
            .andExpect(status().isUnauthorized());

        mockMvc.perform(get("/api/v1/test/protected")
                .header("Authorization", "Bearer " + token))
            .andExpect(status().isUnauthorized());
    }
}
