package io.github.balookrd.pristan.zastava;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.zastava.model.LoginRequest;
import io.github.balookrd.pristan.zastava.model.TokenResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import static org.hamcrest.Matchers.matchesPattern;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TestApplication.class, properties = {
    "zastava.rate-limiter.enabled=true",
    "zastava.rate-limiter.login.burst-capacity=2",
    "zastava.rate-limiter.login.requests-per-minute=1",
    "zastava.rate-limiter.burst-capacity=3",
    "zastava.rate-limiter.requests-per-minute=1",
    "zastava.rate-limiter.trusted-proxies=10.0.0.1"
})
@AutoConfigureMockMvc
class RateLimitFilterIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private static RequestPostProcessor from(String ip) {
        return request -> {
            request.setRemoteAddr(ip);
            return request;
        };
    }

    private ResultActions login(String peer, String forwardedFor) throws Exception {
        MockHttpServletRequestBuilder request = post("/api/v1/auth/login")
            .with(from(peer))
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(new LoginRequest("admin", "any-password")));
        if (forwardedFor != null) {
            request.header("X-Forwarded-For", forwardedFor);
        }
        return mockMvc.perform(request);
    }

    @Test
    @DisplayName("Вход ограничивается по IP клиента: 429 с Retry-After, другой IP проходит")
    void loginIsLimitedPerClientIp() throws Exception {
        login("192.0.2.10", null).andExpect(status().isOk());
        login("192.0.2.10", null).andExpect(status().isOk());
        login("192.0.2.10", null)
            .andExpect(status().isTooManyRequests())
            .andExpect(header().string("Retry-After", matchesPattern("\\d+")))
            .andExpect(jsonPath("$.status").value(429));

        login("192.0.2.11", null).andExpect(status().isOk());
    }

    @Test
    @DisplayName("X-Forwarded-For от недоверенного адреса не меняет ключ")
    void forwardedForIgnoredFromUntrustedPeer() throws Exception {
        login("192.0.2.20", "198.51.100.1").andExpect(status().isOk());
        login("192.0.2.20", "198.51.100.2").andExpect(status().isOk());
        login("192.0.2.20", "198.51.100.3").andExpect(status().isTooManyRequests());
    }

    @Test
    @DisplayName("X-Forwarded-For от доверенного прокси задает ключ")
    void forwardedForHonoredFromTrustedProxy() throws Exception {
        login("10.0.0.1", "198.51.100.50").andExpect(status().isOk());
        login("10.0.0.1", "198.51.100.50").andExpect(status().isOk());
        login("10.0.0.1", "198.51.100.50").andExpect(status().isTooManyRequests());
        login("10.0.0.1", "198.51.100.51").andExpect(status().isOk());
    }

    @Test
    @DisplayName("Запросы к API ограничиваются по пользователю, а не по IP")
    void apiIsLimitedPerUser() throws Exception {
        String body = login("192.0.2.40", null).andExpect(status().isOk())
            .andReturn().getResponse().getContentAsString();
        String token = objectMapper.readValue(body, TokenResponse.class).accessToken();

        for (int i = 0; i < 3; i++) {
            mockMvc.perform(get("/api/v1/test/protected").with(from("192.0.2.41"))
                    .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
        }
        mockMvc.perform(get("/api/v1/test/protected").with(from("192.0.2.42"))
                .header("Authorization", "Bearer " + token))
            .andExpect(status().isTooManyRequests());
    }

    @Test
    @DisplayName("Путь входа с процентным кодированием попадает в уровень входа")
    void encodedLoginPathUsesLoginTier() throws Exception {
        for (int i = 0; i < 2; i++) {
            mockMvc.perform(post(java.net.URI.create("/api/v1/auth/l%6Fgin")).with(from("192.0.2.60"))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(new LoginRequest("admin", "any-password"))))
                .andExpect(status().isOk());
        }
        mockMvc.perform(post(java.net.URI.create("/api/v1/auth/l%6Fgin")).with(from("192.0.2.60"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new LoginRequest("admin", "any-password"))))
            .andExpect(status().isTooManyRequests());
    }

    @Test
    @DisplayName("Анонимный запрос к защищенному API отклоняется цепочкой безопасности и токены не тратит")
    void anonymousRequestsRejectedBySecurityDoNotConsumeTokens() throws Exception {
        for (int i = 0; i < 6; i++) {
            mockMvc.perform(get("/api/v1/test/protected").with(from("192.0.2.70")))
                .andExpect(status().isUnauthorized());
        }
    }
}
