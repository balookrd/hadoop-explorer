package io.github.balookrd.pristan.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.Cookie;
import io.github.balookrd.pristan.common.model.LoginRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TestApplication.class)
@AutoConfigureMockMvc
class ActuatorAccessIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Actuator: health и prometheus публичны, остальные эндпоинты только для ADMIN")
    void actuatorEndpointsAreSegmentedByRole() throws Exception {
        // Health is public; it may report 503 when an LDAP health indicator cannot reach a server
        mockMvc.perform(get("/actuator/health"))
            .andExpect(result -> {
                int code = result.getResponse().getStatus();
                assertTrue(code == 200 || code == 503, "Health endpoint must be public, got " + code);
            });

        mockMvc.perform(get("/actuator/prometheus"))
            .andExpect(status().isOk())
            .andExpect(content().string(containsString("jvm_memory_used_bytes")));

        mockMvc.perform(get("/actuator/metrics"))
            .andExpect(status().isUnauthorized());

        mockMvc.perform(get("/actuator/metrics").cookie(login("reader")))
            .andExpect(status().isForbidden());

        mockMvc.perform(get("/actuator/metrics").cookie(login("admin")))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.names").isArray());
    }

    private Cookie login(String username) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new LoginRequest(username, "any-password"))))
            .andExpect(status().isOk())
            .andReturn();
        Cookie cookie = result.getResponse().getCookie("access_token");
        assertNotNull(cookie, "access_token cookie must be set for " + username);
        return cookie;
    }
}
