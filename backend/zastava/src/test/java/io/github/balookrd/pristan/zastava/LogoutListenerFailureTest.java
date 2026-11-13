package io.github.balookrd.pristan.zastava;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.Cookie;
import io.github.balookrd.pristan.zastava.auth.UserLoggedOutEvent;
import io.github.balookrd.pristan.zastava.model.LoginRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TestApplication.class)
@AutoConfigureMockMvc
@Import(LogoutListenerFailureTest.FailingLogoutListener.class)
class LogoutListenerFailureTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Исключение слушателя UserLoggedOutEvent не ломает logout: 200, токен отозван")
    void failingListenerDoesNotBreakLogout() throws Exception {
        MvcResult loginResult = mockMvc.perform(post("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new LoginRequest("admin", "any-password"))))
            .andExpect(status().isOk())
            .andReturn();
        Cookie authCookie = loginResult.getResponse().getCookie("access_token");

        mockMvc.perform(post("/api/v1/auth/logout")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true));

        assertEquals(1, FailingLogoutListener.invocations.get());
        mockMvc.perform(get("/api/v1/auth/me").cookie(authCookie))
            .andExpect(status().isUnauthorized());
    }

    static class FailingLogoutListener {
        static final java.util.concurrent.atomic.AtomicInteger invocations =
            new java.util.concurrent.atomic.AtomicInteger();

        @EventListener
        void on(UserLoggedOutEvent event) {
            invocations.incrementAndGet();
            throw new IllegalStateException("Livy is unavailable");
        }
    }
}
