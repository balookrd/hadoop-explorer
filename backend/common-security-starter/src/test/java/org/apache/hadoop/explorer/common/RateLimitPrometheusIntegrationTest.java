package org.apache.hadoop.explorer.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.model.LoginRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TestApplication.class, properties = {
    "hadoop.security.rate-limiter.enabled=true",
    "hadoop.security.rate-limiter.login.burst-capacity=1",
    "hadoop.security.rate-limiter.login.requests-per-minute=1"
})
@AutoConfigureMockMvc
class RateLimitPrometheusIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Отказ 429 попадает в /actuator/prometheus как hadoop_rate_limit_blocks_total с tier")
    void blockedLoginIsExportedToPrometheus() throws Exception {
        String body = objectMapper.writeValueAsString(new LoginRequest("admin", "any-password"));
        mockMvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON).content(body))
            .andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON).content(body))
            .andExpect(status().isTooManyRequests());

        mockMvc.perform(get("/actuator/prometheus"))
            .andExpect(status().isOk())
            .andExpect(content().string(containsString("hadoop_rate_limit_blocks_total{")))
            .andExpect(content().string(containsString("tier=\"login\"")));
    }
}
