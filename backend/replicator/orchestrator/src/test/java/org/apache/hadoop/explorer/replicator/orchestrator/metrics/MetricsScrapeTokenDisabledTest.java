package org.apache.hadoop.explorer.replicator.orchestrator.metrics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * With an empty metrics scrape token (the default) HTTP Basic access to /metrics is disabled.
 */
@SpringBootTest(properties = "hadoop.replicator.metrics-scrape-token=")
@AutoConfigureMockMvc
@ActiveProfiles("test")
class MetricsScrapeTokenDisabledTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("Пустой scrape-токен выключает Basic-доступ к /metrics")
    void emptyTokenDisablesBasicAccess() throws Exception {
        for (String password : new String[] {"", "any-token"}) {
            String header = "Basic " + Base64.getEncoder()
                    .encodeToString(("prometheus:" + password).getBytes(StandardCharsets.UTF_8));
            mockMvc.perform(get("/metrics").header("Authorization", header))
                    .andExpect(status().isUnauthorized());
        }
    }
}
