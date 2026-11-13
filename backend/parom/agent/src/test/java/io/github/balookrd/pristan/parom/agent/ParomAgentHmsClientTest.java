package io.github.balookrd.pristan.parom.agent;

import io.github.balookrd.pristan.parom.hms.client.MockHmsClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Selection of the agent HMS client: no silent fallback to the built-in metastore emulator. */
class ParomAgentHmsClientTest {

    private static ParomAgentConfig config(String mode, String hmsMode, String thriftUris) {
        ParomAgentConfig config = new ParomAgentConfig();
        config.setAgentId("agent-test");
        config.setClusterId("dc1");
        config.setMode(mode);
        config.setHmsMode(hmsMode);
        config.setHmsThriftUris(thriftUris);
        config.setOrchestratorUrl("http://orchestrator:8005");
        return config;
    }

    @Test
    void autoWithoutThriftUrisDisablesHmsReplication() {
        ParomAgent agent = new ParomAgent(config("all", "auto", null));

        assertNull(agent.getHmsClient());
        assertNull(agent.getHmsTaskExecutor());
    }

    @Test
    void thriftWithoutUrisFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ParomAgent.createHmsClient(config("all", "thrift", " ")));
        assertTrue(e.getMessage().contains("HMS_THRIFT_URIS"));
    }

    @Test
    void unknownModeFailsAtStartup() {
        assertThrows(IllegalStateException.class,
                () -> ParomAgent.createHmsClient(config("all", "mok", null)));
    }

    @Test
    void mockOnlyWhenRequestedExplicitly() {
        assertInstanceOf(MockHmsClient.class, ParomAgent.createHmsClient(config("all", "mock", null)));
    }
}
