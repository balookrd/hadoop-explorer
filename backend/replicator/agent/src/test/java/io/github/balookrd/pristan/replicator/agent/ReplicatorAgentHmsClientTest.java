package io.github.balookrd.pristan.replicator.agent;

import io.github.balookrd.pristan.replicator.hms.client.MockHmsClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Selection of the agent HMS client: no silent fallback to the built-in metastore emulator. */
class ReplicatorAgentHmsClientTest {

    private static ReplicatorAgentConfig config(String mode, String hmsMode, String thriftUris) {
        ReplicatorAgentConfig config = new ReplicatorAgentConfig();
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
        ReplicatorAgent agent = new ReplicatorAgent(config("all", "auto", null));

        assertNull(agent.getHmsClient());
        assertNull(agent.getHmsTaskExecutor());
    }

    @Test
    void thriftWithoutUrisFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ReplicatorAgent.createHmsClient(config("all", "thrift", " ")));
        assertTrue(e.getMessage().contains("HMS_THRIFT_URIS"));
    }

    @Test
    void unknownModeFailsAtStartup() {
        assertThrows(IllegalStateException.class,
                () -> ReplicatorAgent.createHmsClient(config("all", "mok", null)));
    }

    @Test
    void mockOnlyWhenRequestedExplicitly() {
        assertInstanceOf(MockHmsClient.class, ReplicatorAgent.createHmsClient(config("all", "mock", null)));
    }
}
