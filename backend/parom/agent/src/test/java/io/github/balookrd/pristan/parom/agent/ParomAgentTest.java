package io.github.balookrd.pristan.parom.agent;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParomAgentTest {

    @Test
    public void testConfigDefaults() {
        ParomAgentConfig config = ParomAgentConfig.fromEnv();
        assertNotNull(config.getAgentId());
        assertEquals("http://localhost:8005", config.getOrchestratorUrl());
        assertEquals(50051, config.getReceiverPort());
        assertEquals("/tmp/staging", config.getStagingDir());
        assertEquals(3.0, config.getPollIntervalSec());
        assertEquals(64 * 1024, config.getChunkSize());
    }

    @Test
    public void testTargetAddressResolutionFallback() {
        ParomAgentConfig config = new ParomAgentConfig();
        config.setFallbackTargetAddress("fallback-node:50051");
        config.setOrchestratorUrl("http://invalid-non-existing-host:9999");

        ParomAgent agent = new ParomAgent(config);
        String resolved = agent.resolveTargetAddress("unknown-cluster");
        assertEquals("fallback-node:50051", resolved);
    }
}
