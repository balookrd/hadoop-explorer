package org.apache.hadoop.explorer.replicator.agent;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicatorAgentTest {

    @Test
    public void testConfigDefaults() {
        ReplicatorAgentConfig config = ReplicatorAgentConfig.fromEnv();
        assertNotNull(config.getAgentId());
        assertEquals("http://localhost:8005", config.getOrchestratorUrl());
        assertEquals(50051, config.getReceiverPort());
        assertEquals("/tmp/staging", config.getStagingDir());
        assertEquals(3.0, config.getPollIntervalSec());
        assertEquals(64 * 1024, config.getChunkSize());
    }

    @Test
    public void testTargetAddressResolutionFallback() {
        ReplicatorAgentConfig config = new ReplicatorAgentConfig();
        config.setFallbackTargetAddress("fallback-node:50051");
        config.setOrchestratorUrl("http://invalid-non-existing-host:9999");

        ReplicatorAgent agent = new ReplicatorAgent(config);
        String resolved = agent.resolveTargetAddress("unknown-cluster");
        assertEquals("fallback-node:50051", resolved);
    }
}
