package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Стартовая позиция и лимит полосы Kafka-задачи.
 *
 * <p>Задача читала топик с начала, и на боевом топике через WAN уходил весь его retention. Лимит
 * задачи применял каждый агент целиком: при 12 агентах фактический потолок был в 12 раз выше.
 */
@DisplayName("Kafka: стартовая позиция и доля лимита полосы на агента")
class KafkaJobStartAndBandwidthTest {

    @Test
    @DisplayName("Незаданная позиция — LATEST, регистр не важен, опечатка отклоняется")
    void testStartPositionNormalization() {
        assertEquals("LATEST", KafkaReplicationOrchestratorService.normalizeStartPosition(null));
        assertEquals("LATEST", KafkaReplicationOrchestratorService.normalizeStartPosition("  "));
        assertEquals("EARLIEST", KafkaReplicationOrchestratorService.normalizeStartPosition("earliest"));
        assertThrows(IllegalArgumentException.class,
                () -> KafkaReplicationOrchestratorService.normalizeStartPosition("EARLIES"),
                "Опечатка не должна молча превращаться в перекачку всего топика");
    }

    @Test
    @DisplayName("Лимит задачи делится между живыми отправителями ЦОД")
    void testBandwidthIsSplitBetweenSenders() {
        KafkaReplicationOrchestratorService service = mock(KafkaReplicationOrchestratorService.class, CALLS_REAL_METHODS);
        doReturn(List.of("a:1", "b:1", "c:1", "d:1")).when(service).getLiveSenderEndpoints("dc1");

        assertEquals(300.0, service.perAgentBandwidthMb(1200.0, "dc1"), 1e-9);
    }

    @Test
    @DisplayName("Без лимита делить нечего, без живых отправителей лимит не дробится")
    void testUnlimitedAndNoSenders() {
        KafkaReplicationOrchestratorService service = mock(KafkaReplicationOrchestratorService.class, CALLS_REAL_METHODS);
        doReturn(List.of()).when(service).getLiveSenderEndpoints("dc1");

        assertEquals(0.0, service.perAgentBandwidthMb(0.0, "dc1"));
        assertEquals(0.0, service.perAgentBandwidthMb(null, "dc1"));
        assertEquals(500.0, service.perAgentBandwidthMb(500.0, "dc1"), 1e-9);
    }
}
