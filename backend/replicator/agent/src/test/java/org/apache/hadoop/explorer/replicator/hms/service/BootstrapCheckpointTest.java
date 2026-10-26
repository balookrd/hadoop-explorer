package org.apache.hadoop.explorer.replicator.hms.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("HMS: чекпоинт bootstrap")
class BootstrapCheckpointTest {

    @Test
    @DisplayName("Продвижение, завершение таблицы и разбор из JSON")
    void advanceDoneAndRoundTrip() {
        BootstrapCheckpoint cp = new BootstrapCheckpoint();
        cp.advance("big", "dt=p0010");
        cp.advance("big", "dt=p0020");
        cp.markDone("small", 0);

        BootstrapCheckpoint restored = BootstrapCheckpoint.parse(cp.toJson());

        assertEquals("dt=p0020", restored.resumeAfter("big"));
        assertTrue(restored.isDone("small"));
        restored.markDone("big", 1000);
        assertNull(restored.resumeAfter("big"), "завершенная таблица выходит из незавершенных");
        assertEquals(1000, restored.partitionsOf("big"));
    }

    @Test
    @DisplayName("Пустой и испорченный чекпоинт — bootstrap с начала")
    void emptyOrBrokenMeansFromScratch() {
        assertTrue(BootstrapCheckpoint.parse(null).isEmpty());
        assertTrue(BootstrapCheckpoint.parse("not json").isEmpty());
        assertFalse(BootstrapCheckpoint.parse("{\"done\":{\"t\":1}}").isEmpty());
    }
}
