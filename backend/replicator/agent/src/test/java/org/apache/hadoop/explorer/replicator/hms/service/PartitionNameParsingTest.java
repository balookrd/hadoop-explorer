package org.apache.hadoop.explorer.replicator.hms.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Значения партиции из имени в формате метастора: значения экранированы как {@code %XX}.
 */
@DisplayName("HMS: разбор имени партиции")
class PartitionNameParsingTest {

    @Test
    @DisplayName("Несколько ключей и экранированные символы")
    void multiKeyEscaped() {
        assertEquals(List.of("2026-10-01 10:00", "eu/west"),
                HmsTaskExecutor.partitionValuesFromName("dt=2026-10-01 10%3A00/region=eu%2Fwest"));
    }

    @Test
    @DisplayName("Одиночный ключ и знак процента без кода")
    void singleKey() {
        assertEquals(List.of("50%"), HmsTaskExecutor.partitionValuesFromName("pct=50%"));
    }
}
