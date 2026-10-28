package org.apache.hadoop.explorer.replicator.client;

import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Барьер «данные раньше DDL» открывается только по подтвержденному успеху подзадач.
 *
 * <p>Прежний барьер на ошибке запроса к оркестратору возвращал пустой список и считал данные
 * готовыми — DDL уходил на приемник без данных.
 */
@DisplayName("Data barrier: ожидание подзадач данных")
class SubjobBarrierTest {

    private static JobDto job(String id, String status) {
        return new JobDto(id, "dc1", "dc2", "/src", "/dst", status, null, false);
    }

    /** Клиент, у которого ответы оркестратора заданы сценарием по номеру опроса. */
    private static OrchestratorGrpcClient scripted(java.util.function.IntFunction<Map<String, JobDto>> script) {
        AtomicInteger poll = new AtomicInteger();
        return new OrchestratorGrpcClient() {
            @Override
            public Map<String, JobDto> getJobsByIds(Collection<String> jobIds) {
                Map<String, JobDto> answer = script.apply(poll.incrementAndGet());
                if (answer == null) {
                    throw new IllegalStateException("оркестратор недоступен");
                }
                return answer;
            }
        };
    }

    @Test
    @DisplayName("Сбой связи не открывает барьер: опрос повторяется до успеха подзадач")
    void transportFailureIsRetriedNotTreatedAsReady() {
        OrchestratorGrpcClient client = scripted(n -> n == 1 ? null
                : new HashMap<>(Map.of("s1", job("s1", "COMPLETED"), "s2", job("s2", "COMPLETED"))));

        assertTrue(client.awaitSubjobsCompletion(List.of("s1", "s2"), 30));
    }

    @Test
    @DisplayName("Оркестратор недоступен до таймаута — барьер закрыт")
    void unreachableOrchestratorKeepsBarrierClosed() {
        OrchestratorGrpcClient client = scripted(n -> null);

        assertFalse(client.awaitSubjobsCompletion(List.of("s1"), 1));
    }

    @Test
    @DisplayName("Упавшая подзадача закрывает барьер сразу")
    void failedSubjobClosesBarrier() {
        OrchestratorGrpcClient client = scripted(n -> Map.of("s1", job("s1", "COMPLETED"), "s2", job("s2", "FAILED")));

        assertFalse(client.awaitSubjobsCompletion(List.of("s1", "s2"), 30));
    }

    @Test
    @DisplayName("Пропавшая подзадача закрывает барьер")
    void missingSubjobClosesBarrier() {
        OrchestratorGrpcClient client = scripted(n -> Map.of("s1", job("s1", "COMPLETED")));

        assertFalse(client.awaitSubjobsCompletion(List.of("s1", "s2"), 30));
    }

    @Test
    @DisplayName("Пауза или fencing прерывают ожидание сразу, а не по таймауту барьера")
    void cancellationStopsWaitingBeforeTimeout() {
        OrchestratorGrpcClient client = scripted(n -> Map.of("s1", job("s1", "RUNNING")));
        AtomicInteger checks = new AtomicInteger();

        long started = System.currentTimeMillis();
        assertFalse(client.awaitSubjobsCompletion(List.of("s1"), 3600, () -> checks.incrementAndGet() >= 2));
        assertTrue(System.currentTimeMillis() - started < 10_000, "ожидание должно прерваться за несколько секунд");
    }
}
