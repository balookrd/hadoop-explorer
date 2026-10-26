package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsDeadLetterEventEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsDeadLetterEventRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Переигрывание события из очереди ядовитых событий.
 *
 * <p>Кнопка «переиграть» только меняла статус записи на `REPLAYED`: позиция задачи в журнале
 * метастора оставалась за этим событием, агент к нему не возвращался, и оператор видел
 * «переиграно» там, где ничего не произошло.
 */
@SpringBootTest
@ActiveProfiles("test")
@Transactional
@DisplayName("HMS DLQ: переигрывание возвращает задачу к событию")
class DlqReplayTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository jobRepository;

    @Autowired
    private HmsDeadLetterEventRepository dlqRepository;

    private HmsReplicationJobEntity job(String id, long lastProcessedEventId) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(id);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics_replica");
        job.setStatus("ACTIVE");
        job.setCreatedBy("tester");
        job.setLastProcessedEventId(lastProcessedEventId);
        return jobRepository.saveAndFlush(job);
    }

    private HmsDeadLetterEventEntity dlqEntry(String id, String jobId, long eventId) {
        HmsDeadLetterEventEntity entry = new HmsDeadLetterEventEntity();
        entry.setId(id);
        entry.setHmsJobId(jobId);
        entry.setEventId(eventId);
        entry.setEventType("ALTER_TABLE");
        entry.setTableName("sales_daily");
        entry.setStatus("NEW");
        entry.setFailedAt(Instant.now());
        return dlqRepository.saveAndFlush(entry);
    }

    @Test
    @DisplayName("Позиция задачи откатывается к событию перед ядовитым")
    void replayShouldRewindJobPosition() {
        HmsReplicationJobEntity job = job("hms-job-dlq-replay", 9000L);
        dlqEntry("dlq-1", job.getId(), 8500L);

        assertTrue(coordinatorService.replayDeadLetterEvent(job.getId(), "dlq-1"));

        HmsReplicationJobEntity reloaded = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals(8499L, reloaded.getLastProcessedEventId(),
                "Агент должен вернуться к событию: иначе переигрывание ничего не делает");
        assertEquals("REPLAYED", dlqRepository.findById("dlq-1").orElseThrow().getStatus());
    }

    @Test
    @DisplayName("Событие впереди текущей позиции не откатывает задачу назад")
    void replayShouldNotMoveThePositionForward() {
        HmsReplicationJobEntity job = job("hms-job-dlq-ahead", 100L);
        dlqEntry("dlq-2", job.getId(), 900L);

        coordinatorService.replayDeadLetterEvent(job.getId(), "dlq-2");

        HmsReplicationJobEntity reloaded = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals(100L, reloaded.getLastProcessedEventId(),
                "Позиция и так не дошла до события — двигать ее вперед нельзя");
    }

    @Test
    @DisplayName("Чужая запись очереди не трогает задачу")
    void replayOfForeignEntryShouldBeRejected() {
        HmsReplicationJobEntity job = job("hms-job-dlq-own", 500L);
        dlqEntry("dlq-3", "hms-job-other", 100L);

        assertFalse(coordinatorService.replayDeadLetterEvent(job.getId(), "dlq-3"));
        assertEquals(500L, jobRepository.findById(job.getId()).orElseThrow().getLastProcessedEventId());
    }
}
