package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Отчет о дрейфе схемы по журналу задачи.
 *
 * <p>Сканирование возвращало фиксированный «дрейфа нет» с именем таблицы, подставленным из
 * шаблона включения: интерфейс показывал зеленый статус независимо от того, что происходило на
 * приемнике. Расхождения фиксирует агент при переносе (он единственный видит оба метастора),
 * поэтому оркестратор обязан отдавать именно эти записи, а при их отсутствии — пустой список,
 * а не выдуманный отчет.
 */
@SpringBootTest
@ActiveProfiles("test")
@Transactional
@DisplayName("HMS: отчет о дрейфе схемы отражает журнал задачи")
class HmsSchemaDriftScanTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private HmsEventLogRepository eventLogRepository;

    private HmsReplicationJobEntity createJob(String id) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(id);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics_replica");
        job.setTableIncludePattern("*");
        job.setStatus("ACTIVE");
        job.setCreatedBy("tester");
        return hmsJobRepository.saveAndFlush(job);
    }

    private void recordDriftEvent(String jobId, String table, String discrepancies) {
        recordDriftEvent(jobId, table, discrepancies, Instant.now());
    }

    private void recordDriftEvent(String jobId, String table, String discrepancies, Instant detectedAt) {
        HmsEventLogEntity event = new HmsEventLogEntity();
        event.setId(UUID.randomUUID().toString());
        event.setHmsJobId(jobId);
        event.setEventId(42L);
        event.setEventType("SCHEMA_DRIFT");
        event.setTableName(table);
        event.setStatus("SCHEMA_DRIFT");
        event.setErrorMessage(discrepancies);
        event.setCreatedAt(detectedAt);
        event.setUpdatedAt(detectedAt);
        eventLogRepository.saveAndFlush(event);
    }

    @Test
    @DisplayName("Зафиксированные расхождения попадают в отчет")
    void recordedDriftShouldBeReported() {
        HmsReplicationJobEntity job = createJob("hms-job-drift-found");
        recordDriftEvent(job.getId(), "sales_daily",
                "Несовпадение типа колонки 'amount': источник='decimal(10,2)', таргет='double'"
                        + "; Отсутствует колонка 'region' (string) в целевой таблице");

        List<HmsSchemaDriftReport> reports = coordinatorService.scanSchemaDrift(job.getId());

        assertEquals(1, reports.size(), "Должен быть один отчет — по таблице с расхождениями");
        HmsSchemaDriftReport report = reports.get(0);
        assertTrue(report.hasDrift(), "Отчет должен сообщать о дрейфе");
        assertEquals("sales_daily", report.tableName(), "Имя таблицы берется из журнала, а не из шаблона");
        assertEquals("analytics_replica", report.dbName(), "Дрейф относится к базе на приемнике");
        assertEquals(2, report.discrepancies().size(), "Каждое расхождение — отдельная строка отчета");
        assertTrue(report.discrepancies().stream().anyMatch(d -> d.contains("decimal(10,2)")));
        assertNotNull(report.checkedAt(), "Момент обнаружения должен быть известен");
    }

    @Test
    @DisplayName("Без зафиксированных расхождений отчет пустой, а не выдуманный")
    void noRecordedDriftShouldReturnEmptyReport() {
        HmsReplicationJobEntity job = createJob("hms-job-drift-clean");

        List<HmsSchemaDriftReport> reports = coordinatorService.scanSchemaDrift(job.getId());

        assertTrue(reports.isEmpty(),
                "Пока агент не зафиксировал расхождений, отдавать нечего: " + reports);
    }

    @Test
    @DisplayName("По таблице берется последняя запись, а не все проходы")
    void onlyLatestDriftPerTableShouldBeReported() {
        HmsReplicationJobEntity job = createJob("hms-job-drift-latest");
        Instant firstPass = Instant.now().minusSeconds(120);
        recordDriftEvent(job.getId(), "sales_daily", "Несовпадение InputFormat: источник='A', таргет='B'", firstPass);
        recordDriftEvent(job.getId(), "sales_daily", "Отсутствует колонка 'region' (string) в целевой таблице", firstPass.plusSeconds(60));
        recordDriftEvent(job.getId(), "dim_customers", "Несовпадение типа таблицы: ожидается 'EXTERNAL_TABLE', фактически 'MANAGED_TABLE'", firstPass.plusSeconds(60));

        List<HmsSchemaDriftReport> reports = coordinatorService.scanSchemaDrift(job.getId());

        assertEquals(2, reports.size(), "По одной записи на таблицу: " + reports);
        HmsSchemaDriftReport sales = reports.stream()
                .filter(r -> "sales_daily".equals(r.tableName()))
                .findFirst()
                .orElseThrow();
        assertEquals(List.of("Отсутствует колонка 'region' (string) в целевой таблице"), sales.discrepancies(),
                "Актуально последнее состояние таблицы");
    }

    @Test
    @DisplayName("Сканирование неизвестной задачи отвергается")
    void unknownJobShouldBeRejected() {
        assertThrows(IllegalArgumentException.class, () -> coordinatorService.scanSchemaDrift("no-such-job"));
    }
}
