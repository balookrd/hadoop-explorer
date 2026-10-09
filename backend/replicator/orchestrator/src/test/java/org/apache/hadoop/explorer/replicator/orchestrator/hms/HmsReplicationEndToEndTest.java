package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClientPool;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class HmsReplicationEndToEndTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private HmsEventLogRepository eventLogRepository;

    @Autowired
    private HmsClientPool hmsClientPool;

    @Autowired
    private JobService jobService;

    @Test
    @DisplayName("Сквозной тест Bootstrap, изоляции саб-джоб, Non-ACID шлюза, HDFS Federation и CDC")
    void testFullHmsReplicationPipeline() {
        // 1. Запуск первичного Bootstrap для базы analytics (HDP 3.1 -> Apache Hive 3.1.3)
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1",
                "dc2",
                "analytics",
                "analytics_replica",
                "*",
                "admin_user"
        );

        assertNotNull(job.getId());
        assertEquals("ACTIVE", job.getStatus(), "После завершения Bootstrap статус должен стать ACTIVE");
        assertEquals(3, job.getTotalTables(), "Всего 3 таблицы в источнике (1 external, 1 managed non-acid, 1 acid)");
        assertEquals(2, job.getReplicatedTables(), "Только 2 таблицы должны быть реплицированы (External и Managed Non-Acid)");

        // 2. Проверка изоляции HDFS саб-джоб
        List<JobResponse> standardJobs = jobService.listJobs(null, "admin", true, false);
        for (JobResponse j : standardJobs) {
            assertNotEquals(job.getId(), j.parentJobId(), "Саб-джобы репликатора HMS не должны попадать в регламентный список HDFS");
        }

        List<JobResponse> subjobs = jobService.listSubjobsByParentId(job.getId());
        assertFalse(subjobs.isEmpty(), "Подзадачи должны быть доступны по parentJobId в разделе HMS Replication");

        // 3. Проверка целевого метастора DC2 (Apache Hive 3.1.3)
        MockHmsClient dc2 = (MockHmsClient) hmsClientPool.getClient("dc2");
        assertTrue(dc2.getAllTables("analytics_replica").contains("sales_daily"));
        assertTrue(dc2.getAllTables("analytics_replica").contains("dim_customers"));
        assertFalse(dc2.getAllTables("analytics_replica").contains("acid_orders_streaming"), "ACID таблица должна быть отфильтрована");

        // Проверка очистки параметров HDP 3.1
        Optional<HmsTableDto> targetSales = dc2.getTable("analytics_replica", "sales_daily");
        assertTrue(targetSales.isPresent());
        assertFalse(targetSales.get().parameters().containsKey("hdp.version"), "Вендорный параметр hdp.version должен быть удален");

        // Проверка трансляции федеративного пути партиции (ns-cold -> ns-cold-dc2)
        List<HmsPartitionDto> targetParts = dc2.getPartitions("analytics_replica", "sales_daily");
        assertEquals(1, targetParts.size());
        assertTrue(targetParts.get(0).location().startsWith("hdfs://ns-cold-dc2:8020/"),
                "Путь партиции должен быть оттранслирован на целевой NameService федерации: " + targetParts.get(0).location());

        // Проверка журнала событий
        List<HmsEventLogEntity> events = eventLogRepository.findByHmsJobIdOrderByCreatedAtDesc(job.getId(), PageRequest.of(0, 100));
        boolean hasAcidSkipped = events.stream().anyMatch(e -> "SKIPPED_ACID".equals(e.getStatus()) && "acid_orders_streaming".equals(e.getTableName()));
        assertTrue(hasAcidSkipped, "В журнале событий должна быть зафиксирована запись о пропуске ACID-таблицы");

        // 4. Потоковый режим CDC: добавление новой партиции в источнике
        MockHmsClient dc1 = (MockHmsClient) hmsClientPool.getClient("dc1");
        HmsPartitionDto newPart = new HmsPartitionDto(
                "hive",
                "analytics",
                "sales_daily",
                List.of("2026-10-09"),
                "hdfs://ns-cold:8020/warehouse/tablespace/external/hive/analytics.db/sales_daily/dt=2026-10-09",
                Map.of("numRows", "2000000")
        );
        dc1.addPartitions("analytics", "sales_daily", List.of(newPart));
        dc1.emitEvent("ADD_PARTITION", "analytics", "sales_daily", "Added dt=2026-10-09");

        // Запуск опроса CDC
        int processed = coordinatorService.pollCdcEvents(job.getId());
        assertEquals(1, processed, "Должно быть обработано 1 событие CDC");

        // Проверка появления новой партиции на целевой стороне
        List<HmsPartitionDto> updatedTargetParts = dc2.getPartitions("analytics_replica", "sales_daily");
        assertEquals(2, updatedTargetParts.size());

        // 5. Проверка безопасного удаления DROP с deleteData = false
        dc1.emitEvent("DROP_TABLE", "analytics", "dim_customers", "Drop table dim_customers");
        coordinatorService.pollCdcEvents(job.getId());
        assertFalse(dc2.getAllTables("analytics_replica").contains("dim_customers"), "Таблица должна быть удалена из целевого метастора");
    }
}
