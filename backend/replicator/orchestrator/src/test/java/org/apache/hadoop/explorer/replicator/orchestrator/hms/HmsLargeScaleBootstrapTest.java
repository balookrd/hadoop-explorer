package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClientPool;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class HmsLargeScaleBootstrapTest {

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

    @BeforeEach
    void setup() {
        hmsJobRepository.deleteAll();
        hmsClientPool.initDemoClients();
    }

    @Test
    @DisplayName("Scale: Масштабируемый Bootstrap таблицы с 2500 партициями (Батчинг, агрегация саб-джоб и чанкинг)")
    void testLargeScalePartitionBootstrap() {
        String dbName = "scale_db";
        String tblName = "large_events";
        String rootLocation = "hdfs://ns-hot:8020/warehouse/tablespace/external/hive/scale_db.db/large_events";

        MockHmsClient src = (MockHmsClient) hmsClientPool.getClient("dc1");
        src.createDatabase(dbName, "hdfs://ns-hot:8020/warehouse/tablespace/external/hive/scale_db.db");

        // Создаем партиционированную таблицу
        HmsTableDto table = new HmsTableDto(
                "hive",
                dbName,
                tblName,
                "EXTERNAL_TABLE",
                rootLocation,
                Map.of("EXTERNAL", "TRUE"),
                List.of("event_date"),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        );
        src.createTable(table);

        // Генерируем 2500 партиций, расположенных стандартно внутри корня таблицы
        int partitionCount = 2500;
        List<HmsPartitionDto> generatedPartitions = new ArrayList<>(partitionCount);
        for (int i = 1; i <= partitionCount; i++) {
            String dateVal = String.format("2026-%02d-%02d", (i % 12) + 1, (i % 28) + 1);
            String partLoc = rootLocation + "/event_date=" + dateVal + "_" + i;
            generatedPartitions.add(new HmsPartitionDto(
                    "hive",
                    dbName,
                    tblName,
                    List.of(dateVal + "_" + i),
                    partLoc,
                    Map.of("numRows", "1000")
            ));
        }
        src.addPartitions(dbName, tblName, generatedPartitions);

        // Запуск репликации
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1",
                "dc2",
                dbName,
                "scale_db_replica",
                "*",
                "admin_user"
        );

        assertNotNull(job.getId());
        assertEquals("ACTIVE", job.getStatus());
        assertEquals(partitionCount, job.getReplicatedPartitions());

        // 1. Проверяем умную агрегацию HDFS:
        // Для 2500 партиций внутри таблицы должна быть создана РОВНО 1 саб-джоба HDFS (корневая для таблицы),
        // а не 2500 отдельных саб-джоб!
        List<JobResponse> subjobs = jobService.listSubjobsByParentId(job.getId());
        assertEquals(1, subjobs.size(), "Должна быть создана ровно 1 саб-джоба на корень таблицы вместо 2500 подзадач!");

        // 2. Проверяем целевой метастор: все 2500 партиций реплицированы батчами
        MockHmsClient dst = (MockHmsClient) hmsClientPool.getClient("dc2");
        List<HmsPartitionDto> targetPartitions = dst.getPartitions("scale_db_replica", tblName);
        assertEquals(partitionCount, targetPartitions.size(), "Все 2500 партиций должны присутствовать в целевом метасторе");

        // 3. Проверяем компактность журнала событий (Чанкинг):
        // Вместо 2500 записей должны быть записаны агрегированные чанки (по 1000 шт: 3 чанка) + 1 запись о таблице
        List<HmsEventLogEntity> events = eventLogRepository.findByHmsJobIdOrderByCreatedAtDesc(job.getId(), PageRequest.of(0, 100));
        long chunkEventsCount = events.stream().filter(e -> "BOOTSTRAP_PARTITION_CHUNK".equals(e.getEventType())).count();
        assertEquals(3, chunkEventsCount, "Должно быть ровно 3 агрегированных чанка (1000 + 1000 + 500) в аудит-логе");
    }
}
