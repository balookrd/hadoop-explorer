package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClientPool;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class HmsFailoverRecoveryTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private HmsClientPool hmsClientPool;

    @BeforeEach
    void setup() {
        hmsJobRepository.deleteAll();
        hmsClientPool.initDemoClients();
    }

    @Test
    @DisplayName("Failover: Восстановление задачи Bootstrap, прерванной рестартом оркестратора")
    void testRecoverInterruptedBootstrappingJobs() {
        String jobId = "hms-job-interrupted-" + UUID.randomUUID().toString().substring(0, 8);

        // Имитируем падение оркестратора в процессе Bootstrap: статус остался BOOTSTRAPPING
        HmsReplicationJobEntity interruptedJob = new HmsReplicationJobEntity();
        interruptedJob.setId(jobId);
        interruptedJob.setSourceClusterId("dc1");
        interruptedJob.setTargetClusterId("dc2");
        interruptedJob.setSourceDbName("analytics");
        interruptedJob.setTargetDbName("analytics_failover");
        interruptedJob.setTableIncludePattern("*");
        interruptedJob.setStatus("BOOTSTRAPPING");
        interruptedJob.setCreatedBy("admin_user");
        hmsJobRepository.saveAndFlush(interruptedJob);

        // Вызываем хук старта оркестратора
        coordinatorService.recoverInterruptedJobsOnStartup();

        // Проверяем, что задача была подхвачена и успешно завершена в ACTIVE
        Optional<HmsReplicationJobEntity> recoveredOpt = hmsJobRepository.findById(jobId);
        assertTrue(recoveredOpt.isPresent());
        HmsReplicationJobEntity recovered = recoveredOpt.get();

        assertEquals("ACTIVE", recovered.getStatus(), "Прерванная задача должна перейти в ACTIVE");
        assertTrue(recovered.getReplicatedTables() >= 2, "Таблицы должны быть успешно реплицированы в процессе восстановления");
        assertNotNull(recovered.getLastProcessedEventId(), "Last processed event ID должен быть зафиксирован");

        // Проверяем, что в целевой базе DC2 появились таблицы
        MockHmsClient dc2 = (MockHmsClient) hmsClientPool.getClient("dc2");
        assertTrue(dc2.getAllTables("analytics_failover").contains("sales_daily"));
    }

    @Test
    @DisplayName("Failover: Корректный перевод в FAILED при фатальной ошибке источника во время восстановления")
    void testRecoverInterruptedJobFailureHandling() {
        String jobId = "hms-job-fatal-" + UUID.randomUUID().toString().substring(0, 8);

        // Регистрируем сбойный кластер
        hmsClientPool.registerClient("broken_dc", new HmsClient() {
            @Override
            public long getCurrentNotificationEventId() {
                throw new RuntimeException("Connection refused to HMS Thrift RPC");
            }
            @Override
            public List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents) {
                throw new RuntimeException("Connection refused");
            }
            @Override
            public List<String> getAllDatabases() { throw new RuntimeException("Connection refused"); }
            @Override
            public List<String> getAllTables(String dbName) { throw new RuntimeException("Connection refused"); }
            @Override
            public Optional<HmsTableDto> getTable(String dbName, String tableName) { throw new RuntimeException("Connection refused"); }
            @Override
            public List<HmsPartitionDto> getPartitions(String dbName, String tableName) { throw new RuntimeException("Connection refused"); }
            @Override
            public void createDatabase(String dbName, String locationUri) { throw new RuntimeException("Connection refused"); }
            @Override
            public void createTable(HmsTableDto table) { throw new RuntimeException("Connection refused"); }
            @Override
            public void alterTable(HmsTableDto table) { throw new RuntimeException("Connection refused"); }
            @Override
            public void addPartitions(String dbName, String tableName, List<HmsPartitionDto> partitions) { throw new RuntimeException("Connection refused"); }
            @Override
            public void dropTable(String dbName, String tableName, boolean deleteData) { throw new RuntimeException("Connection refused"); }
            @Override
            public void dropPartition(String dbName, String tableName, List<String> partVals, boolean deleteData) { throw new RuntimeException("Connection refused"); }
        });

        // Имитируем задачу со сбойным кластером источника
        HmsReplicationJobEntity invalidJob = new HmsReplicationJobEntity();
        invalidJob.setId(jobId);
        invalidJob.setSourceClusterId("broken_dc");
        invalidJob.setTargetClusterId("dc2");
        invalidJob.setSourceDbName("analytics");
        invalidJob.setTargetDbName("analytics_invalid");
        invalidJob.setTableIncludePattern("*");
        invalidJob.setStatus("BOOTSTRAPPING");
        invalidJob.setCreatedBy("admin_user");
        hmsJobRepository.saveAndFlush(invalidJob);

        // Запуск восстановления
        coordinatorService.recoverInterruptedJobsOnStartup();

        Optional<HmsReplicationJobEntity> failedOpt = hmsJobRepository.findById(jobId);
        assertTrue(failedOpt.isPresent());
        HmsReplicationJobEntity failed = failedOpt.get();

        assertEquals("FAILED", failed.getStatus(), "При невозможности связаться с источником задача должна получить статус FAILED");
        assertNotNull(failed.getMessage());
        assertTrue(failed.getMessage().contains("Сбой авто-восстановления после перезапуска"),
                "Сообщение должно пояснять причину сбоя при рестарте: " + failed.getMessage());
    }

    @Test
    @DisplayName("CDC: Фоновый периодический опрос активных задач репликации (scheduledCdcPoll)")
    void testScheduledCdcPoll() {
        // Создаем активную задачу
        HmsReplicationJobEntity activeJob = coordinatorService.createAndStartReplication(
                "dc1",
                "dc2",
                "analytics",
                "analytics_scheduled_test",
                "*",
                "admin_user"
        );
        assertEquals("ACTIVE", activeJob.getStatus());
        long lastEventBefore = activeJob.getLastProcessedEventId();

        // Добавляем новое событие в источник DC1
        MockHmsClient dc1 = (MockHmsClient) hmsClientPool.getClient("dc1");
        HmsPartitionDto newPart = new HmsPartitionDto(
                "hive",
                "analytics",
                "sales_daily",
                List.of("2026-10-10"),
                "hdfs://ns-hot:8020/warehouse/tablespace/external/hive/analytics.db/sales_daily/dt=2026-10-10",
                Map.of()
        );
        dc1.addPartitions("analytics", "sales_daily", List.of(newPart));
        dc1.emitEvent("ADD_PARTITION", "analytics", "sales_daily", "Added partition 2026-10-10");

        // Запускаем фоновый шедулер
        coordinatorService.scheduledCdcPoll();

        // Проверяем, что фоновый опрос обработал новое событие
        HmsReplicationJobEntity updated = hmsJobRepository.findById(activeJob.getId()).orElseThrow();
        assertTrue(updated.getLastProcessedEventId() > lastEventBefore,
                "Фоновый шедулер scheduledCdcPoll должен продвинуть lastProcessedEventId с " + lastEventBefore);

        MockHmsClient dc2 = (MockHmsClient) hmsClientPool.getClient("dc2");
        List<HmsPartitionDto> targetParts = dc2.getPartitions("analytics_scheduled_test", "sales_daily");
        boolean hasNewPart = targetParts.stream().anyMatch(p -> p.values().contains("2026-10-10"));
        assertTrue(hasNewPart, "Новая партиция должна быть автоматически реплицирована фоновым шедулером");
    }
}
