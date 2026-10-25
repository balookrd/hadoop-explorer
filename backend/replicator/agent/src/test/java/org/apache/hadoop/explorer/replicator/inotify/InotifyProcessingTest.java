package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class InotifyProcessingTest {

    private HadoopFsManager fsManager;
    private FileSystem fileSystem;
    private OrchestratorGrpcClient orchestratorClient;
    private InotifyBatchProcessor processor;

    private JobDto streamingJob;

    @BeforeEach
    void setUp() throws IOException {
        fsManager = mock(HadoopFsManager.class);
        fileSystem = mock(FileSystem.class);
        when(fsManager.getFileSystem()).thenReturn(fileSystem);

        orchestratorClient = mock(OrchestratorGrpcClient.class);
        when(orchestratorClient.batchCreateTasks(any(BatchCreateTasksRequest.class))).thenReturn(true);
        when(orchestratorClient.batchCreateTasks(anyString(), any(BatchCreateTasksRequest.class))).thenReturn(true);
        when(orchestratorClient.updateJobStreamingTxid(anyString(), anyLong(), anyLong())).thenReturn(true);

        processor = new InotifyBatchProcessor(fsManager, orchestratorClient);

        streamingJob = new JobDto();
        streamingJob.setId("job-streaming-1");
        streamingJob.setSourcePath("/warehouse/tablespace/managed/hive/analytics.db/sales_daily");
        streamingJob.setTargetPath("/backup/warehouse/tablespace/managed/hive/analytics.db/sales_daily");
        streamingJob.setSyncMode("STREAMING_INOTIFY");
        streamingJob.setSyncDeletes(true);
    }

    @Test
    @DisplayName("InotifyPathFilter должен точно фильтровать все staging-пути Spark, Hive и временные файлы")
    void testStagingPathFilter() {
        assertTrue(InotifyPathFilter.isStagedPath("/warehouse/sales/.hive-staging_hive_2026-10-10/0/_temporary/0/part-00000"));
        assertTrue(InotifyPathFilter.isStagedPath("/warehouse/sales/.spark-staging-uuid/data.parquet"));
        assertTrue(InotifyPathFilter.isStagedPath("/warehouse/sales/_temporary/part-0.parquet"));
        assertTrue(InotifyPathFilter.isStagedPath("/warehouse/sales/.staging/job_1/job.xml"));
        assertTrue(InotifyPathFilter.isStagedPath("/warehouse/sales/.tmp/output.tmp"));
        assertTrue(InotifyPathFilter.isStagedPath("/warehouse/sales/data.parquet.tmp"));

        // Постоянные пути не должны блокироваться
        assertFalse(InotifyPathFilter.isStagedPath("/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000.parquet"));
        assertFalse(InotifyPathFilter.isStagedPath("/warehouse/sales_daily/dt=2026-10-10/data.orc"));
    }

    @Test
    @DisplayName("CloseEvent: Промежуточные staged-файлы игнорируются, постоянные файлы ставятся в очередь")
    void testCloseEventFiltering() {
        // 1. CloseEvent во временной staging директории -> игнорируется
        Event.CloseEvent stagedClose = new Event.CloseEvent(
                "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/.hive-staging_123/part-0.tmp",
                1024L, 100L
        );
        int stagedCount = processor.processEvent(stagedClose, 100L, 5L, List.of(streamingJob));
        assertEquals(0, stagedCount);
        verify(orchestratorClient, never()).batchCreateTasks(anyString(), any());

        // 2. CloseEvent в постоянном каталоге -> ставится в очередь
        Event.CloseEvent permanentClose = new Event.CloseEvent(
                "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000.parquet",
                5242880L, 101L
        );
        int permCount = processor.processEvent(permanentClose, 101L, 4L, List.of(streamingJob));
        assertEquals(1, permCount);

        ArgumentCaptor<BatchCreateTasksRequest> captor = ArgumentCaptor.forClass(BatchCreateTasksRequest.class);
        verify(orchestratorClient).batchCreateTasks(captor.capture());
        assertEquals("job-streaming-1", captor.getValue().getJobId());
        assertEquals(1, captor.getValue().getTasks().size());
        assertEquals("/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000.parquet",
                captor.getValue().getTasks().get(0).getSourcePath());
        assertEquals("/backup/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000.parquet",
                captor.getValue().getTasks().get(0).getTargetPath());
    }

    @Test
    @DisplayName("UnlinkEvent: при включенной репликации удалений ставится задача удаления на приемнике")
    void testUnlinkEventEnqueuesDeleteTask() {
        String deleted = "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000.parquet";

        int count = processor.processEvent(new Event.UnlinkEvent.Builder().path(deleted).timestamp(200L).build(), 200L, 3L, List.of(streamingJob));
        processor.flush();

        assertEquals(1, count,
                "Удаление на источнике обязано превращаться в задачу: иначе файл остается на "
                        + "приемнике навсегда и данные двух ЦОД расходятся молча");

        ArgumentCaptor<BatchCreateTasksRequest> captor = ArgumentCaptor.forClass(BatchCreateTasksRequest.class);
        verify(orchestratorClient).batchCreateTasks(captor.capture());
        var tasks = captor.getValue().getTasks();
        assertEquals(1, tasks.size());
        assertEquals("DELETE", tasks.get(0).getTaskType(),
                "Задача удаления обязана отличаться по типу: приемник должен удалить путь, а не ждать байты");
        assertEquals(deleted, tasks.get(0).getSourcePath());
        assertEquals("/backup/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000.parquet",
                tasks.get(0).getTargetPath(),
                "Удалять нужно соответствующий путь на приемнике, а не путь источника");
    }

    @Test
    @DisplayName("UnlinkEvent: при выключенной репликации удалений ничего не ставится")
    void testUnlinkEventIgnoredWhenSyncDeletesDisabled() {
        JobDto withoutDeletes = new JobDto();
        withoutDeletes.setId("job-streaming-no-deletes");
        withoutDeletes.setSourcePath(streamingJob.getSourcePath());
        withoutDeletes.setTargetPath(streamingJob.getTargetPath());
        withoutDeletes.setSyncMode("STREAMING_INOTIFY");
        withoutDeletes.setSyncDeletes(false);

        int count = processor.processEvent(
                new Event.UnlinkEvent.Builder()
                        .path(streamingJob.getSourcePath() + "/dt=2026-10-10/part-0.parquet")
                        .timestamp(201L)
                        .build(),
                201L, 3L, List.of(withoutDeletes));
        processor.flush();

        assertEquals(0, count, "Без флага репликации удалений данные приемника трогать нельзя");
        verify(orchestratorClient, never()).batchCreateTasks(any(BatchCreateTasksRequest.class));
    }

    @Test
    @DisplayName("RenameEvent: Фиксация коммита целой партиции (директории) рекурсивно ставит все вложенные файлы")
    void testStagingCommitPartitionDirectory() throws IOException {
        String stagingDir = "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/.hive-staging_hive/dt=2026-10-10";
        String permanentPartitionDir = "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10";

        Event.RenameEvent partitionCommitEvent = new Event.RenameEvent.Builder()
                .srcPath(stagingDir)
                .dstPath(permanentPartitionDir)
                .timestamp(150L)
                .build();

        Path partitionPath = new Path(permanentPartitionDir);
        when(fileSystem.exists(partitionPath)).thenReturn(true);

        FileStatus dirStatus = mock(FileStatus.class);
        when(dirStatus.isDirectory()).thenReturn(true);
        when(fileSystem.getFileStatus(partitionPath)).thenReturn(dirStatus);

        // Внутри партиции 2 файла
        LocatedFileStatus file1 = mock(LocatedFileStatus.class);
        when(file1.getPath()).thenReturn(new Path(permanentPartitionDir + "/part-00000-data.parquet"));
        when(file1.getLen()).thenReturn(1048576L);

        LocatedFileStatus file2 = mock(LocatedFileStatus.class);
        when(file2.getPath()).thenReturn(new Path(permanentPartitionDir + "/part-00001-data.parquet"));
        when(file2.getLen()).thenReturn(2097152L);

        @SuppressWarnings("unchecked")
        RemoteIterator<LocatedFileStatus> iterator = mock(RemoteIterator.class);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(file1, file2);

        when(fileSystem.listFiles(partitionPath, true)).thenReturn(iterator);

        int count = processor.processEvent(partitionCommitEvent, 150L, 0L, List.of(streamingJob));
        assertEquals(2, count, "Оба файла внутри перемещенной партиции должны быть поставлены в очередь");

        ArgumentCaptor<BatchCreateTasksRequest> captor = ArgumentCaptor.forClass(BatchCreateTasksRequest.class);
        verify(orchestratorClient).batchCreateTasks(captor.capture());
        assertEquals("job-streaming-1", captor.getValue().getJobId());
        assertEquals(2, captor.getValue().getTasks().size());
        assertEquals("/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000-data.parquet",
                captor.getValue().getTasks().get(0).getSourcePath());
        assertEquals("/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00001-data.parquet",
                captor.getValue().getTasks().get(1).getSourcePath());
    }

    @Test
    @DisplayName("RenameEvent: Пофайловый коммит Spark ставит одиночный файл в очередь")
    void testStagingCommitSingleFile() throws IOException {
        String stagingFile = "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/_temporary/part-00000.parquet";
        String permanentFile = "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-00000.parquet";

        Event.RenameEvent singleFileCommit = new Event.RenameEvent.Builder()
                .srcPath(stagingFile)
                .dstPath(permanentFile)
                .timestamp(160L)
                .build();

        Path filePath = new Path(permanentFile);
        when(fileSystem.exists(filePath)).thenReturn(true);

        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(fileStatus.getLen()).thenReturn(8388608L);
        when(fileSystem.getFileStatus(filePath)).thenReturn(fileStatus);

        int count = processor.processEvent(singleFileCommit, 160L, 0L, List.of(streamingJob));
        assertEquals(1, count);

        verify(orchestratorClient).batchCreateTasks(any(BatchCreateTasksRequest.class));
        verify(orchestratorClient).updateJobStreamingTxid("job-streaming-1", 160L, 0L);
    }

    @Test
    @DisplayName("Backpressure: Проверка порогов насыщения (High/Low Watermark) и гистерезиса очереди")
    void testQueueSaturationAndHysteresis() {
        // Создаем процессор с емкостью 10, highWatermark = 0.8 (8 элементов), lowWatermark = 0.3 (3 элемента), flushIntervalMs = 999999 (ручной flush)
        try (InotifyBatchProcessor testProcessor = new InotifyBatchProcessor(fsManager, orchestratorClient, 10, 0.8, 0.3, 10, 999999L)) {
            assertFalse(testProcessor.isQueueSaturated(), "Изначально очередь не насыщена");
            assertEquals(0, testProcessor.getQueueSize());

            // Добавляем 7 событий (70% емкости) -> насыщение еще не должно сработать
            for (int i = 1; i <= 7; i++) {
                Event.CloseEvent ev = new Event.CloseEvent(
                        "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/file-" + i + ".parquet",
                        1024L, 100L + i
                );
                testProcessor.processEvent(ev, 100L + i, 0L, List.of(streamingJob));
            }
            assertEquals(7, testProcessor.getQueueSize());
            assertFalse(testProcessor.isQueueSaturated(), "При 7/10 (70%) насыщение не активно (< 80%)");

            // Добавляем 8-е событие -> достигнут порог highWatermark (80%)
            Event.CloseEvent ev8 = new Event.CloseEvent(
                    "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/file-8.parquet",
                    1024L, 108L
            );
            testProcessor.processEvent(ev8, 108L, 0L, List.of(streamingJob));
            assertEquals(8, testProcessor.getQueueSize());
            assertTrue(testProcessor.isQueueSaturated(), "При 8/10 (80%) активируется Backpressure");

            // Проверка гистерезиса: при частичном сбросе (например, до 5 элементов) saturation остается true
            // Так как очередь опускается ниже 8, но все еще выше lowWatermark (3)
            testProcessor.flush();
            assertEquals(0, testProcessor.getQueueSize());
            assertFalse(testProcessor.isQueueSaturated(), "После полного сброса Backpressure отключается");
        }
    }

    @Test
    @DisplayName("Micro-batching & Deduplication: События объединяются в пачки, а дубликаты путей схлопываются")
    void testMicroBatchingAndDeduplication() {
        try (InotifyBatchProcessor testProcessor = new InotifyBatchProcessor(fsManager, orchestratorClient, 100, 0.8, 0.3, 50, 999999L)) {
            // Одно и то же имя файла модифицируется 3 раза
            String repeatedFile = "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-0.parquet";
            for (int i = 1; i <= 3; i++) {
                Event.CloseEvent ev = new Event.CloseEvent(repeatedFile, 1000L * i, 200L + i);
                testProcessor.processEvent(ev, 200L + i, 10L - i, List.of(streamingJob));
            }

            // Добавляем еще один уникальный файл
            String otherFile = "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/part-1.parquet";
            Event.CloseEvent evOther = new Event.CloseEvent(otherFile, 2048L, 205L);
            testProcessor.processEvent(evOther, 205L, 0L, List.of(streamingJob));

            // До вызова flush() оркестратор не должен был вызываться повторно
            assertEquals(4, testProcessor.getQueueSize());

            int flushed = testProcessor.flush();
            // 4 события для 2 уникальных файлов дедуплицируются в 2 задачи
            assertEquals(2, flushed, "Дедупликация должна оставить 2 уникальные задачи репликации");
            assertEquals(0, testProcessor.getQueueSize());
        }
    }

    @Test
    @DisplayName("Queue Overflow: При заполнении до 100% новые события корректно отклоняются")
    void testQueueCapacityLimitOverflow() {
        // Емкость 3 элемента
        try (InotifyBatchProcessor testProcessor = new InotifyBatchProcessor(fsManager, orchestratorClient, 3, 0.8, 0.3, 10, 999999L)) {
            for (int i = 1; i <= 3; i++) {
                Event.CloseEvent ev = new Event.CloseEvent(
                        "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/file-" + i + ".parquet",
                        1024L, 300L + i
                );
                int queued = testProcessor.processEvent(ev, 300L + i, 0L, List.of(streamingJob));
                assertEquals(1, queued);
            }

            assertTrue(testProcessor.isQueueFull(), "Очередь должна быть заполнена");

            // 4-й элемент при переполнении буфера должен выбрасывать InotifyQueueOverflowException
            // для предотвращения продвижения txid и исключения потери данных
            Event.CloseEvent evOverflow = new Event.CloseEvent(
                    "/warehouse/tablespace/managed/hive/analytics.db/sales_daily/dt=2026-10-10/file-overflow.parquet",
                    1024L, 304L
            );
            assertThrows(InotifyBatchProcessor.InotifyQueueOverflowException.class, () -> {
                testProcessor.processEvent(evOverflow, 304L, 0L, List.of(streamingJob));
            }, "Переполненная очередь должна выбросить InotifyQueueOverflowException вместо тихого сброса данных");
        }
    }

    @Test
    @DisplayName("P4: HdfsInotifyListener должен совершать self-fencing при потере связи с оркестратором")
    void testStreamerSelfFencingOnOrchestratorConnectionLoss() {
        org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig config =
                new org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig();
        config.setClusterId("dc1");
        config.setAgentId("streamer-node1");

        HdfsInotifyListener listener = new HdfsInotifyListener(config, orchestratorClient, fsManager);
        assertFalse(listener.isActive());

        // Симулируем активное состояние стримера с успешным лизингом 15 секунд назад
        long staleTime = System.currentTimeMillis() - 15_000L;
        listener.setActiveForTesting(true, staleTime);
        assertTrue(listener.isActive());

        // При сбое связи с оркестратором стример должен перевести себя в STANDBY (self-fencing)
        listener.handleLeaseCheckFailure();
        assertFalse(listener.isActive(), "Стример должен совершить self-fencing и сбросить active в false");
    }

    @Test
    @DisplayName("Reconciliation Scan: Обход директории задачи и постановка файлов при пропуске событий")
    void testReconciliationScanRecoversFiles() throws IOException {
        org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig config =
                new org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig();
        config.setClusterId("dc1");
        config.setAgentId("streamer-node1");

        HdfsInotifyListener listener = new HdfsInotifyListener(config, orchestratorClient, fsManager, processor);

        Path rootPath = new Path(streamingJob.getSourcePath());
        when(fileSystem.exists(rootPath)).thenReturn(true);

        LocatedFileStatus file1 = mock(LocatedFileStatus.class);
        when(file1.getPath()).thenReturn(new Path(streamingJob.getSourcePath() + "/file-reconcile-1.parquet"));
        when(file1.getLen()).thenReturn(4096L);

        LocatedFileStatus file2 = mock(LocatedFileStatus.class);
        when(file2.getPath()).thenReturn(new Path(streamingJob.getSourcePath() + "/file-reconcile-2.parquet"));
        when(file2.getLen()).thenReturn(8192L);

        @SuppressWarnings("unchecked")
        RemoteIterator<LocatedFileStatus> iterator = mock(RemoteIterator.class);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(file1, file2);

        when(fileSystem.listFiles(rootPath, true)).thenReturn(iterator);

        int reconciled = listener.reconcileJobDirectory(streamingJob);
        assertEquals(2, reconciled, "Должно быть найдено и поставлено в очередь 2 файла");

        verify(orchestratorClient, atLeastOnce()).batchCreateTasks(any(BatchCreateTasksRequest.class));
    }
}
