package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
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
    private OrchestratorClient orchestratorClient;
    private InotifyBatchProcessor processor;

    private JobDto streamingJob;

    @BeforeEach
    void setUp() throws IOException {
        fsManager = mock(HadoopFsManager.class);
        fileSystem = mock(FileSystem.class);
        when(fsManager.getFileSystem()).thenReturn(fileSystem);

        orchestratorClient = mock(OrchestratorClient.class);
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
}
