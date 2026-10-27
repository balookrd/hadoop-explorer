package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Пауза и возобновление потокового задания.
 *
 * <p>Стример отбирал задания только по режиму синхронизации: пауза HMS-задачи останавливала
 * компаньон в оркестраторе, но файлы продолжали переноситься (стенд inotify).
 */
@DisplayName("Inotify: пауза и возобновление компаньона")
class InotifyPauseResumeTest {

    private OrchestratorGrpcClient orchestratorClient;
    private InotifyBatchProcessor processor;
    private FileSystem fs;
    private HdfsInotifyListener listener;

    @BeforeEach
    void setUp() throws Exception {
        ReplicatorAgentConfig config = new ReplicatorAgentConfig();
        config.setClusterId("dc1");
        orchestratorClient = mock(OrchestratorGrpcClient.class);
        processor = mock(InotifyBatchProcessor.class);
        HadoopFsManager fsManager = mock(HadoopFsManager.class);
        fs = mock(FileSystem.class);
        when(fsManager.getFileSystem()).thenReturn(fs);
        listener = new HdfsInotifyListener(config, orchestratorClient, fsManager, processor);
        listener.setReconcileExecutorForTesting(Runnable::run);
    }

    private static JobDto companion(String status) {
        JobDto job = new JobDto();
        job.setId("companion-1");
        job.setSourceClusterId("dc1");
        job.setSourcePath("hdfs://nn1:9000/warehouse/db.db");
        job.setTargetPath("hdfs://nn2:9000/warehouse/db.db");
        job.setSyncMode("STREAMING_INOTIFY");
        job.setStatus(status);
        job.setJobType("HMS_COMPANION_STREAMING");
        job.setSyncDeletes(true);
        job.setDeleteInsideTables(true);
        return job;
    }

    private static LocatedFileStatus file(String path, long mtime) {
        LocatedFileStatus status = mock(LocatedFileStatus.class);
        when(status.getPath()).thenReturn(new Path(path));
        when(status.getModificationTime()).thenReturn(mtime);
        when(status.getLen()).thenReturn(10L);
        return status;
    }

    private static RemoteIterator<LocatedFileStatus> files(LocatedFileStatus... items) {
        Iterator<LocatedFileStatus> it = List.of(items).iterator();
        return new RemoteIterator<>() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public LocatedFileStatus next() { return it.next(); }
        };
    }

    @Test
    @DisplayName("Остановленный компаньон не переносит события, возобновленный сверяет файлы с паузы")
    void stoppedCompanionIsSkippedAndResumedOneReconcilesFilesSincePause() throws Exception {
        when(orchestratorClient.getJobs("STREAMING", false)).thenReturn(List.of(companion("STOPPED")));
        assertTrue(listener.getStreamingJobs().isEmpty(), "события остановленного компаньона переносились");

        long pausedAt = System.currentTimeMillis();
        when(fs.exists(any(Path.class))).thenReturn(true);
        RemoteIterator<LocatedFileStatus> listing = files(
                file("hdfs://nn1:9000/warehouse/db.db/t/dt=1/old.parquet", pausedAt - 3_600_000L),
                file("hdfs://nn1:9000/warehouse/db.db/t/dt=1/written-on-pause.parquet", pausedAt + 1_000L));
        when(fs.listFiles(any(Path.class), eq(true))).thenReturn(listing);
        when(orchestratorClient.getJobs("STREAMING", false)).thenReturn(List.of(companion("STREAMING")));
        Thread.sleep(5_100L); // кэш списка заданий

        assertEquals(1, listener.getStreamingJobs().size());
        verify(processor).enqueueFile(any(), eq("/warehouse/db.db/t/dt=1/written-on-pause.parquet"), anyLong(), anyLong(), anyLong());
        verify(processor, never()).enqueueFile(any(), eq("/warehouse/db.db/t/dt=1/old.parquet"), anyLong(), anyLong(), anyLong());
    }

    @Test
    @DisplayName("Новый компаньон (после bootstrap) каталог не сверяет: данные уже скопированы")
    void newCompanionDoesNotReconcile() throws Exception {
        when(orchestratorClient.getJobs("STREAMING", false)).thenReturn(List.of(companion("STREAMING")));

        assertEquals(1, listener.getStreamingJobs().size());
        verify(fs, never()).listFiles(any(Path.class), anyBoolean());
    }

    @Test
    @DisplayName("Удаления на паузе переносятся после возобновления, кроме снова созданных путей")
    void deletesWhilePausedAreReplayedOnResume() throws Exception {
        when(orchestratorClient.getJobs("STREAMING", false)).thenReturn(List.of(companion("STOPPED")));
        assertTrue(listener.getStreamingJobs().isEmpty());

        String gone = "/warehouse/db.db/t/dt=1/old.parquet";
        String recreated = "/warehouse/db.db/t/dt=2/part-0.parquet";
        String tableDir = "/warehouse/db.db/t2";
        listener.processBatch(new EventBatch(100L, new Event[] {
                new Event.UnlinkEvent.Builder().path(gone).timestamp(1L).build(),
                new Event.UnlinkEvent.Builder().path(recreated).timestamp(1L).build(),
                new Event.UnlinkEvent.Builder().path(tableDir).timestamp(1L).build(),
                new Event.UnlinkEvent.Builder().path("/warehouse/other.db/t/x").timestamp(1L).build()}), 0L);
        verify(processor, never()).enqueueDelete(any(), anyString(), anyLong(), anyLong());

        when(fs.exists(any(Path.class))).thenReturn(true);
        when(fs.exists(new Path(gone))).thenReturn(false);
        when(fs.listFiles(any(Path.class), eq(true))).thenReturn(files());
        when(orchestratorClient.getJobs("STREAMING", false)).thenReturn(List.of(companion("STREAMING")));
        Thread.sleep(5_100L); // кэш списка заданий

        assertEquals(1, listener.getStreamingJobs().size());
        verify(processor).enqueueDelete(any(), eq(gone), anyLong(), anyLong());
        verify(processor, never()).enqueueDelete(any(), eq(recreated), anyLong(), anyLong());
        verify(processor, never()).enqueueDelete(any(), eq(tableDir), anyLong(), anyLong());
        verify(processor, times(1)).enqueueDelete(any(), anyString(), anyLong(), anyLong());
    }
}
