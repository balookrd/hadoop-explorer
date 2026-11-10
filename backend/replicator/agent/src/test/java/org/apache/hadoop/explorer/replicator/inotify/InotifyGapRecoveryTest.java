package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Recovery after the NameNode dropped edits the streamer had not read (MissingEventsException).
 *
 * <p>The journal position was moved to the current txid before the directories were re-scanned
 * and the scan swallowed every error, so a failed scan lost the whole gap. Deletions that
 * happened inside the gap were never carried to the target.
 */
@DisplayName("Inotify: восстановление после разрыва журнала NameNode")
class InotifyGapRecoveryTest {

    private OrchestratorGrpcClient orchestratorClient;
    private InotifyBatchProcessor processor;
    private FileSystem fileSystem;
    private HdfsInotifyListener listener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws IOException {
        ReplicatorAgentConfig config = new ReplicatorAgentConfig();
        config.setClusterId("dc1");
        config.setAgentId("streamer-dc1-1");
        orchestratorClient = mock(OrchestratorGrpcClient.class);
        processor = mock(InotifyBatchProcessor.class);
        fileSystem = mock(FileSystem.class);
        HadoopFsManager fsManager = mock(HadoopFsManager.class);
        when(fsManager.getFileSystem()).thenReturn(fileSystem);
        when(fileSystem.exists(any(Path.class))).thenReturn(true);
        RemoteIterator<LocatedFileStatus> empty = mock(RemoteIterator.class);
        when(fileSystem.listFiles(any(Path.class), anyBoolean())).thenReturn(empty);
        listener = new HdfsInotifyListener(config, orchestratorClient, fsManager, processor);
    }

    private static JobDto job(String id, boolean syncDeletes) {
        JobDto job = new JobDto();
        job.setId(id);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourcePath("/warehouse/" + id);
        job.setTargetPath("/backup/" + id);
        job.setSyncMode("STREAMING_INOTIFY");
        job.setStatus("STREAMING");
        job.setSyncDeletes(syncDeletes);
        return job;
    }

    @Test
    @DisplayName("Сверка не удалась — позиция журнала остается на месте, восстановление повторится")
    void failedScanDoesNotAdvanceTxid() throws IOException {
        when(orchestratorClient.fetchJobs("STREAMING", false)).thenReturn(List.of(job("sales", false)));
        when(fileSystem.listFiles(any(Path.class), anyBoolean())).thenThrow(new IOException("NameNode в safe mode"));

        assertFalse(listener.recoverFromJournalGap(9000L));

        assertEquals(0L, listener.getCurrentTxid());
    }

    @Test
    @DisplayName("Список потоковых задач неизвестен — позиция журнала остается на месте")
    void unknownJobListDoesNotAdvanceTxid() {
        when(orchestratorClient.fetchJobs("STREAMING", false)).thenThrow(new RuntimeException("оркестратор недоступен"));

        assertFalse(listener.recoverFromJournalGap(9000L));

        assertEquals(0L, listener.getCurrentTxid());
    }

    @Test
    @DisplayName("Успешная сверка двигает позицию и переносит удаления из разрыва")
    void successfulScanAdvancesTxidAndReconcilesDeletes() {
        JobDto withDeletes = job("sales", true);
        JobDto withoutDeletes = job("logs", false);
        when(orchestratorClient.fetchJobs("STREAMING", false)).thenReturn(List.of(withDeletes, withoutDeletes));

        assertTrue(listener.recoverFromJournalGap(9000L));

        assertEquals(9000L, listener.getCurrentTxid());
        // Задача удаления на корень задания: каталог на источнике есть, поэтому воркер удалит
        // на приемнике только файлы, которых на источнике нет
        verify(processor).enqueueDelete(eq(withDeletes), eq("/warehouse/sales"), eq(9000L), anyLong());
        verify(processor, never()).enqueueDelete(eq(withoutDeletes), any(), anyLong(), anyLong());
    }
}
