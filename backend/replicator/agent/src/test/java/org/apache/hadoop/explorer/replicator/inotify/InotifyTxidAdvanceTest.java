package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Продвижение позиции в журнале транзакций NameNode.
 *
 * <p>Позиция двигалась до обработки пачки событий. Если оркестратор в этот момент недоступен,
 * список потоковых задач приходил пустым, события отбрасывались — а txid уже уехал вперед.
 * После перезапуска стример продолжал с новой позиции, и эти события не возвращались никогда.
 */
@DisplayName("Inotify: позиция журнала двигается только после обработки")
class InotifyTxidAdvanceTest {

    private OrchestratorGrpcClient orchestratorClient;
    private InotifyBatchProcessor processor;
    private HdfsInotifyListener listener;

    @BeforeEach
    void setUp() {
        ReplicatorAgentConfig config = new ReplicatorAgentConfig();
        config.setClusterId("dc1");
        config.setAgentId("streamer-dc1-1");

        orchestratorClient = mock(OrchestratorGrpcClient.class);
        processor = mock(InotifyBatchProcessor.class);
        listener = new HdfsInotifyListener(config, orchestratorClient, mock(HadoopFsManager.class), processor);
    }

    private EventBatch batch(long txid) {
        Event.CloseEvent event = new Event.CloseEvent("/warehouse/sales/part-0.parquet", 1024L, System.currentTimeMillis());
        return new EventBatch(txid, new Event[]{event});
    }

    private JobDto streamingJob() {
        JobDto job = new JobDto();
        job.setId("job-streaming-1");
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourcePath("/warehouse/sales");
        job.setTargetPath("/backup/warehouse/sales");
        job.setSyncMode("STREAMING_INOTIFY");
        return job;
    }

    @Test
    @DisplayName("Обработанная пачка двигает позицию вперед")
    void processedBatchShouldAdvanceTxid() {
        when(orchestratorClient.getJobs()).thenReturn(List.of(streamingJob()));

        assertTrue(listener.processBatch(batch(4200L), 0L));

        assertEquals(4200L, listener.getCurrentTxid());
        verify(processor).processEvent(any(), eq(4200L), anyLong(), anyList());
    }

    @Test
    @DisplayName("Оркестратор недоступен — позиция остается на месте")
    void unknownJobListShouldNotAdvanceTxid() {
        when(orchestratorClient.getJobs()).thenThrow(new RuntimeException("оркестратор недоступен"));

        assertFalse(listener.processBatch(batch(4200L), 0L),
                "Пока список задач неизвестен, события обрабатывать не с чем");

        assertEquals(0L, listener.getCurrentTxid(),
                "Позиция не должна уезжать вперед: события придется перечитать");
        verify(processor, never()).processEvent(any(), anyLong(), anyLong(), anyList());
    }

    @Test
    @DisplayName("Переполнение очереди не двигает позицию")
    void queueOverflowShouldNotAdvanceTxid() {
        when(orchestratorClient.getJobs()).thenReturn(List.of(streamingJob()));
        doThrow(new InotifyBatchProcessor.InotifyQueueOverflowException("очередь переполнена"))
                .when(processor).processEvent(any(), anyLong(), anyLong(), anyList());

        assertThrows(InotifyBatchProcessor.InotifyQueueOverflowException.class, () -> listener.processBatch(batch(4200L), 0L));

        assertEquals(0L, listener.getCurrentTxid(),
                "События не поставлены в очередь — позицию двигать нельзя");
    }

    @Test
    @DisplayName("Потоковых задач нет — позиция двигается, перечитывать нечего")
    void emptyJobListShouldStillAdvanceTxid() {
        when(orchestratorClient.getJobs()).thenReturn(List.of());

        assertTrue(listener.processBatch(batch(4200L), 0L));

        assertEquals(4200L, listener.getCurrentTxid(),
                "Список получен и пуст — события никому не нужны, и позиция должна двигаться");
    }
}
