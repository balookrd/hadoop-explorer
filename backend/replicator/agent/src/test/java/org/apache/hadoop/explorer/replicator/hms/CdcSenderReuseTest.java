package org.apache.hadoop.explorer.replicator.hms;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsFieldSchemaDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.hms.service.HmsPipelineSender;
import org.apache.hadoop.explorer.replicator.hms.service.HmsTaskExecutor;
import org.apache.hadoop.explorer.replicator.hms.service.HmsTransferServiceImpl;
import org.apache.hadoop.explorer.replicator.model.HmsPendingJobDto;
import org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Канал к агенту приемника переживает опрос CDC.
 *
 * <p>Опрос идет по расписанию для каждой задачи, и на каждый вызов поднимался новый gRPC-канал:
 * разрешение имени, TCP-соединение и TLS-рукопожатие ради нескольких событий — а при пустом
 * журнале и вовсе ради ничего.
 */
@DisplayName("HMS CDC: канал к приемнику переиспользуется между опросами")
class CdcSenderReuseTest {

    private static final String SRC_DB = "analytics";
    private static final String DST_DB = "analytics_replica";

    private Server targetGrpcServer;
    private int targetGrpcPort;
    private MockHmsClient srcHmsClient;
    private MockHmsClient dstHmsClient;
    private CountingExecutor executor;

    /** Считает каналы, поднятые к агенту приемника. */
    private static class CountingExecutor extends HmsTaskExecutor {
        final AtomicInteger created = new AtomicInteger();

        CountingExecutor(MockHmsClient src, OrchestratorGrpcClient orchestrator) {
            super("agent-src-01", "dc1", src, orchestrator, false, false, 4, 100);
        }

        @Override
        protected HmsPipelineSender newPipelineSender(String targetAddress) {
            created.incrementAndGet();
            return super.newPipelineSender(targetAddress);
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            targetGrpcPort = socket.getLocalPort();
        }
        srcHmsClient = new MockHmsClient("dc1", "HDP_3.1");
        dstHmsClient = new MockHmsClient("dc2", "APACHE_3.1.3");

        targetGrpcServer = NettyServerBuilder.forPort(targetGrpcPort)
                .addService(new HmsTransferServiceImpl(dstHmsClient))
                .build()
                .start();

        OrchestratorGrpcClient orchestratorClient = new OrchestratorGrpcClient() {
            @Override
            public boolean reportHmsProgress(String jobId, HmsProgressReportRequest report) {
                return true;
            }
        };

        executor = new CountingExecutor(srcHmsClient, orchestratorClient);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) executor.close();
        if (targetGrpcServer != null) targetGrpcServer.shutdownNow();
    }

    @Test
    @DisplayName("Три опроса одной задачи поднимают один канал")
    void repeatedPollsReuseOneChannel() {
        srcHmsClient.createDatabase(SRC_DB, "hdfs://ns-hot:8020/warehouse/analytics.db");
        srcHmsClient.createTable(table(SRC_DB));
        dstHmsClient.createDatabase(DST_DB, null);

        long lastEventId = 0L;
        for (int i = 0; i < 3; i++) {
            srcHmsClient.emitEvent("CREATE_TABLE", SRC_DB, "sales_daily", "{}");
            executor.pollAndSyncCdc(job(lastEventId));
            lastEventId++;
        }

        assertEquals(1, executor.created.get(), "канал к приемнику поднимается один раз на адрес");
    }

    private HmsTableDto table(String db) {
        return new HmsTableDto("hive", db, "sales_daily", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + db + ".db/sales_daily",
                Map.of("EXTERNAL", "TRUE"), Collections.emptyList(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                List.of(new HmsFieldSchemaDto("id", "bigint")), Collections.emptyList(), Map.of());
    }

    private HmsPendingJobDto job(long lastEventId) {
        return new HmsPendingJobDto("hms-job-cdc", "dc1", "dc2", SRC_DB, DST_DB, "*",
                false, false, "127.0.0.1:" + targetGrpcPort, "ACTIVE", lastEventId);
    }
}
