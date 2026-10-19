package org.apache.hadoop.explorer.replicator.kafka;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.generated.*;
import org.apache.hadoop.explorer.replicator.kafka.offset.SafetyCommitManager;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.service.KafkaDataTransferServiceImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("Integration Smoke Test: Multi-Agent Parallel Replication (2 In-Process Receivers)")
class KafkaMultiAgentReplicationSmokeTest {

    private Server server1;
    private Server server2;
    private ManagedChannel channel1;
    private ManagedChannel channel2;

    private final AtomicInteger server1Batches = new AtomicInteger(0);
    private final AtomicInteger server2Batches = new AtomicInteger(0);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        String serverName1 = "test-receiver-1-" + UUID.randomUUID();
        String serverName2 = "test-receiver-2-" + UUID.randomUUID();

        // Мокаем Producer и CommitManager для сервера 1
        KafkaProducer<byte[], byte[]> producerMock1 = mock(KafkaProducer.class);
        when(producerMock1.send(any(), any())).thenAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback cb = invocation.getArgument(1);
            if (cb != null) {
                cb.onCompletion(new RecordMetadata(new TopicPartition("orders", 0), 0, 0, System.currentTimeMillis(), 0, 0), null);
            }
            return CompletableFuture.completedFuture(null);
        });
        KafkaBatchProducer batchProducer1 = new KafkaBatchProducer(producerMock1);
        AdminClient admin1 = mock(AdminClient.class);
        KafkaConsumer<byte[], byte[]> cons1 = mock(KafkaConsumer.class);
        SafetyCommitManager commitManager1 = new SafetyCommitManager(admin1, cons1);

        KafkaDataTransferServiceImpl service1 = new KafkaDataTransferServiceImpl(batchProducer1, commitManager1, "DC2-NODE1") {
            @Override
            public StreamObserver<KafkaBatchRequest> streamBatches(StreamObserver<KafkaBatchAck> responseObserver) {
                StreamObserver<KafkaBatchRequest> delegate = super.streamBatches(responseObserver);
                return new StreamObserver<>() {
                    @Override
                    public void onNext(KafkaBatchRequest value) {
                        server1Batches.incrementAndGet();
                        delegate.onNext(value);
                    }
                    @Override public void onError(Throwable t) { delegate.onError(t); }
                    @Override public void onCompleted() { delegate.onCompleted(); }
                };
            }
        };

        server1 = InProcessServerBuilder.forName(serverName1).directExecutor().addService(service1).build().start();
        channel1 = InProcessChannelBuilder.forName(serverName1).directExecutor().build();

        // Мокаем Producer и CommitManager для сервера 2
        KafkaProducer<byte[], byte[]> producerMock2 = mock(KafkaProducer.class);
        when(producerMock2.send(any(), any())).thenAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback cb = invocation.getArgument(1);
            if (cb != null) {
                cb.onCompletion(new RecordMetadata(new TopicPartition("orders", 1), 0, 0, System.currentTimeMillis(), 0, 0), null);
            }
            return CompletableFuture.completedFuture(null);
        });
        KafkaBatchProducer batchProducer2 = new KafkaBatchProducer(producerMock2);
        AdminClient admin2 = mock(AdminClient.class);
        KafkaConsumer<byte[], byte[]> cons2 = mock(KafkaConsumer.class);
        SafetyCommitManager commitManager2 = new SafetyCommitManager(admin2, cons2);

        KafkaDataTransferServiceImpl service2 = new KafkaDataTransferServiceImpl(batchProducer2, commitManager2, "DC2-NODE2") {
            @Override
            public StreamObserver<KafkaBatchRequest> streamBatches(StreamObserver<KafkaBatchAck> responseObserver) {
                StreamObserver<KafkaBatchRequest> delegate = super.streamBatches(responseObserver);
                return new StreamObserver<>() {
                    @Override
                    public void onNext(KafkaBatchRequest value) {
                        server2Batches.incrementAndGet();
                        delegate.onNext(value);
                    }
                    @Override public void onError(Throwable t) { delegate.onError(t); }
                    @Override public void onCompleted() { delegate.onCompleted(); }
                };
            }
        };

        server2 = InProcessServerBuilder.forName(serverName2).directExecutor().addService(service2).build().start();
        channel2 = InProcessChannelBuilder.forName(serverName2).directExecutor().build();
    }

    @AfterEach
    void tearDown() {
        if (channel1 != null) channel1.shutdownNow();
        if (channel2 != null) channel2.shutdownNow();
        if (server1 != null) server1.shutdownNow();
        if (server2 != null) server2.shutdownNow();
    }

    @Test
    @DisplayName("Сквозная передача батчей в 2 параллельных приемника с маршрутизацией по партициям")
    void shouldRouteBatchesAcrossTwoReceiversByPartition() throws Exception {
        KafkaReplicationClient client = new KafkaReplicationClient(
                List.of(channel1, channel2),
                List.of("node1:50052", "node2:50052")
        );

        assertEquals(2, client.getEndpointCount());

        // Находим партицию, маршрутизируемую на сервер 1 (targetIdx = 0), и партицию для сервера 2 (targetIdx = 1)
        int pServer1 = 0;
        while (client.getBalancer().selectTargetIndex(pServer1) != 0) {
            pServer1++;
        }
        int pServer2 = 0;
        while (client.getBalancer().selectTargetIndex(pServer2) != 1) {
            pServer2++;
        }

        // Отправка батча для партиции сервера #1 (targetIdx = 0)
        KafkaBatchRequest batchP0 = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-p0")
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .setTopic("orders")
                .addRecords(KafkaRecord.newBuilder().setPartition(pServer1).setOriginalOffset(100L).build())
                .build();

        CompletableFuture<KafkaBatchAck> ackFuture0 = new CompletableFuture<>();
        StreamObserver<KafkaBatchRequest> stream0 = client.openBatchStreamForPartition(pServer1, new StreamObserver<>() {
            @Override public void onNext(KafkaBatchAck value) { ackFuture0.complete(value); }
            @Override public void onError(Throwable t) { ackFuture0.completeExceptionally(t); }
            @Override public void onCompleted() {}
        });
        stream0.onNext(batchP0);
        stream0.onCompleted();

        KafkaBatchAck ack0 = ackFuture0.get(5, TimeUnit.SECONDS);
        assertTrue(ack0.getSuccess());
        assertEquals("batch-p0", ack0.getBatchId());

        // Отправка батча для партиции сервера #2 (targetIdx = 1)
        KafkaBatchRequest batchP1 = KafkaBatchRequest.newBuilder()
                .setBatchId("batch-p1")
                .setSourceDc("DC1")
                .setTargetDc("DC2")
                .setTopic("orders")
                .addRecords(KafkaRecord.newBuilder().setPartition(pServer2).setOriginalOffset(200L).build())
                .build();

        CompletableFuture<KafkaBatchAck> ackFuture1 = new CompletableFuture<>();
        StreamObserver<KafkaBatchRequest> stream1 = client.openBatchStreamForPartition(pServer2, new StreamObserver<>() {
            @Override public void onNext(KafkaBatchAck value) { ackFuture1.complete(value); }
            @Override public void onError(Throwable t) { ackFuture1.completeExceptionally(t); }
            @Override public void onCompleted() {}
        });
        stream1.onNext(batchP1);
        stream1.onCompleted();

        KafkaBatchAck ack1 = ackFuture1.get(5, TimeUnit.SECONDS);
        assertTrue(ack1.getSuccess());
        assertEquals("batch-p1", ack1.getBatchId());

        // Проверяем, что сервер 1 обработал ровно батч партиции pServer1, а сервер 2 — батч партиции pServer2
        assertEquals(1, server1Batches.get(), "Сервер 1 должен был получить 1 батч (партиция " + pServer1 + ")");
        assertEquals(1, server2Batches.get(), "Сервер 2 должен был получить 1 батч (партиция " + pServer2 + ")");

        client.close();
    }
}
