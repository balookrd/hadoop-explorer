package org.apache.hadoop.explorer.replicator.kafka.service;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaDataTransferServiceGrpc;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Предел памяти приемника на все стримы.
 *
 * <p>Каждый стрим запрашивал окно в 16 батчей сразу, а стримов на приемник приходится по
 * одному от каждого потока отправителей, чьи партиции на него выпали. При догонке хвоста
 * медленный целевой брокер держал все эти батчи в хипе, и приемник падал с OutOfMemoryError.
 */
@DisplayName("Kafka: приемник не берет батчей сверх бюджета памяти")
class ReceiverMemoryBudgetTest {

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    private static KafkaBatchRequest batch(int partition, long offset) {
        return KafkaBatchRequest.newBuilder()
                .setBatchId(UUID.randomUUID().toString())
                .setTopic("events")
                .addRecords(KafkaRecord.newBuilder()
                        .setPartition(partition)
                        .setOriginalOffset(offset)
                        .setPayload(ByteString.copyFromUtf8("v" + offset))
                        .build())
                .build();
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    @DisplayName("При исчерпанном бюджете стрим держит в работе один батч, остальные ждут")
    @SuppressWarnings("unchecked")
    void testStreamsStopRequestingWhenBudgetIsExhausted() throws Exception {
        // Брокер не отвечает, пока тест не отпустит записи
        List<Callback> pendingCallbacks = new CopyOnWriteArrayList<>();
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(inv -> {
            pendingCallbacks.add(inv.getArgument(1));
            return CompletableFuture.completedFuture(null);
        });
        KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(
                new KafkaBatchProducer(producer), null, "DC2",
                KafkaDataTransferServiceImpl.DEFAULT_PIPELINE_WINDOW, null, null, null, 1L);
        String name = "budget-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name).addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        KafkaDataTransferServiceGrpc.KafkaDataTransferServiceStub stub = KafkaDataTransferServiceGrpc.newStub(channel);

        AtomicInteger acks = new AtomicInteger();
        StreamObserver<KafkaBatchAck> ackObserver = new StreamObserver<>() {
            @Override public void onNext(KafkaBatchAck ack) {
                if (ack.getSuccess()) {
                    acks.incrementAndGet();
                }
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        StreamObserver<KafkaBatchRequest> first = stub.streamBatches(ackObserver);
        StreamObserver<KafkaBatchRequest> second = stub.streamBatches(ackObserver);
        for (int i = 0; i < 5; i++) {
            first.onNext(batch(0, i));
            second.onNext(batch(1, i));
        }

        await(() -> pendingCallbacks.size() >= 2);
        Thread.sleep(300);
        assertEquals(2, pendingCallbacks.size(),
                "сверх бюджета каждый стрим держит в работе только один батч, а взято: " + pendingCallbacks.size());
        assertTrue(service.getInFlightBytes() > 0);

        // Брокер отвечает: батчи по одному проходят до конца
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        int released = 0;
        while (acks.get() < 10 && System.nanoTime() < deadline) {
            while (released < pendingCallbacks.size()) {
                pendingCallbacks.get(released++).onCompletion(
                        new RecordMetadata(new TopicPartition("events", 0), 0L, 0, 0L, 0, 0), null);
            }
            Thread.sleep(10);
        }
        assertEquals(10, acks.get(), "после ответов брокера все батчи подтверждены");
        await(() -> service.getInFlightBytes() == 0);
        assertEquals(0L, service.getInFlightBytes(), "бюджет освобожден полностью");
        first.onCompleted();
        second.onCompleted();
    }
}
