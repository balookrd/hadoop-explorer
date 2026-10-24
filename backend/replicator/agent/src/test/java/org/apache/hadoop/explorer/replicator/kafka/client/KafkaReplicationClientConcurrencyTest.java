package org.apache.hadoop.explorer.replicator.kafka.client;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Безопасность динамического переключения топологии приемников.
 *
 * <p>Пул каналов перестраивается heartbeat-потоком (раз в несколько секунд), а воркеры
 * репликации читают его непрерывно. Если перестройка видна читателям частично (пустой пул
 * в момент пересоздания), воркер падает с {@code IndexOutOfBoundsException}, его
 * {@code catch (Exception)} гасит поток, и шард перестает реплицироваться до рестарта агента.
 */
@DisplayName("KafkaReplicationClient: конкурентная смена пула эндпоинтов")
class KafkaReplicationClientConcurrencyTest {

    private static ManagedChannel inProcessChannel(String name) {
        return InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    private static final StreamObserver<KafkaBatchAck> NOOP_ACK = new StreamObserver<>() {
        @Override public void onNext(KafkaBatchAck value) {}
        @Override public void onError(Throwable t) {}
        @Override public void onCompleted() {}
    };

    @Test
    @DisplayName("Чтение пула во время updateEndpoints не видит промежуточного пустого состояния")
    void testConcurrentUpdateDoesNotBreakReaders() throws Exception {
        KafkaReplicationClient client = new KafkaReplicationClient(
                List.of(inProcessChannel("topology-a"), inProcessChannel("topology-b")),
                List.of("127.0.0.1:19090", "127.0.0.1:19091"));

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch done = new CountDownLatch(3);

        Runnable reader = () -> {
            try {
                while (!stop.get()) {
                    int target = client.selectTargetIndex(7);
                    client.openBatchStream(target, NOOP_ACK);
                    client.getEndpointCount();
                }
            } catch (Throwable t) {
                failures.add(t);
            } finally {
                done.countDown();
            }
        };

        Thread readerOne = new Thread(reader, "pool-reader-1");
        Thread readerTwo = new Thread(reader, "pool-reader-2");
        Thread updater = new Thread(() -> {
            try {
                for (int i = 0; i < 200 && !stop.get(); i++) {
                    client.updateEndpoints(List.of("127.0.0.1:1909" + (i % 2), "127.0.0.1:1919" + (i % 2)));
                }
            } catch (Throwable t) {
                failures.add(t);
            } finally {
                stop.set(true);
                done.countDown();
            }
        }, "pool-updater");

        readerOne.start();
        readerTwo.start();
        updater.start();

        assertTrue(done.await(60, TimeUnit.SECONDS), "Потоки должны завершиться");
        readerOne.join(5_000);
        readerTwo.join(5_000);
        updater.join(5_000);

        assertTrue(failures.isEmpty(),
                "Чтение пула во время смены топологии упало: " + (failures.isEmpty() ? "" : failures.get(0)));

        client.close();
    }
}
