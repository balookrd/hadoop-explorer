package org.apache.hadoop.explorer.replicator.sender;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Ожидание готовности исходящего gRPC-сокета перед отправкой.
 *
 * <p>Пофайловая передача вызывала {@code onNext} в цикле чтения HDFS, не спрашивая
 * {@code isReady()}. При чтении с HDFS на гигабитах и узком WAN исходящая очередь Netty
 * росла без ограничения, и отправитель падал по памяти на первом же большом файле.
 * Flow-control реализован в TAR-конвейере, здесь он вынесен в общий вид.
 */
@DisplayName("Flow-control: ожидание готовности сокета")
class StreamReadinessGateTest {

    @SuppressWarnings("unchecked")
    private static ClientCallStreamObserver<Object> readyStream(boolean ready) {
        ClientCallStreamObserver<Object> stream = mock(ClientCallStreamObserver.class);
        when(stream.isReady()).thenReturn(ready);
        return stream;
    }

    @Test
    @DisplayName("Готовый сокет пропускает отправку без ожидания")
    void testReadyStreamDoesNotBlock() throws Exception {
        StreamReadinessGate gate = StreamReadinessGate.attach(readyStream(true));

        assertNotNull(gate, "Для клиентского стрима шлюз обязан создаваться");
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), () -> gate.awaitReady(5_000));
    }

    @Test
    @DisplayName("Неготовый сокет задерживает отправку до сигнала готовности")
    void testNotReadyStreamBlocksUntilSignalled() throws Exception {
        ClientCallStreamObserver<Object> stream = readyStream(false);
        StreamReadinessGate gate = StreamReadinessGate.attach(stream);
        assertNotNull(gate);

        CountDownLatch entered = new CountDownLatch(1);
        AtomicBoolean passed = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread sender = new Thread(() -> {
            entered.countDown();
            try {
                gate.awaitReady(5_000);
                passed.set(true);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "sender");
        sender.start();

        assertTrue(entered.await(2, TimeUnit.SECONDS));
        Thread.sleep(200);
        assertFalse(passed.get(), "Отправка не должна идти, пока сокет не готов");

        // Сокет освободился: gRPC вызывает зарегистрированный обработчик готовности
        when(stream.isReady()).thenReturn(true);
        ArgumentCaptor<Runnable> handler = ArgumentCaptor.forClass(Runnable.class);
        verify(stream).setOnReadyHandler(handler.capture());
        handler.getValue().run();

        sender.join(5_000);
        assertNull(failure.get(), "Ожидание готовности не должно падать: " + failure.get());
        assertTrue(passed.get(), "После сигнала готовности отправка должна продолжиться");
    }

    @Test
    @DisplayName("Завершение вызова прекращает ожидание")
    void testTerminationStopsWaiting() throws Exception {
        ClientCallStreamObserver<Object> stream = readyStream(false);
        StreamReadinessGate gate = StreamReadinessGate.attach(stream);
        assertNotNull(gate);

        CountDownLatch finished = new CountDownLatch(1);
        Thread sender = new Thread(() -> {
            try {
                gate.awaitReady(5_000);
            } catch (IOException expected) {
                // ожидание прервано завершением вызова
            } finally {
                finished.countDown();
            }
        }, "sender");
        sender.start();
        Thread.sleep(100);

        gate.terminate();

        assertTrue(finished.await(5, TimeUnit.SECONDS),
                "Завершение вызова обязано разблокировать отправителя, иначе поток висит навсегда");
    }

    @Test
    @DisplayName("Ожидание не длится дольше заданного предела")
    void testAwaitHonoursTimeout() {
        StreamReadinessGate gate = StreamReadinessGate.attach(readyStream(false));
        assertNotNull(gate);

        long started = System.currentTimeMillis();
        assertThrows(IOException.class, () -> gate.awaitReady(300),
                "По истечении предела ожидание обязано прерваться ошибкой, а не висеть");
        assertTrue(System.currentTimeMillis() - started < 5_000);
    }

    @Test
    @DisplayName("Для неклиентского наблюдателя шлюз не создается")
    void testPlainObserverHasNoGate() {
        StreamObserver<Object> plain = new StreamObserver<>() {
            @Override public void onNext(Object value) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };

        assertNull(StreamReadinessGate.attach(plain),
                "Без клиентского стрима flow-control недоступен, и отправка идет как раньше");
    }
}
