package org.apache.hadoop.explorer.replicator.sender;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ожидание готовности исходящего gRPC-сокета перед отправкой очередного сообщения.
 *
 * <p>Без этой проверки отправитель вызывает {@code onNext} со скоростью чтения источника:
 * при чтении с HDFS на гигабитах и узком WAN исходящая очередь Netty растет без ограничения,
 * и агент падает по памяти на первом же большом файле. Ожидание построено на
 * {@link ReentrantLock} с {@link Condition}, а не на {@code synchronized}/{@code wait},
 * чтобы не закреплять carrier-поток при работе на виртуальных потоках.
 */
public final class StreamReadinessGate {

    private static final Logger logger = LoggerFactory.getLogger(StreamReadinessGate.class);

    private final ClientCallStreamObserver<?> stream;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readyCondition = lock.newCondition();
    private volatile boolean terminated;

    private StreamReadinessGate(ClientCallStreamObserver<?> stream) {
        this.stream = stream;
    }

    /**
     * Шлюз для наблюдателя запроса, если он поддерживает flow-control.
     *
     * @return {@code null}, если наблюдатель не является клиентским стримом (in-process тесты,
     *         нестандартные реализации) — тогда отправка идет без ожидания
     */
    public static StreamReadinessGate attach(StreamObserver<?> requestObserver) {
        if (!(requestObserver instanceof ClientCallStreamObserver<?> clientStream)) {
            return null;
        }
        StreamReadinessGate gate = new StreamReadinessGate(clientStream);
        try {
            clientStream.setOnReadyHandler(gate::signalReady);
        } catch (Exception e) {
            // Некоторые реализации запрещают регистрацию обработчика после старта вызова:
            // тогда остается проверка isReady() с ожиданием по таймауту
            logger.debug("Не удалось зарегистрировать обработчик готовности стрима: {}", e.getMessage());
        }
        return gate;
    }

    /** Сигнал gRPC о том, что сокет снова принимает данные. */
    public void signalReady() {
        lock.lock();
        try {
            readyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Прекращение ожидания при завершении или обрыве вызова. */
    public void terminate() {
        terminated = true;
        signalReady();
    }

    /**
     * Ожидание готовности сокета.
     *
     * @throws IOException если вызов завершился или сокет не освободился за отведенное время
     */
    public void awaitReady(long timeoutMs) throws IOException {
        if (stream.isReady()) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1, timeoutMs));
        lock.lock();
        try {
            while (!stream.isReady()) {
                if (terminated) {
                    throw new IOException("Передача прервана: gRPC вызов завершен");
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IOException("Исходящий сокет не освободился за " + timeoutMs
                            + " мс: приемник не успевает принимать данные");
                }
                try {
                    readyCondition.await(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)),
                            TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Ожидание готовности сокета прервано", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
