package org.apache.hadoop.explorer.replicator.receiver;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Буфер склейки gRPC-чанков в поток байтов для распаковщика TAR-бандла.
 *
 * <p>Замена связки {@code PipedOutputStream}/{@code PipedInputStream}, непригодной для
 * gRPC-сервера с {@code newVirtualThreadPerTaskExecutor()}: {@code PipedInputStream}
 * запоминает поток-писателя при первой записи и бросает {@code IOException("Write end dead")},
 * как только тот поток завершился, а буфер опустел. При thread-per-task executor каждая
 * порция сообщений приходит в своем потоке, поэтому любая пауза в передаче роняла бандл
 * целиком — то есть весь путь репликации мелких файлов.
 *
 * <p>Здесь писателем может быть любой поток: очередь чанков не привязана к их идентичности.
 * Ограниченная емкость сохраняет backpressure исходной схемы — запись блокируется, пока
 * распаковщик не разберет накопленное.
 */
final class ChunkStreamBuffer implements Closeable {

    /** Маркер конца потока. */
    private static final byte[] END_OF_STREAM = new byte[0];

    /** Предполагаемый размер чанка для пересчета байтовой емкости в число чанков. */
    private static final int ASSUMED_CHUNK_BYTES = 64 * 1024;

    private final BlockingQueue<byte[]> chunks;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final long writeTimeoutMs;
    private volatile boolean closed;
    private volatile boolean completed;

    ChunkStreamBuffer(int bufferBytes, long writeTimeoutMs) {
        int capacity = Math.max(2, bufferBytes / ASSUMED_CHUNK_BYTES);
        this.chunks = new ArrayBlockingQueue<>(capacity);
        this.writeTimeoutMs = writeTimeoutMs > 0 ? writeTimeoutMs : 120_000L;
    }

    /** Добавление очередного чанка; блокируется, пока распаковщик не освободит место. */
    void write(byte[] chunk) throws IOException {
        if (chunk == null || chunk.length == 0) {
            return;
        }
        ensureUsable();
        try {
            if (!chunks.offer(chunk, writeTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IOException("Распаковщик TAR-бандла не успевает читать поток: таймаут "
                        + writeTimeoutMs + " мс при записи чанка");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Запись чанка прервана", e);
        }
    }

    /** Признак штатного конца потока: распаковщик получит EOF после разбора остатка. */
    void complete() throws IOException {
        ensureUsable();
        completed = true;
        try {
            if (!chunks.offer(END_OF_STREAM, writeTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IOException("Не удалось закрыть поток чанков: очередь переполнена");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Закрытие потока чанков прервано", e);
        }
    }

    /** Аварийное завершение: распаковщик получит исключение вместо EOF. */
    void fail(Throwable cause) {
        failure.compareAndSet(null, cause != null ? cause : new IOException("Поток чанков прерван"));
        chunks.clear();
        chunks.offer(END_OF_STREAM);
    }

    private void ensureUsable() throws IOException {
        if (closed) {
            throw new IOException("Поток чанков уже закрыт");
        }
        Throwable cause = failure.get();
        if (cause != null) {
            throw new IOException("Поток чанков прерван: " + cause.getMessage(), cause);
        }
    }

    /** Поток для чтения распаковщиком: EOF после complete(), исключение после fail(). */
    InputStream inputStream() {
        return new InputStream() {
            private byte[] current = END_OF_STREAM;
            private int position;
            private boolean eof;

            @Override
            public int read() throws IOException {
                byte[] single = new byte[1];
                int read = read(single, 0, 1);
                return read < 0 ? -1 : (single[0] & 0xFF);
            }

            @Override
            public int read(byte[] target, int offset, int length) throws IOException {
                if (length == 0) {
                    return 0;
                }
                if (!ensureCurrent()) {
                    return -1;
                }
                int available = current.length - position;
                int toCopy = Math.min(available, length);
                System.arraycopy(current, position, target, offset, toCopy);
                position += toCopy;
                return toCopy;
            }

            @Override
            public int available() {
                return current.length - position;
            }

            /** Подкачка следующего непустого чанка; false означает конец потока. */
            private boolean ensureCurrent() throws IOException {
                while (position >= current.length) {
                    if (eof) {
                        return false;
                    }
                    Throwable cause = failure.get();
                    if (cause != null) {
                        throw new IOException("Поток чанков прерван: " + cause.getMessage(), cause);
                    }
                    byte[] next;
                    try {
                        next = chunks.poll(writeTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Чтение потока чанков прервано", e);
                    }
                    if (next == null) {
                        if (closed && !completed) {
                            return false;
                        }
                        throw new IOException("Отправитель не передал данные за " + writeTimeoutMs + " мс");
                    }
                    if (next == END_OF_STREAM) {
                        eof = true;
                        Throwable failed = failure.get();
                        if (failed != null) {
                            throw new IOException("Поток чанков прерван: " + failed.getMessage(), failed);
                        }
                        return false;
                    }
                    current = next;
                    position = 0;
                }
                return true;
            }
        };
    }

    @Override
    public void close() {
        closed = true;
        chunks.clear();
        chunks.offer(END_OF_STREAM);
    }
}
