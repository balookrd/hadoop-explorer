package org.apache.hadoop.explorer.replicator.kafka.consumer;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.client.KafkaReplicationClient;
import org.apache.hadoop.explorer.replicator.kafka.filter.LoopPreventionFilter;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.schema.PayloadFormatClassifier;
import org.apache.hadoop.explorer.replicator.kafka.schema.ZeroCopyWireTranscoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Высокопроизводительный многопоточный пул консьюмеров для непрерывного чтения сообщений
 * из локальной Kafka, шардирования партиций через Consumer Group, фильтрации циклов (Loop Prevention),
 * транскодинга схем и стриминга батчей в удаленный ДЦ с балансировкой по партициям (Partition-Pinned Routing).
 */
public class KafkaBatchConsumerPool implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchConsumerPool.class);

    private final List<Consumer<byte[], byte[]>> consumers;
    /** Активные воркеры: нужны остановке, чтобы зафиксировать подтвержденные оффсеты. */
    private final List<ConsumerWorker> workers = new CopyOnWriteArrayList<>();
    private final KafkaReplicationClient replicationClient;
    private final LoopPreventionFilter loopFilter;
    private final PayloadFormatClassifier formatClassifier;
    private final ZeroCopyWireTranscoder transcoder;

    private final String sourceDcId;
    private final String targetDcId;
    private final PayloadFormat defaultTopicFormat;
    private final PayloadFormat targetFormat;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<TopicPartition, Long> lastObservedTimestamps = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong totalMessagesReplicated = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong totalBytesReplicated = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong currentLagMs = new java.util.concurrent.atomic.AtomicLong(0);

    private int maxBatchSize = 2000;
    private int maxInFlightBatches = 100;
    private volatile int maxBatchRetries = 3;
    private volatile BiConsumer<KafkaBatchRequest, String> deadLetterHandler;
    private final java.util.concurrent.atomic.AtomicLong totalBatchesDeadLettered =
            new java.util.concurrent.atomic.AtomicLong(0);
    private ExecutorService executor;
    private volatile org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter bandwidthLimiter;
    private final java.util.concurrent.atomic.AtomicReference<List<String>> subscribedTopics =
            new java.util.concurrent.atomic.AtomicReference<>(Collections.emptyList());
    private final java.util.concurrent.atomic.AtomicLong topicsVersion =
            new java.util.concurrent.atomic.AtomicLong(1);

    public void setBandwidthLimiter(org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter bandwidthLimiter) {
        this.bandwidthLimiter = bandwidthLimiter;
    }

    public void setBandwidthLimitMb(double limitMb) {
        if (limitMb > 0) {
            if (this.bandwidthLimiter == null) {
                this.bandwidthLimiter = new org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter(limitMb);
            } else {
                this.bandwidthLimiter.setLimit(limitMb);
            }
        }
    }

    public org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter getBandwidthLimiter() {
        return bandwidthLimiter;
    }

    public KafkaBatchConsumerPool(Consumer<byte[], byte[]> consumer,
                                  KafkaReplicationClient replicationClient,
                                  LoopPreventionFilter loopFilter,
                                  PayloadFormatClassifier formatClassifier,
                                  ZeroCopyWireTranscoder transcoder,
                                  String sourceDcId,
                                  String targetDcId,
                                  PayloadFormat defaultTopicFormat,
                                  PayloadFormat targetFormat) {
        this(List.of(Objects.requireNonNull(consumer, "consumer must not be null")),
                replicationClient, loopFilter, formatClassifier, transcoder,
                sourceDcId, targetDcId, defaultTopicFormat, targetFormat);
    }

    public KafkaBatchConsumerPool(List<? extends Consumer<byte[], byte[]>> consumers,
                                  KafkaReplicationClient replicationClient,
                                  LoopPreventionFilter loopFilter,
                                  PayloadFormatClassifier formatClassifier,
                                  ZeroCopyWireTranscoder transcoder,
                                  String sourceDcId,
                                  String targetDcId,
                                  PayloadFormat defaultTopicFormat,
                                  PayloadFormat targetFormat) {
        if (consumers == null || consumers.isEmpty()) {
            throw new IllegalArgumentException("consumers list must not be null or empty");
        }
        this.consumers = new ArrayList<>(consumers);
        this.replicationClient = Objects.requireNonNull(replicationClient, "replicationClient must not be null");
        this.loopFilter = Objects.requireNonNull(loopFilter, "loopFilter must not be null");
        this.formatClassifier = Objects.requireNonNull(formatClassifier, "formatClassifier must not be null");
        this.transcoder = Objects.requireNonNull(transcoder, "transcoder must not be null");
        this.sourceDcId = sourceDcId;
        this.targetDcId = targetDcId;
        this.defaultTopicFormat = defaultTopicFormat;
        this.targetFormat = targetFormat;

        Set<String> initialTopics = new HashSet<>();
        for (Consumer<byte[], byte[]> c : this.consumers) {
            try {
                Set<String> sub = c.subscription();
                if (sub != null) {
                    initialTopics.addAll(sub);
                }
            } catch (Exception ignored) {
            }
        }
        if (!initialTopics.isEmpty()) {
            this.subscribedTopics.set(new ArrayList<>(initialTopics));
        }
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /**
     * Число повторных отправок батча, отвергнутого приемником (ack success=false), до изоляции.
     */
    public void setMaxBatchRetries(int maxBatchRetries) {
        this.maxBatchRetries = Math.max(0, maxBatchRetries);
    }

    public int getMaxBatchRetries() {
        return maxBatchRetries;
    }

    /**
     * Обработчик батчей, которые не удалось доставить за отведенное число попыток.
     *
     * <p>Если обработчик задан, изолированный батч считается обработанным и коммит оффсетов
     * партиции продолжается за ним. Если не задан, оффсет остается на батче: репликация
     * партиции встает, но данные не теряются.
     */
    public void setDeadLetterHandler(BiConsumer<KafkaBatchRequest, String> deadLetterHandler) {
        this.deadLetterHandler = deadLetterHandler;
    }

    public long getTotalBatchesDeadLettered() {
        return totalBatchesDeadLettered.get();
    }

    public void setMaxInFlightBatches(int maxInFlightBatches) {
        this.maxInFlightBatches = Math.max(1, maxInFlightBatches);
    }

    public int getMaxInFlightBatches() {
        return maxInFlightBatches;
    }

    public int getPoolSize() {
        return consumers.size();
    }

    /**
     * Запускает фоновый пул воркеров репликации в отдельном пуле потоков.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Запуск пула KafkaBatchConsumerPool из {} воркеров ({} -> {}, batchSize={}, targets={})",
                    consumers.size(), sourceDcId, targetDcId, maxBatchSize, replicationClient.getEndpointCount());
            if (executor == null || executor.isShutdown()) {
                executor = Executors.newFixedThreadPool(consumers.size(), new ThreadFactory() {
                    private final AtomicInteger count = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "kafka-consumer-worker-" + count.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                });
            }
            workers.clear();
            for (int i = 0; i < consumers.size(); i++) {
                ConsumerWorker worker = new ConsumerWorker(i + 1, consumers.get(i));
                workers.add(worker);
                executor.submit(worker);
            }
        }
    }

    @Override
    public void run() {
        if (consumers.size() == 1 && executor == null) {
            running.set(true);
            ConsumerWorker worker = new ConsumerWorker(1, consumers.get(0));
            workers.add(worker);
            worker.run();
        } else {
            start();
            while (running.get()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private class ConsumerWorker implements Runnable {
        private final int workerId;
        private final Consumer<byte[], byte[]> workerConsumer;
        private final Map<Integer, StreamObserver<KafkaBatchRequest>> activeStreams = new ConcurrentHashMap<>();

        /**
         * Батч, отправленный приемнику и ожидающий подтверждения.
         *
         * <p>Хранит исходный запрос: он нужен для повторной отправки при ack success=false.
         * Поле {@code settled} означает «судьба батча решена» — он либо подтвержден приемником,
         * либо изолирован в DLQ; только такие батчи позволяют продвинуть коммит оффсетов.
         */
        private final class PendingBatch {
            private final String batchId;
            private final TopicPartition topicPartition;
            private final long maxSourceOffset;
            private final int recordCount;
            private final long bytesSize;
            private final KafkaBatchRequest request;
            private int attempts;
            private volatile boolean settled;
            /** Индекс целевого приемника, на который батч отправлен в данный момент. */
            private volatile int targetIdx;

            private PendingBatch(KafkaBatchRequest request, TopicPartition topicPartition,
                                 long maxSourceOffset, int recordCount, long bytesSize) {
                this.batchId = request.getBatchId();
                this.request = request;
                this.topicPartition = topicPartition;
                this.maxSourceOffset = maxSourceOffset;
                this.recordCount = recordCount;
                this.bytesSize = bytesSize;
            }
        }

        /** Очередь батчей одной партиции в порядке отправки — основа непрерывного префикса. */
        private final class PartitionAckTracker {
            private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();
            private final ArrayDeque<PendingBatch> queue = new ArrayDeque<>();
        }

        private final Map<String, PendingBatch> inFlightBatches = new ConcurrentHashMap<>();
        private final Map<TopicPartition, PartitionAckTracker> partitionTrackers = new ConcurrentHashMap<>();
        private final Map<TopicPartition, Long> pendingCommittedOffsets = new ConcurrentHashMap<>();
        private final AtomicInteger inFlightCount = new AtomicInteger(0);
        private long localTopicsVersion = 1;

        ConsumerWorker(int workerId, Consumer<byte[], byte[]> workerConsumer) {
            this.workerId = workerId;
            this.workerConsumer = workerConsumer;
        }

        private StreamObserver<KafkaBatchRequest> getOrCreateStream(int targetIdx) {
            return activeStreams.computeIfAbsent(targetIdx, idx -> {
                StreamObserver<KafkaBatchAck> ackObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(KafkaBatchAck ack) {
                        PendingBatch pending = inFlightBatches.get(ack.getBatchId());

                        if (ack.getSuccess()) {
                            if (pending != null) {
                                inFlightBatches.remove(ack.getBatchId());
                                inFlightCount.decrementAndGet();
                                totalMessagesReplicated.addAndGet(pending.recordCount);
                                totalBytesReplicated.addAndGet(pending.bytesSize);
                                log.trace("[Worker {} -> Target {}] Успешно зафиксирован пакет {} в топике {}: {} записей ({} байт)",
                                        workerId, idx, ack.getBatchId(), ack.getTopic(), pending.recordCount, pending.bytesSize);
                                settleAndAdvance(pending);
                            } else {
                                // Повторный ack на уже учтенный батч — учитываем только счетчик приемника
                                totalMessagesReplicated.addAndGet(ack.getCommittedRecords());
                            }
                            return;
                        }

                        log.error("[Worker {} -> Target {}] Удаленный узел вернул ошибку для пакета {}: {}",
                                workerId, idx, ack.getBatchId(), ack.getErrorMessage());
                        if (pending == null) {
                            return;
                        }
                        handleRejectedBatch(pending, idx, ack.getErrorMessage());
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("[Worker {} -> Target {}] Сбой gRPC стрима: {}", workerId, idx, t.getMessage());
                        replicationClient.markTargetSuspect(idx, t);
                        activeStreams.remove(idx);
                        // Батчи оборванного стрима никогда не получат ack: без их освобождения
                        // счетчик in-flight залипает и backpressure-петля воркера ждет вечно.
                        releaseBatchesOfTarget(idx, "обрыв gRPC стрима: " + t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        log.info("[Worker {} -> Target {}] gRPC стрим закрыт удаленным узлом", workerId, idx);
                        activeStreams.remove(idx);
                    }
                };

                if (replicationClient.getEndpointCount() <= 1) {
                    return replicationClient.openBatchStream(ackObserver);
                } else {
                    return replicationClient.openBatchStream(idx, ackObserver);
                }
            });
        }

        @Override
        public void run() {
            log.info("Запущен поток ConsumerWorker-{} ({} -> {})", workerId, sourceDcId, targetDcId);

            // Переподписка с обработчиком ребаланса: вызывающая сторона подписывает консьюмер
            // без него, и отзыв партиций проходил для пула незаметно.
            List<String> startupTopics = subscribedTopics.get();
            if (startupTopics != null && !startupTopics.isEmpty()) {
                try {
                    workerConsumer.subscribe(startupTopics, rebalanceListener());
                } catch (Exception e) {
                    log.warn("[Worker {}] Не удалось переподписаться с обработчиком ребаланса: {}",
                            workerId, e.getMessage());
                }
            }

            try {
                while (running.get()) {
                    // 1. Фиксируем оффсеты, успешно подтвержденные целевым узлом через Ack
                    drainAndCommitPendingOffsets();

                    // Контроль Backpressure: притормаживаем вычитку, если буфер in-flight переполнен
                    while (running.get() && inFlightCount.get() >= maxInFlightBatches) {
                        drainAndCommitPendingOffsets();
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    if (localTopicsVersion != topicsVersion.get()) {
                        localTopicsVersion = topicsVersion.get();
                        List<String> newTopics = subscribedTopics.get();
                        drainAndCommitPendingOffsets();
                        if (newTopics != null && !newTopics.isEmpty()) {
                            workerConsumer.subscribe(newTopics, rebalanceListener());
                            log.info("[Worker {}] Динамически переподписан на топики: {}", workerId, newTopics);
                        } else {
                            workerConsumer.unsubscribe();
                            log.info("[Worker {}] Отписан от топиков (список пуст)", workerId);
                        }
                    }


                    ConsumerRecords<byte[], byte[]> records;
                    try {
                        records = workerConsumer.poll(Duration.ofMillis(100));
                    } catch (WakeupException we) {
                        if (!running.get()) {
                            break;
                        }
                        if (localTopicsVersion != topicsVersion.get()) {
                            localTopicsVersion = topicsVersion.get();
                            List<String> newTopics = subscribedTopics.get();
                            drainAndCommitPendingOffsets();
                            if (newTopics != null && !newTopics.isEmpty()) {
                                workerConsumer.subscribe(newTopics, rebalanceListener());
                                log.info("[Worker {}] Обработан сигнал Wakeup: переподписка на топики: {}", workerId, newTopics);
                            } else {
                                workerConsumer.unsubscribe();
                                log.info("[Worker {}] Обработан сигнал Wakeup: отписка от топиков", workerId);
                            }
                        }
                        continue;
                    } catch (IllegalStateException ise) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        continue;
                    }
                    if (records.isEmpty()) {
                        currentLagMs.set(0);
                        continue;
                    }

                    long now = System.currentTimeMillis();
                    long maxRecordLag = 0;

                    // Группировка строго по TopicPartition для сохранения порядка при балансировке по агентам
                    Map<TopicPartition, List<KafkaRecord>> recordsByTp = new LinkedHashMap<>();

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        lastObservedTimestamps.put(tp, record.timestamp());
                        long recLag = Math.max(0, now - record.timestamp());
                        if (recLag > maxRecordLag) {
                            maxRecordLag = recLag;
                        }

                        // 1. Проверяем на бесконечную петлю (Loop / Echo)
                        if (!loopFilter.shouldReplicate(record)) {
                            continue;
                        }

                        // 2. Определяем формат и обрабатываем полезную нагрузку
                        byte[] originalPayload = record.value();
                        boolean isTombstone = (originalPayload == null);

                        byte[] transformedPayload = originalPayload;
                        if (!isTombstone) {
                            PayloadFormat detectedFormat = formatClassifier.classify(originalPayload, defaultTopicFormat);
                            transformedPayload = transcoder.transcode(record.topic(), originalPayload, detectedFormat, targetFormat);
                        }

                        // 3. Формируем Protobuf запись с сохранением партиции и обогащением заголовков
                        KafkaRecord.Builder recBuilder = KafkaRecord.newBuilder()
                                .setPartition(record.partition())
                                .setOriginalOffset(record.offset())
                                .setTimestamp(record.timestamp())
                                .setIsTombstone(isTombstone)
                                .addAllHeaders(loopFilter.enrichHeaders(record.headers()));

                        if (record.key() != null) {
                            recBuilder.setKey(ByteString.copyFrom(record.key()));
                        }

                        if (transformedPayload != null) {
                            recBuilder.setPayload(ByteString.copyFrom(transformedPayload));
                        }

                        recordsByTp.computeIfAbsent(tp, k -> new ArrayList<>())
                                .add(recBuilder.build());
                    }
                    currentLagMs.set(maxRecordLag);

                    // 4. Отправляем батчи по партициям в соответствующие gRPC-стримы
                    for (Map.Entry<TopicPartition, List<KafkaRecord>> entry : recordsByTp.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        List<KafkaRecord> partitionRecords = entry.getValue();

                        int targetIdx = replicationClient.selectTargetIndex(tp.topic(), tp.partition());
                        StreamObserver<KafkaBatchRequest> targetStream = getOrCreateStream(targetIdx);

                        // Дробим на пачки до maxBatchSize
                        for (int i = 0; i < partitionRecords.size(); i += maxBatchSize) {
                            List<KafkaRecord> chunk = partitionRecords.subList(i, Math.min(i + maxBatchSize, partitionRecords.size()));
                            long maxChunkOffset = chunk.get(chunk.size() - 1).getOriginalOffset();

                            KafkaBatchRequest batchRequest = KafkaBatchRequest.newBuilder()
                                    .setBatchId(UUID.randomUUID().toString())
                                    .setSourceDc(sourceDcId)
                                    .setTargetDc(targetDcId)
                                    .setTopic(tp.topic())
                                    .setPayloadFormat(targetFormat)
                                    .setSendTimestampMs(System.currentTimeMillis())
                                    .addAllRecords(chunk)
                                    .build();

                            // Проверка готовности сетевого буфера gRPC (Client-side Backpressure Flow Control)
                            if (targetStream instanceof io.grpc.stub.ClientCallStreamObserver<?> clientCallStreamObserver) {
                                while (running.get() && !clientCallStreamObserver.isReady()) {
                                    drainAndCommitPendingOffsets();
                                    try {
                                        Thread.sleep(5);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        break;
                                    }
                                }
                            }

                            long batchBytes = batchRequest.getSerializedSize();
                            PendingBatch pending = registerPendingBatch(
                                    batchRequest, tp, maxChunkOffset, chunk.size(), batchBytes, targetIdx);

                            if (bandwidthLimiter != null) {
                                bandwidthLimiter.throttle(batchBytes);
                            }

                            try {
                                targetStream.onNext(batchRequest);
                            } catch (Exception ex) {
                                log.warn("[Worker {} -> Target {}] Ошибка отправки батча, failover: {}",
                                        workerId, targetIdx, ex.getMessage());
                                replicationClient.markTargetSuspect(targetIdx, ex);
                                activeStreams.remove(targetIdx);
                                int fallbackIdx = replicationClient.selectTargetIndex(tp.topic(), tp.partition());
                                // Батч уже зарегистрирован и остается в очереди партиции:
                                // переотправка не создает дубликат записи о нем.
                                pending.targetIdx = fallbackIdx;
                                getOrCreateStream(fallbackIdx).onNext(batchRequest);
                            }
                        }
                    }

                    // Фиксируем подтвержденные смещения
                    drainAndCommitPendingOffsets();
                }
            } catch (WakeupException e) {
                log.debug("[Worker {}] Получен сигнал Wakeup, штатное завершение", workerId);
            } catch (Exception e) {
                log.error("[Worker {}] Фатальная ошибка в цикле вычитки Kafka: {}", workerId, e.getMessage(), e);
            } finally {
                drainAndCommitPendingOffsets();
                for (StreamObserver<KafkaBatchRequest> s : activeStreams.values()) {
                    try {
                        s.onCompleted();
                    } catch (Exception ignored) {
                    }
                }
                activeStreams.clear();
                log.info("Остановлен рабочий поток ConsumerWorker-{}", workerId);
            }
        }

        /**
         * Регистрация отправленного батча: в карту по batchId и в хвост очереди его партиции.
         * Порядок в очереди повторяет порядок оффсетов, поэтому голова очереди — это граница
         * непрерывно подтвержденного префикса.
         */
        private PendingBatch registerPendingBatch(KafkaBatchRequest request, TopicPartition tp,
                                                  long maxSourceOffset, int recordCount, long bytesSize,
                                                  int targetIdx) {
            PendingBatch pending = new PendingBatch(request, tp, maxSourceOffset, recordCount, bytesSize);
            pending.targetIdx = targetIdx;
            inFlightBatches.put(pending.batchId, pending);
            PartitionAckTracker tracker = partitionTrackers.computeIfAbsent(tp, k -> new PartitionAckTracker());
            tracker.lock.lock();
            try {
                tracker.queue.addLast(pending);
            } finally {
                tracker.lock.unlock();
            }
            inFlightCount.incrementAndGet();
            return pending;
        }

        /**
         * Отметка батча как решенного и продвижение коммита до конца непрерывного префикса.
         *
         * <p>Коммитится {@code maxSourceOffset + 1} последнего батча непрерывной серии решенных
         * батчей. Батч, судьба которого не решена, останавливает продвижение: иначе коммит
         * перескочил бы через его записи, и при перезапуске они были бы потеряны.
         */
        private void settleAndAdvance(PendingBatch pending) {
            pending.settled = true;
            PartitionAckTracker tracker = partitionTrackers.get(pending.topicPartition);
            if (tracker == null) {
                return;
            }
            Long commitUpTo = null;
            tracker.lock.lock();
            try {
                while (!tracker.queue.isEmpty() && tracker.queue.peekFirst().settled) {
                    PendingBatch head = tracker.queue.pollFirst();
                    commitUpTo = head.maxSourceOffset + 1;
                }
            } finally {
                tracker.lock.unlock();
            }
            if (commitUpTo != null) {
                pendingCommittedOffsets.merge(pending.topicPartition, commitUpTo, Math::max);
            }
        }

        /**
         * Освобождение батчей, отправленных на указанный приемник, после обрыва его стрима.
         *
         * <p>Такие батчи никогда не получат ack, поэтому проходят обычным путем отказа:
         * повторная отправка на другой приемник, затем DLQ или блокировка коммита партиции.
         */
        private void releaseBatchesOfTarget(int targetIdx, String reason) {
            for (PendingBatch pending : new ArrayList<>(inFlightBatches.values())) {
                if (pending.targetIdx == targetIdx && !pending.settled) {
                    handleRejectedBatch(pending, targetIdx, reason);
                }
            }
        }

        /**
         * Обработка батча, отвергнутого приемником: повторная отправка, затем изоляция в DLQ.
         *
         * <p>Без DLQ-обработчика батч остается нерешенным и блокирует коммит своей партиции:
         * репликация встает, но записи не теряются. Это осознанный выбор fail-closed.
         */
        private void handleRejectedBatch(PendingBatch pending, int targetIdx, String reason) {
            if (pending.attempts < maxBatchRetries) {
                pending.attempts++;
                log.warn("[Worker {}] Повторная отправка пакета {} (попытка {} из {}): {}",
                        workerId, pending.batchId, pending.attempts, maxBatchRetries, reason);
                try {
                    int retryIdx = replicationClient.selectTargetIndex(
                            pending.topicPartition.topic(), pending.topicPartition.partition());
                    pending.targetIdx = retryIdx;
                    getOrCreateStream(retryIdx).onNext(pending.request);
                    return;
                } catch (Exception resendFailure) {
                    log.warn("[Worker {}] Не удалось переотправить пакет {}: {}",
                            workerId, pending.batchId, resendFailure.getMessage());
                    replicationClient.markTargetSuspect(targetIdx, resendFailure);
                    activeStreams.remove(targetIdx);
                }
            }

            inFlightBatches.remove(pending.batchId);
            inFlightCount.decrementAndGet();

            BiConsumer<KafkaBatchRequest, String> handler = deadLetterHandler;
            if (handler == null) {
                log.error("[Worker {}] Пакет {} партиции {} не доставлен за {} попыток и DLQ не настроен: "
                                + "фиксация оффсетов партиции остановлена во избежание потери записей",
                        workerId, pending.batchId, pending.topicPartition, maxBatchRetries);
                return;
            }

            try {
                handler.accept(pending.request, reason);
                totalBatchesDeadLettered.incrementAndGet();
                log.error("[Worker {}] Пакет {} партиции {} изолирован в DLQ после {} попыток: {}",
                        workerId, pending.batchId, pending.topicPartition, maxBatchRetries, reason);
                settleAndAdvance(pending);
            } catch (Exception dlqFailure) {
                log.error("[Worker {}] Сбой DLQ-обработчика для пакета {}: {} — фиксация оффсетов остановлена",
                        workerId, pending.batchId, dlqFailure.getMessage());
            }
        }

        /**
         * Обработчик ребаланса группы.
         *
         * <p>При отзыве партиций подтвержденный префикс фиксируется синхронно: {@code commitAsync}
         * без последующего {@code poll} до брокера не доходит, и записи приедут повторно уже
         * другому консьюмеру. Затем состояние отозванных партиций выбрасывается: их батчи ack
         * больше не получат (партиции обслуживает другой консьюмер), а без освобождения счетчик
         * in-flight залипает и петля backpressure останавливает вычитку насовсем.
         */
        private ConsumerRebalanceListener rebalanceListener() {
            return new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<TopicPartition> revoked) {
                    if (revoked == null || revoked.isEmpty()) {
                        return;
                    }
                    log.info("[Worker {}] Отзыв партиций при ребалансе: {}", workerId, revoked);
                    commitOffsetsSync(revoked);
                    releasePartitions(revoked);
                }

                @Override
                public void onPartitionsAssigned(java.util.Collection<TopicPartition> assigned) {
                    if (assigned != null && !assigned.isEmpty()) {
                        log.info("[Worker {}] Назначены партиции при ребалансе: {}", workerId, assigned);
                    }
                }

                @Override
                public void onPartitionsLost(java.util.Collection<TopicPartition> lost) {
                    // Партиции потеряны: фиксировать за них оффсеты уже нельзя, только освободить состояние
                    if (lost != null && !lost.isEmpty()) {
                        log.warn("[Worker {}] Партиции потеряны без отзыва: {}", workerId, lost);
                        releasePartitions(lost);
                    }
                }
            };
        }

        /**
         * Синхронная фиксация подтвержденного префикса. {@code partitions} равно {@code null} —
         * фиксируются все накопленные оффсеты (остановка пула).
         */
        void commitOffsetsSync(java.util.Collection<TopicPartition> partitions) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> toCommit = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : pendingCommittedOffsets.entrySet()) {
                if (partitions == null || partitions.contains(entry.getKey())) {
                    toCommit.put(entry.getKey(),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(entry.getValue()));
                }
            }
            if (toCommit.isEmpty()) {
                return;
            }
            try {
                workerConsumer.commitSync(toCommit);
                for (TopicPartition tp : toCommit.keySet()) {
                    pendingCommittedOffsets.remove(tp);
                }
                log.info("[Worker {}] Синхронно зафиксированы оффсеты: {}", workerId, toCommit);
            } catch (Exception e) {
                log.warn("[Worker {}] Не удалось синхронно зафиксировать оффсеты {}: {}",
                        workerId, toCommit, e.getMessage());
            }
        }

        /** Выбрасывание состояния партиций, которые воркер больше не обслуживает. */
        private void releasePartitions(java.util.Collection<TopicPartition> partitions) {
            for (TopicPartition tp : partitions) {
                PartitionAckTracker tracker = partitionTrackers.remove(tp);
                if (tracker != null) {
                    tracker.lock.lock();
                    try {
                        for (PendingBatch pending : tracker.queue) {
                            if (!pending.settled && inFlightBatches.remove(pending.batchId) != null) {
                                pending.settled = true;
                                inFlightCount.decrementAndGet();
                            }
                        }
                        tracker.queue.clear();
                    } finally {
                        tracker.lock.unlock();
                    }
                }
                pendingCommittedOffsets.remove(tp);
            }
        }

        private void drainAndCommitPendingOffsets() {
            if (pendingCommittedOffsets.isEmpty()) {
                return;
            }
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> toCommit = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : pendingCommittedOffsets.entrySet()) {
                toCommit.put(entry.getKey(), new org.apache.kafka.clients.consumer.OffsetAndMetadata(entry.getValue()));
            }
            if (!toCommit.isEmpty()) {
                workerConsumer.commitAsync(toCommit, (offsets, exception) -> {
                    if (exception != null) {
                        log.warn("[Worker {}] Предупреждение фиксации подтвержденных оффсетов: {}", workerId, exception.getMessage());
                    } else {
                        log.trace("[Worker {}] Успешно зафиксированы подтвержденные оффсеты: {}", workerId, offsets);
                    }
                });
            }
        }
    }

    public long getTotalMessagesReplicated() {
        return totalMessagesReplicated.get();
    }

    public long getTotalBytesReplicated() {
        return totalBytesReplicated.get();
    }

    public long getCurrentLagMs() {
        return currentLagMs.get();
    }

    public Map<TopicPartition, Long> getLastObservedTimestamps() {
        return Collections.unmodifiableMap(lastObservedTimestamps);
    }

    public void updateSubscribedTopics(List<String> newTopics) {
        if (newTopics != null) {
            List<String> sorted = new ArrayList<>(newTopics);
            Collections.sort(sorted);
            List<String> current = subscribedTopics.get();
            if (current != null) {
                List<String> sortedCurrent = new ArrayList<>(current);
                Collections.sort(sortedCurrent);
                if (sortedCurrent.equals(sorted)) {
                    return;
                }
            }
            this.subscribedTopics.set(new ArrayList<>(newTopics));
            this.topicsVersion.incrementAndGet();
            for (Consumer<byte[], byte[]> c : consumers) {
                try {
                    c.wakeup();
                } catch (Exception ignored) {
                }
            }
        }
    }

    public List<String> getSubscribedTopics() {
        return Collections.unmodifiableList(subscribedTopics.get());
    }

    @Override
    public void close() {
        running.set(false);
        // Подтвержденный префикс фиксируется до закрытия консьюмера: commitAsync без
        // последующего poll до брокера не доходит, и после перезапуска агент заново
        // гонит через WAN уже перенесенные записи.
        for (ConsumerWorker worker : workers) {
            worker.commitOffsetsSync(null);
        }
        for (Consumer<byte[], byte[]> c : consumers) {
            try {
                c.wakeup();
            } catch (Exception ignored) {
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        for (Consumer<byte[], byte[]> c : consumers) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }
}
