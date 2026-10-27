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
    /**
     * Подтвержденные приемником оффсеты по партициям — снимок на уровне пула.
     *
     * <p>Нужен переносу смещений консьюмер-групп: он должен знать, до какой точки источника
     * данные уже доставлены, иначе группа на целевом кластере будет переведена на оффсет,
     * данные до которого еще не перенесены.
     */
    private final Map<TopicPartition, Long> confirmedOffsets = new ConcurrentHashMap<>();
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

    /** Лимит задачи на этом агенте, МБ/с; 0 — без лимита. */
    private double jobBandwidthLimitMb;
    /** Потолок узла ({@code --max-bandwidth-mb-s}), МБ/с; 0 — без потолка. */
    private double hostBandwidthCapMb;

    /**
     * Лимит задачи на этом агенте. 0 снимает лимит: оркестратор пересчитывает долю агента при
     * смене состава отправителей, и снятие лимита тоже должно доезжать до живого пула.
     */
    public synchronized void setBandwidthLimitMb(double limitMb) {
        this.jobBandwidthLimitMb = Math.max(0.0, limitMb);
        applyBandwidth();
    }

    /** Потолок полосы узла: действует поверх лимита задачи и при его отсутствии. */
    public synchronized void setHostBandwidthCapMb(double capMb) {
        this.hostBandwidthCapMb = Math.max(0.0, capMb);
        applyBandwidth();
    }

    /** Действующий лимит: меньший из заданных, 0 — ограничения нет. */
    public synchronized double getEffectiveBandwidthLimitMb() {
        if (jobBandwidthLimitMb > 0 && hostBandwidthCapMb > 0) {
            return Math.min(jobBandwidthLimitMb, hostBandwidthCapMb);
        }
        return Math.max(jobBandwidthLimitMb, hostBandwidthCapMb);
    }

    private void applyBandwidth() {
        double effective = getEffectiveBandwidthLimitMb();
        if (effective <= 0) {
            this.bandwidthLimiter = null;
        } else if (this.bandwidthLimiter == null) {
            this.bandwidthLimiter = new org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter(effective);
        } else {
            this.bandwidthLimiter.setLimit(effective);
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
            /** Меняется при переотправке: ack прежней отправки после этого не учитывается. */
            private volatile String batchId;
            private final TopicPartition topicPartition;
            private final long maxSourceOffset;
            private final int recordCount;
            private final long bytesSize;
            private volatile KafkaBatchRequest request;
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
        /**
         * Партиции, снятые с вычитки из-за недоступного маршрута.
         *
         * <p>Отправлять их батчи некуда: приемников маршрута в пуле нет. Партиция ставится на
         * паузу со сдвигом позиции назад, чтобы после восстановления маршрута чтение началось с
         * первой неотправленной записи. Пропустить их и продолжить нельзя: тогда коммит
         * непрерывного префикса перешагнул бы через пропуск и записи были бы потеряны.
         */
        private final Set<TopicPartition> routingBlockedPartitions = ConcurrentHashMap.newKeySet();
        /** Наибольшее смещение в последнем отправленном батче партиции: предшественник следующего. */
        private final Map<TopicPartition, Long> lastSentMaxOffsets = new ConcurrentHashMap<>();
        /**
         * Партиции, батч которых не доставлен и не изолирован (DLQ нет): все следующие батчи
         * приемник отклонит как нарушающие порядок, поэтому вычитка партиции останавливается.
         */
        private final Set<TopicPartition> stalledPartitions = ConcurrentHashMap.newKeySet();
        private final Set<TopicPartition> pausedStalledPartitions = ConcurrentHashMap.newKeySet();
        private final AtomicInteger inFlightCount = new AtomicInteger(0);
        private long localTopicsVersion = 1;
        /** Воркер вышел из цикла и зафиксировал оффсеты: только после этого консьюмер можно закрыть. */
        private final java.util.concurrent.CountDownLatch finished = new java.util.concurrent.CountDownLatch(1);

        ConsumerWorker(int workerId, Consumer<byte[], byte[]> workerConsumer) {
            this.workerId = workerId;
            this.workerConsumer = workerConsumer;
        }

        private StreamObserver<KafkaBatchRequest> getOrCreateStream(int targetIdx) {
            if (targetIdx < 0) {
                // Защита от подстановки отсутствующего приемника: вызывающая сторона обязана
                // проверить NO_TARGET и задержать батч
                throw new IllegalStateException("Приемник для батча не выбран (NO_TARGET)");
            }
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

                        if (pending == null) {
                            // Отказ прежней отправки уже переотправленного батча
                            log.debug("[Worker {} -> Target {}] Отказ по замененному пакету {}: {}",
                                    workerId, idx, ack.getBatchId(), ack.getErrorMessage());
                            return;
                        }
                        log.error("[Worker {} -> Target {}] Удаленный узел вернул ошибку для пакета {}: {}",
                                workerId, idx, ack.getBatchId(), ack.getErrorMessage());
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

            /**
             * Записи, вычитанные опросом во время ожидания: отдаются в обработку следующей пачкой.
             * Доступ только из потока воркера, поэтому без синхронизации.
             */
            private final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> deferredRecords =
                    new LinkedHashMap<>();

            /**
             * Добавление отложенных записей к только что вычитанным.
             *
             * <p>Записи партиции, которой у воркера больше нет, отбрасываются: их перечитает новый
             * владелец с последнего зафиксированного смещения.
             */
            private ConsumerRecords<byte[], byte[]> withDeferred(ConsumerRecords<byte[], byte[]> polled) {
                if (deferredRecords.isEmpty()) {
                    return polled;
                }
                Set<TopicPartition> assigned = workerConsumer.assignment();
                Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> merged = new LinkedHashMap<>();
                for (Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> e : deferredRecords.entrySet()) {
                    if (assigned.contains(e.getKey())) {
                        merged.put(e.getKey(), new ArrayList<>(e.getValue()));
                    }
                }
                deferredRecords.clear();
                for (TopicPartition tp : polled.partitions()) {
                    merged.computeIfAbsent(tp, k -> new ArrayList<>()).addAll(polled.records(tp));
                }
                return merged.isEmpty() ? polled : new ConsumerRecords<>(merged);
            }

            /**
             * Ожидание, которое не отнимает у брокера признак жизни.
             *
             * <p>Ограничитель полосы, пустой буфер gRPC и переполненный буфер in-flight заставляют
             * ждать в том самом потоке, который вызывает {@code poll()}. Heartbeat идет отдельным
             * потоком, но {@code max.poll.interval.ms} отсчитывается именно от опроса: при узком
             * WAN пауза выходила за него, консьюмера исключали из группы, и начинался ребаланс —
             * ровно тогда, когда канал и без того перегружен.
             *
             * <p>Поэтому на время ожидания назначенные партиции ставятся на паузу, а опрос
             * продолжается: записей он не вернет, но брокер видит живого участника группы.
             * Партиции, уже приостановленные из-за отсутствующего маршрута, не возобновляются.
             */
            private void awaitInPollLoop(long waitMillis) {
                if (waitMillis <= 0) {
                    return;
                }
                long deadlineNanos = System.nanoTime() + waitMillis * 1_000_000L;
                Set<TopicPartition> toPause = new HashSet<>(workerConsumer.assignment());
                toPause.removeAll(workerConsumer.paused());
                if (!toPause.isEmpty()) {
                    workerConsumer.pause(toPause);
                }
                try {
                    while (running.get()) {
                        long leftMs = (deadlineNanos - System.nanoTime()) / 1_000_000L;
                        if (leftMs <= 0) {
                            break;
                        }
                        drainAndCommitPendingOffsets();
                        try {
                            ConsumerRecords<byte[], byte[]> stray =
                                    workerConsumer.poll(Duration.ofMillis(Math.min(50L, leftMs)));
                            if (!stray.isEmpty()) {
                                // Ребаланс мог назначить партицию уже после паузы, и опрос ожидания
                                // вернул ее записи. Обработать их здесь нельзя, выбросить — потерять:
                                // они откладываются и попадают в обработку следующей же пачкой.
                                for (TopicPartition tp : stray.partitions()) {
                                    deferredRecords.computeIfAbsent(tp, k -> new ArrayList<>())
                                            .addAll(stray.records(tp));
                                }
                            }
                        } catch (WakeupException | IllegalStateException e) {
                            break;
                        }
                    }
                } finally {
                    Set<TopicPartition> toResume = new HashSet<>(toPause);
                    toResume.retainAll(workerConsumer.assignment());
                    if (!toResume.isEmpty()) {
                        workerConsumer.resume(toResume);
                    }
                }
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
                    resumePartitionsWithRestoredRoutes();
                    pauseStalledPartitions();

                    // Контроль Backpressure: притормаживаем вычитку, если буфер in-flight переполнен
                    while (running.get() && inFlightCount.get() >= maxInFlightBatches) {
                        drainAndCommitPendingOffsets();
                        awaitInPollLoop(10);
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
                        records = withDeferred(workerConsumer.poll(Duration.ofMillis(100)));
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
                    // Записи, отброшенные фильтром петель: если в партиции за опрос не осталось
                    // ничего на перенос, подтверждать нечего, и без явной фиксации смещение
                    // стоит на месте вечно — лаг растет, а перезапуск перечитывает топик заново
                    Map<TopicPartition, Long> filteredOnlyOffsets = new HashMap<>();

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        lastObservedTimestamps.put(tp, record.timestamp());
                        long recLag = Math.max(0, now - record.timestamp());
                        if (recLag > maxRecordLag) {
                            maxRecordLag = recLag;
                        }

                        // 1. Проверяем на бесконечную петлю (Loop / Echo)
                        if (!loopFilter.shouldReplicate(record)) {
                            filteredOnlyOffsets.merge(tp, record.offset(), Math::max);
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

                    // 3.1 Партиции, где все записи отброшены фильтром: батча не будет, и
                    // смещение надо продвинуть самим. Партиции, задержанные из-за отсутствия
                    // маршрута, сюда не попадают — их записи еще предстоит перенести.
                    for (Map.Entry<TopicPartition, Long> filtered : filteredOnlyOffsets.entrySet()) {
                        TopicPartition tp = filtered.getKey();
                        if (!recordsByTp.containsKey(tp) && !routingBlockedPartitions.contains(tp)) {
                            pendingCommittedOffsets.merge(tp, filtered.getValue() + 1, Math::max);
                        }
                    }

                    // 4. Отправляем батчи по партициям в соответствующие gRPC-стримы
                    for (Map.Entry<TopicPartition, List<KafkaRecord>> entry : recordsByTp.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        List<KafkaRecord> partitionRecords = entry.getValue();

                        int targetIdx = replicationClient.selectTargetIndex(tp.topic(), tp.partition());
                        if (targetIdx == KafkaReplicationClient.NO_TARGET) {
                            blockPartitionUntilRouteAppears(tp, partitionRecords);
                            continue;
                        }
                        StreamObserver<KafkaBatchRequest> targetStream = getOrCreateStream(targetIdx);

                        // Дробим на пачки до maxBatchSize
                        for (int i = 0; i < partitionRecords.size(); i += maxBatchSize) {
                            List<KafkaRecord> chunk = partitionRecords.subList(i, Math.min(i + maxBatchSize, partitionRecords.size()));
                            long maxChunkOffset = chunk.get(chunk.size() - 1).getOriginalOffset();

                            KafkaBatchRequest.Builder batchBuilder = KafkaBatchRequest.newBuilder()
                                    .setBatchId(UUID.randomUUID().toString())
                                    .setSourceDc(sourceDcId)
                                    .setTargetDc(targetDcId)
                                    .setTopic(tp.topic())
                                    .setPayloadFormat(targetFormat)
                                    .setSendTimestampMs(System.currentTimeMillis())
                                    .addAllRecords(chunk);
                            Long prevMaxOffset = lastSentMaxOffsets.put(tp, maxChunkOffset);
                            if (prevMaxOffset != null) {
                                batchBuilder.setPrevMaxSourceOffset(prevMaxOffset);
                            }
                            KafkaBatchRequest batchRequest = batchBuilder.build();

                            // Проверка готовности сетевого буфера gRPC (Client-side Backpressure Flow Control)
                            if (targetStream instanceof io.grpc.stub.ClientCallStreamObserver<?> clientCallStreamObserver) {
                                while (running.get() && !clientCallStreamObserver.isReady()) {
                                    drainAndCommitPendingOffsets();
                                    awaitInPollLoop(10);
                                }
                            }

                            long batchBytes = batchRequest.getSerializedSize();
                            PendingBatch pending = registerPendingBatch(
                                    batchRequest, tp, maxChunkOffset, chunk.size(), batchBytes, targetIdx);

                            if (bandwidthLimiter != null) {
                                awaitInPollLoop((long) Math.ceil(bandwidthLimiter.reserve(batchBytes) * 1000.0));
                            }

                            try {
                                sendOn(targetStream, batchRequest);
                            } catch (Exception ex) {
                                log.warn("[Worker {} -> Target {}] Ошибка отправки батча, failover: {}",
                                        workerId, targetIdx, ex.getMessage());
                                replicationClient.markTargetSuspect(targetIdx, ex);
                                activeStreams.remove(targetIdx);
                                int fallbackIdx = replicationClient.selectTargetIndex(tp.topic(), tp.partition());
                                if (fallbackIdx == KafkaReplicationClient.NO_TARGET) {
                                    // Приемников маршрута нет: батч остается неподтвержденным и
                                    // блокирует фиксацию оффсетов партиции, а не уезжает в чужой кластер
                                    handleRejectedBatch(pending, targetIdx,
                                            "нет доступного приемника маршрута для топика " + tp.topic());
                                } else {
                                    // Батч уже зарегистрирован и остается в очереди партиции:
                                    // переотправка не создает дубликат записи о нем.
                                    pending.targetIdx = fallbackIdx;
                                    sendOn(getOrCreateStream(fallbackIdx), batchRequest);
                                }
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
                // Финальная фиксация — синхронно и в своем потоке: KafkaConsumer не
                // потокобезопасен, а commitAsync без последующего poll до брокера не доходит
                commitOffsetsSync(null);
                for (StreamObserver<KafkaBatchRequest> s : activeStreams.values()) {
                    try {
                        synchronized (s) {
                            s.onCompleted();
                        }
                    } catch (Exception ignored) {
                    }
                }
                activeStreams.clear();
                finished.countDown();
                log.info("Остановлен рабочий поток ConsumerWorker-{}", workerId);
            }
        }

        /**
         * Отправка батча в стрим приемника.
         *
         * <p>Клиентский {@code StreamObserver} gRPC не потокобезопасен, а пишут в него двое:
         * поток воркера и поток gRPC, который переотправляет отвергнутый приемником батч из
         * обработчика ack. Без сериализации одновременный {@code onNext} ломает стрим.
         */
        private void sendOn(StreamObserver<KafkaBatchRequest> stream, KafkaBatchRequest request) {
            synchronized (stream) {
                stream.onNext(request);
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
                confirmedOffsets.merge(pending.topicPartition, commitUpTo, Math::max);
            }
        }

        /**
         * Освобождение батчей, отправленных на указанный приемник, после обрыва его стрима.
         *
         * <p>Такие батчи никогда не получат ack, поэтому проходят обычным путем отказа. Отказ
         * обрабатывается для самого раннего батча партиции: переотправка идет от него вместе со
         * всеми следующими и сохраняет их порядок.
         */
        private void releaseBatchesOfTarget(int targetIdx, String reason) {
            for (PartitionAckTracker tracker : new ArrayList<>(partitionTrackers.values())) {
                PendingBatch earliest = null;
                tracker.lock.lock();
                try {
                    for (PendingBatch pending : tracker.queue) {
                        if (!pending.settled && pending.targetIdx == targetIdx
                                && inFlightBatches.containsKey(pending.batchId)) {
                            earliest = pending;
                            break;
                        }
                    }
                } finally {
                    tracker.lock.unlock();
                }
                if (earliest != null) {
                    handleRejectedBatch(earliest, targetIdx, reason);
                }
            }
        }

        /**
         * Обработка батча, отвергнутого приемником: повторная отправка, затем изоляция в DLQ.
         *
         * <p>Переотправляется не только отказавший батч, но и все следующие батчи его партиции,
         * по порядку и под новыми идентификаторами. Приемник после отказа отклоняет обогнавшие
         * батчи (см. {@code prev_max_source_offset}), а ack их прежних отправок отбрасываются как
         * неизвестные. Иначе повтор батча N ложился в целевой топик после уже принятого N+1.
         *
         * <p>Без DLQ-обработчика батч остается нерешенным и блокирует коммит своей партиции, а
         * вычитка партиции останавливается: репликация встает, но записи не теряются и не
         * переставляются. Это осознанный выбор fail-closed.
         */
        private void handleRejectedBatch(PendingBatch pending, int targetIdx, String reason) {
            PartitionAckTracker tracker = partitionTrackers.get(pending.topicPartition);
            if (tracker == null) {
                // Партиция уже отозвана: ее батчи выброшены вместе с состоянием
                return;
            }
            tracker.lock.lock();
            try {
                if (pending.settled || inFlightBatches.get(pending.batchId) != pending) {
                    return;
                }
                List<PendingBatch> followers = followersOf(tracker, pending);
                if (stalledPartitions.contains(pending.topicPartition)) {
                    // Перед ним уже стоит недоставленный батч: повторять бессмысленно
                    dropFromFlight(pending);
                    return;
                }
                if (pending.attempts < maxBatchRetries) {
                    pending.attempts++;
                    log.warn("[Worker {}] Повторная отправка пакета {} (попытка {} из {}) и следующих за ним: {}",
                            workerId, pending.batchId, pending.attempts, maxBatchRetries, reason);
                    try {
                        int retryIdx = replicationClient.selectTargetIndex(
                                pending.topicPartition.topic(), pending.topicPartition.partition());
                        if (retryIdx == KafkaReplicationClient.NO_TARGET) {
                            // Повторять некуда: батч остается in-flight и держит оффсеты партиции,
                            // пока приемники маршрута не появятся в пуле
                            log.warn("[Worker {}] Пакет {} партиции {} ждет приемников своего маршрута",
                                    workerId, pending.batchId, pending.topicPartition);
                            return;
                        }
                        StreamObserver<KafkaBatchRequest> stream = getOrCreateStream(retryIdx);
                        resend(pending, stream, retryIdx, false);
                        for (PendingBatch follower : followers) {
                            resend(follower, stream, retryIdx, false);
                        }
                        return;
                    } catch (Exception resendFailure) {
                        log.warn("[Worker {}] Не удалось переотправить пакет {}: {}",
                                workerId, pending.batchId, resendFailure.getMessage());
                        replicationClient.markTargetSuspect(targetIdx, resendFailure);
                        activeStreams.remove(targetIdx);
                    }
                }

                dropFromFlight(pending);

                BiConsumer<KafkaBatchRequest, String> handler = deadLetterHandler;
                if (handler == null) {
                    log.error("[Worker {}] Пакет {} партиции {} не доставлен за {} попыток и DLQ не настроен: "
                                    + "вычитка и фиксация оффсетов партиции остановлены во избежание потери записей",
                            workerId, pending.batchId, pending.topicPartition, maxBatchRetries);
                    stalledPartitions.add(pending.topicPartition);
                    followers.forEach(this::dropFromFlight);
                    return;
                }

                try {
                    handler.accept(pending.request, reason);
                    totalBatchesDeadLettered.incrementAndGet();
                    log.error("[Worker {}] Пакет {} партиции {} изолирован в DLQ после {} попыток: {}",
                            workerId, pending.batchId, pending.topicPartition, maxBatchRetries, reason);
                    settleAndAdvance(pending);
                } catch (Exception dlqFailure) {
                    log.error("[Worker {}] Сбой DLQ-обработчика для пакета {}: {} — вычитка и фиксация оффсетов остановлены",
                            workerId, pending.batchId, dlqFailure.getMessage());
                    stalledPartitions.add(pending.topicPartition);
                    followers.forEach(this::dropFromFlight);
                    return;
                }
                // Изолированный батч в целевой топик не попал: следующие начинают
                // последовательность приемника заново
                if (!followers.isEmpty()) {
                    int retryIdx = replicationClient.selectTargetIndex(
                            pending.topicPartition.topic(), pending.topicPartition.partition());
                    if (retryIdx != KafkaReplicationClient.NO_TARGET) {
                        try {
                            StreamObserver<KafkaBatchRequest> stream = getOrCreateStream(retryIdx);
                            boolean first = true;
                            for (PendingBatch follower : followers) {
                                resend(follower, stream, retryIdx, first);
                                first = false;
                            }
                        } catch (Exception resendFailure) {
                            log.warn("[Worker {}] Не удалось переотправить батчи за изолированным пакетом {}: {}",
                                    workerId, pending.batchId, resendFailure.getMessage());
                        }
                    }
                }
            } finally {
                tracker.lock.unlock();
            }
        }

        /** Нерешенные батчи партиции, отправленные после указанного. Вызывается под локом трекера. */
        private List<PendingBatch> followersOf(PartitionAckTracker tracker, PendingBatch pending) {
            List<PendingBatch> followers = new ArrayList<>();
            boolean after = false;
            for (PendingBatch queued : tracker.queue) {
                if (queued == pending) {
                    after = true;
                } else if (after && !queued.settled && inFlightBatches.get(queued.batchId) == queued) {
                    followers.add(queued);
                }
            }
            return followers;
        }

        /**
         * Переотправка батча под новым идентификатором: ack прежней отправки станет неизвестным.
         *
         * @param restartSequence не передавать предшественника: приемник начинает
         *                        последовательность партиции с этого батча
         */
        private void resend(PendingBatch pending, StreamObserver<KafkaBatchRequest> stream, int targetIdx,
                            boolean restartSequence) {
            KafkaBatchRequest.Builder builder = pending.request.toBuilder()
                    .setBatchId(UUID.randomUUID().toString());
            if (restartSequence) {
                builder.clearPrevMaxSourceOffset();
            }
            KafkaBatchRequest request = builder.build();
            inFlightBatches.remove(pending.batchId);
            pending.request = request;
            pending.batchId = request.getBatchId();
            pending.targetIdx = targetIdx;
            inFlightBatches.put(pending.batchId, pending);
            sendOn(stream, request);
        }

        /** Батч больше не ждет ack: он остается в очереди партиции и держит ее коммит. */
        private void dropFromFlight(PendingBatch pending) {
            if (inFlightBatches.remove(pending.batchId, pending)) {
                inFlightCount.decrementAndGet();
            }
        }

        /** Остановка вычитки партиций с недоставленным батчем: следующие все равно будут отклонены. */
        private void pauseStalledPartitions() {
            if (stalledPartitions.isEmpty()) {
                return;
            }
            for (TopicPartition tp : stalledPartitions) {
                if (pausedStalledPartitions.add(tp)) {
                    try {
                        workerConsumer.pause(List.of(tp));
                    } catch (Exception e) {
                        log.warn("[Worker {}] Не удалось приостановить партицию {}: {}", workerId, tp, e.getMessage());
                    }
                }
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
                try {
                    workerConsumer.commitSync(toCommit);
                } catch (WakeupException pendingWakeup) {
                    // Сигнал остановки пришел, когда воркер был вне poll(), и сработал здесь:
                    // флаг уже снят, повтор фиксирует оффсеты
                    workerConsumer.commitSync(toCommit);
                }
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
                lastSentMaxOffsets.remove(tp);
                stalledPartitions.remove(tp);
                pausedStalledPartitions.remove(tp);
            }
        }

        /**
         * Снятие партиции с вычитки, пока приемники ее маршрута не появятся в пуле.
         *
         * <p>Позиция сдвигается назад на первую неотправленную запись: иначе после
         * возобновления чтение продолжилось бы за пропущенными записями, а коммит непрерывного
         * префикса перешагнул бы через них — это и была бы потеря данных.
         */
        private void blockPartitionUntilRouteAppears(TopicPartition tp, List<KafkaRecord> skipped) {
            try {
                if (!skipped.isEmpty()) {
                    workerConsumer.seek(tp, skipped.get(0).getOriginalOffset());
                }
                workerConsumer.pause(List.of(tp));
            } catch (Exception e) {
                log.warn("[Worker {}] Не удалось приостановить партицию {}: {}", workerId, tp, e.getMessage());
            }
            if (routingBlockedPartitions.add(tp)) {
                log.error("[Worker {}] Приемники маршрута топика '{}' отсутствуют в пуле: "
                                + "вычитка партиции {} приостановлена, оффсеты не фиксируются",
                        workerId, tp.topic(), tp);
            }
        }

        /** Возврат к вычитке партиций, маршрут которых снова доступен. */
        private void resumePartitionsWithRestoredRoutes() {
            if (routingBlockedPartitions.isEmpty()) {
                return;
            }
            for (TopicPartition tp : Set.copyOf(routingBlockedPartitions)) {
                if (replicationClient.selectTargetIndex(tp.topic(), tp.partition())
                        == KafkaReplicationClient.NO_TARGET) {
                    continue;
                }
                try {
                    workerConsumer.resume(List.of(tp));
                    routingBlockedPartitions.remove(tp);
                    log.info("[Worker {}] Приемники маршрута топика '{}' появились: вычитка партиции {} возобновлена",
                            workerId, tp.topic(), tp);
                } catch (Exception e) {
                    log.warn("[Worker {}] Не удалось возобновить партицию {}: {}", workerId, tp, e.getMessage());
                }
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

    /** Снимок подтвержденных приемником оффсетов по партициям источника. */
    public Map<TopicPartition, Long> getConfirmedOffsets() {
        return Map.copyOf(confirmedOffsets);
    }

    public List<String> getSubscribedTopics() {
        return Collections.unmodifiableList(subscribedTopics.get());
    }

    @Override
    public void close() {
        running.set(false);
        // Подтвержденный префикс фиксирует сам воркер на выходе из цикла: KafkaConsumer не
        // потокобезопасен, и commitSync отсюда, пока воркер еще в poll(), падал с
        // ConcurrentModificationException — после перезапуска агент заново гнал через WAN
        // уже перенесенные записи. Поэтому здесь только сигнал остановки и ожидание воркеров.
        for (Consumer<byte[], byte[]> c : consumers) {
            try {
                c.wakeup();
            } catch (Exception ignored) {
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        // Воркер без executor (run() с одним консьюмером) работает в чужом потоке: ждем его
        // финальную фиксацию, чтобы не закрыть консьюмер у него из-под рук
        for (ConsumerWorker worker : workers) {
            try {
                if (!worker.finished.await(30, TimeUnit.SECONDS)) {
                    log.warn("Воркер {} не завершился за 30 с, консьюмер закрывается принудительно",
                            worker.workerId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
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
