package org.apache.hadoop.explorer.replicator.kafka.agent;

import org.apache.hadoop.explorer.replicator.generated.control.KafkaJobItemProto;
import org.apache.hadoop.explorer.replicator.kafka.consumer.KafkaBatchConsumerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Пулы консьюмеров по задачам репликации Kafka.
 *
 * <p>Агент держал один пул с одной consumer-группой на все задачи своего ЦОД. Следствия:
 * топик, присутствующий в двух задачах, вычитывался один раз, и вторая задача не получала
 * данных вовсе; настройки задач (размер батча, число потоков, полоса, peer-DC для фильтра
 * петель) смешивались в одни значения на всех. Здесь у каждой задачи свой пул со своей
 * группой, своими приемниками и своими настройками, а появление и исчезновение задач
 * отражается на жизненном цикле пулов.
 */
public class KafkaJobPoolManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaJobPoolManager.class);

    /** Запрос на создание пула под конкретную задачу. */
    public record JobPoolSpec(
            String jobId,
            String groupId,
            String sourceDc,
            String targetDc,
            List<String> topics,
            List<String> targetEndpoints,
            List<String> consumerGroups,
            int consumerThreads,
            int batchSize,
            double bandwidthLimitMb,
            int offsetSyncIntervalSec,
            boolean schemaTranscoding,
            String sourceSchemaRegistryUrl,
            String sourceSchemaRegistryType,
            String targetSchemaRegistryUrl,
            String targetSchemaRegistryType,
            /** С какой позиции новая группа задачи начинает чтение: LATEST или EARLIEST. */
            String startPosition,
            /** Живых отправителей ЦОД задачи; 0 — неизвестно. */
            int liveSenders
    ) {}

    /**
     * Конвейер задачи: пул консьюмеров и, если перенос смещений включен, его планировщик.
     *
     * <p>Перенос смещений привязан к задаче, а не к демону: у задачи свои группы, свой целевой
     * кластер и свой клиент приемников.
     */
    public record JobPipeline(KafkaBatchConsumerPool pool, AutoCloseable offsetSync) {}

    /** Состояние запущенной задачи: пул и то, по чему видно необходимость обновления. */
    private record JobRuntime(JobPipeline pipeline, List<String> topics,
                              List<String> endpoints, double bandwidthLimitMb,
                              int offsetSyncIntervalSec, boolean schemaTranscoding,
                              String registrySignature, int consumerThreads, int batchSize) {
        KafkaBatchConsumerPool pool() {
            return pipeline.pool();
        }
    }

    private final String localDcId;
    private final Function<JobPoolSpec, JobPipeline> poolFactory;
    private final Map<String, JobRuntime> runtimes = new ConcurrentHashMap<>();
    /**
     * Счетчики остановленных пулов. Оркестратор считает метрики агента накопительными, поэтому
     * при пересоздании пула (смена приемников, настроек) их сумма не должна проседать.
     */
    private final AtomicLong retiredMessages = new AtomicLong();
    private final AtomicLong retiredBytes = new AtomicLong();
    /** Счетчики остановленных пулов по задачам: [сообщения, байты]. */
    private final Map<String, long[]> retiredByJob = new ConcurrentHashMap<>();

    public KafkaJobPoolManager(String localDcId, Function<JobPoolSpec, JobPipeline> poolFactory) {
        this.localDcId = Objects.requireNonNull(localDcId, "localDcId must not be null");
        this.poolFactory = Objects.requireNonNull(poolFactory, "poolFactory must not be null");
    }

    /** Перенесенные записи по всем пулам агента, включая уже остановленные. */
    public long totalMessagesReplicated() {
        long total = retiredMessages.get();
        for (JobRuntime runtime : runtimes.values()) {
            total += runtime.pool().getTotalMessagesReplicated();
        }
        return total;
    }

    /** Перенесенные байты по всем пулам агента, включая уже остановленные. */
    public long totalBytesReplicated() {
        long total = retiredBytes.get();
        for (JobRuntime runtime : runtimes.values()) {
            total += runtime.pool().getTotalBytesReplicated();
        }
        return total;
    }

    /**
     * Телеметрия по задачам: накопительные счетчики с момента старта агента и текущий лаг.
     *
     * <p>Задачи, остановленные на агенте, в отчет не попадают: оркестратор помнит их последние
     * значения сам, а снятой с агента задаче новые счетчики не нужны.
     */
    public List<org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest.KafkaJobStats> jobStats() {
        List<org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest.KafkaJobStats> stats = new ArrayList<>();
        for (Map.Entry<String, JobRuntime> e : runtimes.entrySet()) {
            KafkaBatchConsumerPool pool = e.getValue().pool();
            long[] retired = retiredByJob.getOrDefault(e.getKey(), new long[2]);
            stats.add(new org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest.KafkaJobStats(
                    e.getKey(),
                    retired[0] + pool.getTotalMessagesReplicated(),
                    retired[1] + pool.getTotalBytesReplicated(),
                    pool.getCurrentLagMs()));
        }
        return stats;
    }

    /** Наибольший лаг среди запущенных пулов. */
    public long maxLagMs() {
        long max = 0L;
        for (JobRuntime runtime : runtimes.values()) {
            max = Math.max(max, runtime.pool().getCurrentLagMs());
        }
        return max;
    }

    /**
     * Число консьюмеров задачи на этом агенте.
     *
     * <p>В группе задачи партиции делятся между всеми агентами-отправителями, и консьюмер сверх
     * {@code ceil(партиций / отправителей)} простаивает, но держит свой Kerberos-логин и
     * соединения с брокерами. Неизвестные число партиций или отправителей (0) ограничением не
     * служат: тогда действует заданное число потоков.
     */
    public static int consumerThreadsFor(int configured, int partitions, int liveSenders) {
        int threads = Math.max(1, configured);
        if (partitions > 0) {
            int perAgent = (int) Math.ceil((double) partitions / Math.max(1, liveSenders));
            threads = Math.min(threads, Math.max(1, perAgent));
        }
        return threads;
    }

    /** Идентификаторы задач, пулы которых запущены. */
    public Set<String> activeJobIds() {
        return Set.copyOf(runtimes.keySet());
    }

    /**
     * Приведение набора запущенных пулов к списку активных задач.
     *
     * <p>Новая задача получает пул, исчезнувшая — останавливается, у существующей
     * обновляются топики и полоса без пересоздания пула.
     */
    public synchronized void sync(List<KafkaJobItemProto> activeJobs) {
        List<KafkaJobItemProto> jobs = activeJobs != null ? activeJobs : List.of();
        Set<String> seen = new LinkedHashSet<>();

        for (KafkaJobItemProto job : jobs) {
            String jobId = job.getJobId();
            if (jobId == null || jobId.isBlank()) {
                continue;
            }
            seen.add(jobId);

            List<String> topics = distinct(job.getTopicsList());
            List<String> endpoints = distinct(job.getTargetEndpointsList());
            if (topics.isEmpty() || endpoints.isEmpty()) {
                log.debug("Задача '{}' пропущена: нет топиков или приемников", jobId);
                continue;
            }

            JobRuntime existing = runtimes.get(jobId);
            if (existing == null) {
                startJob(job, jobId, topics, endpoints);
                continue;
            }

            // Смена приемников требует пересборки клиента, поэтому пул поднимается заново.
            // Сравнивается состав, а не порядок: оркестратор собирает список из хеш-таблицы,
            // и перестановка без изменения состава перезапускала бы пулы и группу целиком.
            if (!Set.copyOf(existing.endpoints()).equals(Set.copyOf(endpoints))) {
                log.info("Задача '{}': сменились приемники {} -> {}, пул перезапускается",
                        jobId, existing.endpoints(), endpoints);
                stopJob(jobId);
                startJob(job, jobId, topics, endpoints);
                continue;
            }

            updateJob(job, jobId, topics, endpoints, existing);
        }

        for (String jobId : List.copyOf(runtimes.keySet())) {
            if (!seen.contains(jobId)) {
                log.info("Задача '{}' больше не активна: пул консьюмеров останавливается", jobId);
                stopJob(jobId);
            }
        }
    }

    private void startJob(KafkaJobItemProto job, String jobId, List<String> topics, List<String> endpoints) {
        JobPoolSpec spec = new JobPoolSpec(
                jobId,
                consumerGroupFor(job, jobId),
                localDcFor(job),
                peerDcFor(job),
                topics,
                endpoints,
                List.copyOf(job.getConsumerGroupsList()),
                Math.max(1, job.getConsumerThreads()),
                Math.max(1, job.getBatchSize()),
                job.getBandwidthLimitMb(),
                Math.max(0, job.getOffsetSyncIntervalSec()),
                job.getSchemaTranscoding(),
                job.getSourceSchemaRegistryUrl(),
                job.getSourceSchemaRegistryType(),
                job.getTargetSchemaRegistryUrl(),
                job.getTargetSchemaRegistryType(),
                job.getStartPosition(),
                Math.max(0, job.getLiveSenders())
        );

        JobPipeline pipeline;
        try {
            pipeline = poolFactory.apply(spec);
        } catch (Exception e) {
            log.error("Не удалось создать пул консьюмеров для задачи '{}': {}", jobId, e.getMessage());
            return;
        }
        if (pipeline == null || pipeline.pool() == null) {
            return;
        }

        try {
            pipeline.pool().start();
            runtimes.put(jobId, new JobRuntime(pipeline, topics, endpoints, spec.bandwidthLimitMb(),
                    spec.offsetSyncIntervalSec(), spec.schemaTranscoding(), registrySignature(job),
                    spec.consumerThreads(), spec.batchSize()));
            log.info("Задача '{}' запущена: группа '{}', топики {}, приемники {}, потоков {}",
                    jobId, spec.groupId(), topics, endpoints, spec.consumerThreads());
        } catch (Exception e) {
            log.error("Пул задачи '{}' не запустился: {}", jobId, e.getMessage());
            closeQuietly(pipeline, jobId);
        }
    }

    private void updateJob(KafkaJobItemProto job, String jobId, List<String> topics,
                           List<String> endpoints, JobRuntime existing) {
        // Транскодер схем, перенос смещений, число консьюмеров и размер батча (он же
        // max.poll.records) задаются при создании конвейера, поэтому их нельзя поменять на
        // живом пуле: задача пересоздается целиком. Static membership возвращает консьюмеры
        // в группу под теми же идентификаторами, без ребаланса всей группы.
        int offsetSyncInterval = Math.max(0, job.getOffsetSyncIntervalSec());
        boolean transcoding = job.getSchemaTranscoding();
        String registries = registrySignature(job);
        int threads = Math.max(1, job.getConsumerThreads());
        int batchSize = Math.max(1, job.getBatchSize());
        if (offsetSyncInterval != existing.offsetSyncIntervalSec()
                || transcoding != existing.schemaTranscoding()
                || !registries.equals(existing.registrySignature())
                || threads != existing.consumerThreads()
                || batchSize != existing.batchSize()) {
            log.info("Задача '{}': изменились настройки конвейера (перенос смещений {} -> {} с, "
                            + "транскодинг {} -> {}, потоков {} -> {}, батч {} -> {}), пул пересоздается",
                    jobId, existing.offsetSyncIntervalSec(), offsetSyncInterval,
                    existing.schemaTranscoding(), transcoding,
                    existing.consumerThreads(), threads, existing.batchSize(), batchSize);
            stopJob(jobId);
            startJob(job, jobId, topics, endpoints);
            return;
        }

        if (!existing.topics().equals(topics)) {
            existing.pool().updateSubscribedTopics(topics);
            log.info("Задача '{}': список топиков обновлен {} -> {}", jobId, existing.topics(), topics);
        }
        // Доля агента в лимите задачи меняется вместе с числом отправителей ЦОД; 0 — лимит снят
        double limit = Math.max(0.0, job.getBandwidthLimitMb());
        if (Math.abs(limit - existing.bandwidthLimitMb()) > 1e-6) {
            existing.pool().setBandwidthLimitMb(limit);
            log.info("Задача '{}': лимит полосы агента {} -> {} МБ/с", jobId, existing.bandwidthLimitMb(),
                    limit > 0 ? limit : "без лимита");
        }
        runtimes.put(jobId, new JobRuntime(existing.pipeline(), topics, endpoints,
                limit,
                existing.offsetSyncIntervalSec(), existing.schemaTranscoding(),
                existing.registrySignature(), existing.consumerThreads(), existing.batchSize()));
    }

    private void stopJob(String jobId) {
        JobRuntime runtime = runtimes.remove(jobId);
        if (runtime != null) {
            closeQuietly(runtime.pipeline(), jobId);
            KafkaBatchConsumerPool pool = runtime.pool();
            if (pool != null) {
                // Пересозданный пул задачи считает с нуля: прошлое копится здесь
                retiredByJob.merge(jobId,
                        new long[]{pool.getTotalMessagesReplicated(), pool.getTotalBytesReplicated()},
                        (a, b) -> new long[]{a[0] + b[0], a[1] + b[1]});
            }
        }
    }

    private void closeQuietly(JobPipeline pipeline, String jobId) {
        if (pipeline == null) {
            return;
        }
        if (pipeline.offsetSync() != null) {
            try {
                pipeline.offsetSync().close();
            } catch (Exception e) {
                log.warn("Ошибка остановки переноса смещений задачи '{}': {}", jobId, e.getMessage());
            }
        }
        if (pipeline.pool() != null) {
            try {
                pipeline.pool().close();
                retiredMessages.addAndGet(pipeline.pool().getTotalMessagesReplicated());
                retiredBytes.addAndGet(pipeline.pool().getTotalBytesReplicated());
            } catch (Exception e) {
                log.warn("Ошибка остановки пула задачи '{}': {}", jobId, e.getMessage());
            }
        }
    }

    /**
     * Группа консьюмеров задачи.
     *
     * <p>Идентификатор задачи входит в имя группы: именно это дает независимую вычитку
     * топика, присутствующего в нескольких задачах.
     */
    /** Реестры задачи одной строкой: по ней видно, нужно ли пересоздавать транскодер. */
    private static String registrySignature(KafkaJobItemProto job) {
        return job.getSourceSchemaRegistryUrl() + "|" + job.getSourceSchemaRegistryType()
                + "->" + job.getTargetSchemaRegistryUrl() + "|" + job.getTargetSchemaRegistryType();
    }

    /**
     * Консьюмер-группа задачи. ЦОД берется в написании задачи, а не из {@code --dc-id}
     * агента: оркестратор сопоставляет ЦОД без учета регистра, и агенты одного ЦОД с флагами
     * {@code DC1} и {@code dc1} попадали в разные группы — каждая читала все партиции, и записи
     * переносились дважды.
     */
    static String consumerGroupFor(KafkaJobItemProto job, String localDcId, String jobId) {
        return "kafka-replicator-" + canonicalLocalDc(job, localDcId) + "-" + jobId;
    }

    private String consumerGroupFor(KafkaJobItemProto job, String jobId) {
        return consumerGroupFor(job, localDcId, jobId);
    }

    private String localDcFor(KafkaJobItemProto job) {
        return canonicalLocalDc(job, localDcId);
    }

    /** Локальный ЦОД в написании задачи: тот из ее ЦОД, что совпадает с агентом без учета регистра. */
    static String canonicalLocalDc(KafkaJobItemProto job, String localDcId) {
        if (job.getSourceDc().equalsIgnoreCase(localDcId)) {
            return job.getSourceDc();
        }
        if (job.getTargetDc().equalsIgnoreCase(localDcId)) {
            return job.getTargetDc();
        }
        return localDcId;
    }

    /** Peer-DC задачи: для обратного направления BIDIRECTIONAL им оказывается ЦОД источника. */
    private String peerDcFor(KafkaJobItemProto job) {
        String sourceDc = job.getSourceDc();
        String targetDc = job.getTargetDc();
        if (localDcId.equalsIgnoreCase(targetDc) && sourceDc != null && !sourceDc.isBlank()) {
            return sourceDc;
        }
        return (targetDc != null && !targetDc.isBlank()) ? targetDc : sourceDc;
    }

    private static List<String> distinct(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>(new LinkedHashSet<>(values.stream()
                .filter(v -> v != null && !v.isBlank())
                .toList()));
        return List.copyOf(result);
    }

    @Override
    public synchronized void close() {
        for (String jobId : List.copyOf(runtimes.keySet())) {
            stopJob(jobId);
        }
    }
}
