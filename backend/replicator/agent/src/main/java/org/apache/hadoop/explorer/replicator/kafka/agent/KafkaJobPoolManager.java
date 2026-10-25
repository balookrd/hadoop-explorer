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
            double bandwidthLimitMb
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
                              List<String> endpoints, double bandwidthLimitMb) {
        KafkaBatchConsumerPool pool() {
            return pipeline.pool();
        }
    }

    private final String localDcId;
    private final Function<JobPoolSpec, JobPipeline> poolFactory;
    private final Map<String, JobRuntime> runtimes = new ConcurrentHashMap<>();

    public KafkaJobPoolManager(String localDcId, Function<JobPoolSpec, JobPipeline> poolFactory) {
        this.localDcId = Objects.requireNonNull(localDcId, "localDcId must not be null");
        this.poolFactory = Objects.requireNonNull(poolFactory, "poolFactory must not be null");
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

            // Смена приемников требует пересборки клиента, поэтому пул поднимается заново
            if (!existing.endpoints().equals(endpoints)) {
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
                consumerGroupFor(jobId),
                localDcId,
                peerDcFor(job),
                topics,
                endpoints,
                List.copyOf(job.getConsumerGroupsList()),
                Math.max(1, job.getConsumerThreads()),
                Math.max(1, job.getBatchSize()),
                job.getBandwidthLimitMb()
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
            runtimes.put(jobId, new JobRuntime(pipeline, topics, endpoints, spec.bandwidthLimitMb()));
            log.info("Задача '{}' запущена: группа '{}', топики {}, приемники {}, потоков {}",
                    jobId, spec.groupId(), topics, endpoints, spec.consumerThreads());
        } catch (Exception e) {
            log.error("Пул задачи '{}' не запустился: {}", jobId, e.getMessage());
            closeQuietly(pipeline, jobId);
        }
    }

    private void updateJob(KafkaJobItemProto job, String jobId, List<String> topics,
                           List<String> endpoints, JobRuntime existing) {
        if (!existing.topics().equals(topics)) {
            existing.pool().updateSubscribedTopics(topics);
            log.info("Задача '{}': список топиков обновлен {} -> {}", jobId, existing.topics(), topics);
        }
        double limit = job.getBandwidthLimitMb();
        if (limit > 0 && limit != existing.bandwidthLimitMb()) {
            existing.pool().setBandwidthLimitMb(limit);
            log.info("Задача '{}': лимит полосы обновлен {} -> {} МБ/с", jobId, existing.bandwidthLimitMb(), limit);
        }
        runtimes.put(jobId, new JobRuntime(existing.pipeline(), topics, endpoints, limit > 0 ? limit : existing.bandwidthLimitMb()));
    }

    private void stopJob(String jobId) {
        JobRuntime runtime = runtimes.remove(jobId);
        if (runtime != null) {
            closeQuietly(runtime.pipeline(), jobId);
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
    private String consumerGroupFor(String jobId) {
        return "kafka-replicator-" + localDcId + "-" + jobId;
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
