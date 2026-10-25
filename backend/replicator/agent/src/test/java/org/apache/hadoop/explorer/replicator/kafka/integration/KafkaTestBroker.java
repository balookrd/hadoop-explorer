package org.apache.hadoop.explorer.replicator.kafka.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Одноузловая Kafka в контейнере для интеграционных тестов.
 *
 * <p>Контейнер поднимается через Docker CLI, а не через Testcontainers: клиент Testcontainers
 * согласует версию Docker API 1.32, тогда как современные движки требуют не ниже 1.40 и отвечают
 * на такие запросы 400 — тесты в итоге молча пропускались бы. Docker CLI не зависит от версии
 * протокола, а сам Docker в среде сборки может отсутствовать: вызывающий тест проверяет
 * {@link #isDockerAvailable()} и пропускается без него.
 */
final class KafkaTestBroker implements AutoCloseable {

    private static final String IMAGE = "apache/kafka-native:3.8.0";
    private static final long STARTUP_TIMEOUT_MS = 120_000;

    private final String containerName;
    private final int port;

    private KafkaTestBroker(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    static boolean isDockerAvailable() {
        try {
            Process process = new ProcessBuilder("docker", "info", "--format", "{{.ServerVersion}}")
                    .redirectErrorStream(true)
                    .start();
            if (!process.waitFor(20, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return false;
            }
            return process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    static KafkaTestBroker start() throws Exception {
        int port = freePort();
        String name = "replicator-kafka-test-" + UUID.randomUUID().toString().substring(0, 8);

        run("docker", "run", "-d", "--name", name, "-p", port + ":9092",
                "-e", "KAFKA_NODE_ID=1",
                "-e", "KAFKA_PROCESS_ROLES=broker,controller",
                "-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:" + port,
                "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
                "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                IMAGE);

        KafkaTestBroker broker = new KafkaTestBroker(name, port);
        broker.awaitReady();
        return broker;
    }

    String bootstrapServers() {
        return "localhost:" + port;
    }

    void createTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(60, TimeUnit.SECONDS);
        }
    }

    private void awaitReady() throws Exception {
        long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_MS;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try (AdminClient admin = AdminClient.create(Map.of(
                    "bootstrap.servers", bootstrapServers(),
                    "default.api.timeout.ms", "5000",
                    "request.timeout.ms", "5000"))) {
                admin.listTopics().names().get(10, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(500);
            }
        }
        close();
        throw new IllegalStateException("Брокер Kafka не поднялся за "
                + (STARTUP_TIMEOUT_MS / 1000) + " с", last);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void run(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        if (!process.waitFor(120, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Команда не завершилась: " + String.join(" ", command));
        }
        if (process.exitValue() != 0) {
            String output = new String(process.getInputStream().readAllBytes());
            throw new IllegalStateException("Команда завершилась с кодом " + process.exitValue()
                    + ": " + String.join(" ", command) + "\n" + output);
        }
    }

    @Override
    public void close() {
        try {
            run("docker", "rm", "-f", containerName);
        } catch (Exception ignored) {
            // контейнер мог не создаться
        }
    }
}
