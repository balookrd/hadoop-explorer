package io.github.balookrd.pristan.tolmach.service.engine.hive;

import io.github.balookrd.pristan.tolmach.config.SqlProperties;
import io.github.balookrd.pristan.tolmach.dto.ColumnMetadata;
import io.github.balookrd.pristan.tolmach.service.engine.HiveSqlEngine;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngine;
import io.github.balookrd.pristan.tolmach.service.engine.SqlEngineException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class HiveSqlEngineContainerTest {

    // HiveServer2 with an embedded Derby metastore and NONE authentication: hive.server2.proxy.user is
    // substituted without a proxyuser check. SessionStateUserAuthenticator makes current_user() return
    // the session (proxy) user even with doAs disabled.
    @Container
    static final GenericContainer<?> HIVE = new GenericContainer<>(DockerImageName.parse("apache/hive:4.0.0"))
            .withEnv("SERVICE_NAME", "hiveserver2")
            .withEnv("SERVICE_OPTS", "-Dhive.server2.enable.doAs=false"
                    + " -Dhive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator")
            .withExposedPorts(10000)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(4)));

    private static HiveSqlEngine engine;

    @BeforeAll
    static void createEngine() throws InterruptedException {
        SqlProperties.ClusterConfig cluster = new SqlProperties.ClusterConfig();
        cluster.setId("hive-it");
        cluster.setName("Hive IT");
        cluster.setType("hive");
        cluster.setHost(HIVE.getHost());
        cluster.setPort(HIVE.getMappedPort(10000));
        cluster.setCatalog("default");
        cluster.setSchema("default");
        engine = new HiveSqlEngine(cluster);
        awaitHiveServer2();
    }

    // The port opens before HiveServer2 accepts sessions
    private static void awaitHiveServer2() throws InterruptedException {
        SqlEngineException last = null;
        for (int attempt = 0; attempt < 60; attempt++) {
            try {
                engine.getSchemas("analyst", "default");
                return;
            } catch (SqlEngineException e) {
                last = e;
                Thread.sleep(2000);
            }
        }
        throw last;
    }

    @Test
    void catalogsListHasSingleDefaultEntry() {
        assertEquals(List.of("default"), engine.getCatalogs("analyst"));
        assertTrue(engine.getSchemas("analyst", "default").contains("default"));
    }

    @Test
    void runsQueryAsProxyUser() {
        SqlEngine.ExecutionResult result = engine.execute("h-user", "SELECT current_user()", "analyst", 10, e -> { }, new AtomicBoolean());

        assertEquals("FINISHED", result.status());
        assertEquals(List.of(List.of("analyst")), result.rows());
    }

    @Test
    void withoutImpersonationQueryRunsAsServiceUser() {
        // Proves current_user() above reflects hive.server2.proxy.user, not the JDBC user
        SqlProperties.ClusterConfig cluster = new SqlProperties.ClusterConfig();
        cluster.setId("hive-it-service");
        cluster.setName("Hive IT service user");
        cluster.setType("hive");
        cluster.setHost(HIVE.getHost());
        cluster.setPort(HIVE.getMappedPort(10000));
        cluster.getImpersonation().setEnabled(false);

        SqlEngine.ExecutionResult result = new HiveSqlEngine(cluster)
                .execute("h-service", "SELECT current_user()", "analyst", 10, e -> { }, new AtomicBoolean());

        assertEquals(List.of(List.of("pristan")), result.rows());
    }

    private static SqlProperties.ClusterConfig clusterWithSocketTimeout(int seconds) {
        SqlProperties.ClusterConfig cluster = new SqlProperties.ClusterConfig();
        cluster.setId("hive-it-short-socket");
        cluster.setName("Hive IT short socket timeout");
        cluster.setType("hive");
        cluster.setHost(HIVE.getHost());
        cluster.setPort(HIVE.getMappedPort(10000));
        cluster.getJdbc().setSocketTimeoutSeconds(seconds);
        return cluster;
    }

    @Test
    void queryLongerThanSocketTimeoutIsNotCut() {
        // ExecuteStatement is async and GetOperationStatus is long-polled (<= 5 s), so no single read waits 6 s
        HiveSqlEngine shortSocket = new HiveSqlEngine(clusterWithSocketTimeout(2));

        long start = System.nanoTime();
        SqlEngine.ExecutionResult result = shortSocket.execute("h-sleep",
                "SELECT reflect('java.lang.Thread', 'sleep', cast(6000 as bigint))", "analyst", 10, e -> { }, new AtomicBoolean());

        assertEquals("FINISHED", result.status());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() >= 6000);
    }

    @Test
    void cancelStopsRunningQuery() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean();
        CompletableFuture<SqlEngine.ExecutionResult> run = CompletableFuture.supplyAsync(() -> engine.execute("h-cancel",
                "SELECT reflect('java.lang.Thread', 'sleep', cast(60000 as bigint))", "analyst", 10, e -> { }, cancelled));
        Thread.sleep(3000);
        cancelled.set(true);

        SqlEngine.ExecutionResult result = run.get(30, TimeUnit.SECONDS);

        assertEquals("CANCELLED", result.status());
    }

    @Test
    void readsTablesAndColumnsOfCreatedTable() {
        SqlEngine.ExecutionResult ddl = engine.execute("h-ddl",
                "CREATE TABLE IF NOT EXISTS it_orders (id BIGINT, amount DECIMAL(10,2))", "analyst", 10, e -> { }, new AtomicBoolean());
        assertEquals("FINISHED", ddl.status());

        assertTrue(engine.getTables("analyst", "default", "default").contains("it_orders"));
        assertEquals(List.of("id", "amount"),
                engine.getColumns("analyst", "default", "default", "it_orders").stream().map(ColumnMetadata::name).toList());
    }
}
