package org.apache.hadoop.explorer.spark.service.engine;

import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.service.engine.livy.LivyAuthHeaderProvider;
import org.apache.hadoop.explorer.spark.service.engine.livy.LivySparkEngine;
import org.apache.hadoop.explorer.spark.service.engine.livy.SpnegoAuthHeaderProvider;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkEngineRegistryTest {

    private static SparkProperties.SparkClusterConfig cluster(String id, String livyUrl, boolean mockStorage) {
        SparkProperties.SparkClusterConfig cluster = new SparkProperties.SparkClusterConfig();
        cluster.setId(id);
        cluster.setLivyUrl(livyUrl);
        cluster.setMockStorage(mockStorage);
        return cluster;
    }

    private static SparkProperties properties(SparkProperties.SparkClusterConfig... clusters) {
        SparkProperties properties = new SparkProperties();
        properties.setClusters(List.of(clusters));
        return properties;
    }

    @Test
    void mockStorageClusterGetsEmulator() {
        SparkProperties.SparkClusterConfig cluster = cluster("demo", null, true);
        SparkEngineRegistry registry = new SparkEngineRegistry(properties(cluster));
        assertSame(registry.mock(), registry.forCluster(cluster));
    }

    @Test
    void realLivyClusterGetsLivyEngineOncePerCluster() {
        SparkProperties.SparkClusterConfig cluster = cluster("prod", "http://livy.example:8998", false);
        SparkEngineRegistry registry = new SparkEngineRegistry(properties(cluster));
        SparkEngine engine = registry.forCluster(cluster);
        assertInstanceOf(LivySparkEngine.class, engine);
        assertSame(engine, registry.forCluster(cluster));
    }

    @Test
    void realClusterWithoutLivyUrlFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster("prod", null, false))));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy-url"), e.getMessage());
        assertTrue(e.getMessage().contains("mock-storage: true"), e.getMessage());
    }

    @Test
    void livyUrlWithoutSchemeFailsAtStartup() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster("prod", "livy.example:8998", false))));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy-url"), e.getMessage());
    }

    @Test
    void credentialsInLivyUrlAreRejectedWithoutEchoingThem() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster("prod", "http://svc:topsecret@livy.example:8998", false))));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy-url"), e.getMessage());
        assertTrue(!e.getMessage().contains("topsecret"), e.getMessage());
    }

    @Test
    void unknownAuthFailsAtStartup() {
        SparkProperties.SparkClusterConfig cluster = cluster("prod", "http://livy.example:8998", false);
        cluster.getLivy().setAuth("kerberos");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster)));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy.auth"), e.getMessage());
    }

    @Test
    void authModes() {
        SparkProperties.SparkClusterConfig cluster = spnegoCluster();
        SparkEngineRegistry registry = new SparkEngineRegistry(properties(cluster));
        assertInstanceOf(SpnegoAuthHeaderProvider.class, registry.authFor(cluster));
        assertInstanceOf(LivySparkEngine.class, registry.forCluster(cluster));

        cluster.getLivy().setAuth("none");
        assertSame(LivyAuthHeaderProvider.NONE, registry.authFor(cluster));

        cluster.getLivy().setAuth("basic");
        assertThrows(IllegalStateException.class, () -> registry.authFor(cluster));
    }

    @Test
    void spnegoWithoutPrincipalFailsAtStartup() {
        SparkProperties.SparkClusterConfig cluster = spnegoCluster();
        cluster.getLivy().setPrincipal(" ");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster)));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy.principal"), e.getMessage());
    }

    @Test
    void spnegoWithoutKeytabFailsAtStartup() {
        SparkProperties.SparkClusterConfig cluster = spnegoCluster();
        cluster.getLivy().setKeytab(null);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster)));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy.keytab"), e.getMessage());
    }

    @Test
    void spnegoWithRelativeKeytabFailsAtStartup() {
        SparkProperties.SparkClusterConfig cluster = spnegoCluster();
        cluster.getLivy().setKeytab("spark-explorer.keytab");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster)));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy.keytab"), e.getMessage());
        assertTrue(e.getMessage().contains("абсолютным"), e.getMessage());
    }

    @Test
    void spnegoWithNonPositiveReloginIntervalFailsAtStartup() {
        SparkProperties.SparkClusterConfig cluster = spnegoCluster();
        cluster.getLivy().setReloginIntervalMinutes(0);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster)));
        assertTrue(e.getMessage().contains("spark.clusters[prod].livy.relogin-interval-minutes"), e.getMessage());
    }

    @Test
    void spnegoWithIpAddressInLivyUrlFailsAtStartup() {
        for (String url : List.of("http://10.0.0.5:8998", "http://[::1]:8998")) {
            SparkProperties.SparkClusterConfig cluster = spnegoCluster();
            cluster.setLivyUrl(url);
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> new SparkEngineRegistry(properties(cluster)));
            assertTrue(e.getMessage().contains("spark.clusters[prod].livy-url"), e.getMessage());
            assertTrue(e.getMessage().contains("IP-адрес"), e.getMessage());
        }
    }

    private static SparkProperties.SparkClusterConfig spnegoCluster() {
        SparkProperties.SparkClusterConfig cluster = cluster("prod", "http://livy.example:8998", false);
        cluster.getLivy().setAuth("spnego");
        cluster.getLivy().setPrincipal("svc_spark_explorer@COMPANY.LOCAL");
        cluster.getLivy().setKeytab("/etc/security/keytabs/spark-explorer.keytab");
        return cluster;
    }

    @Test
    void nonPositiveTimeoutFailsAtStartup() {
        SparkProperties.SparkClusterConfig cluster = cluster("prod", "http://livy.example:8998", false);
        cluster.getLivy().setReadTimeoutSeconds(0);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SparkEngineRegistry(properties(cluster)));
        assertTrue(e.getMessage().contains("livy.read-timeout-seconds"), e.getMessage());
    }

    @Test
    void mockClustersAreNotValidated() {
        SparkProperties.SparkClusterConfig cluster = cluster("demo", null, true);
        cluster.getLivy().setAuth("kerberos");
        new SparkEngineRegistry(properties(cluster));
    }
}
