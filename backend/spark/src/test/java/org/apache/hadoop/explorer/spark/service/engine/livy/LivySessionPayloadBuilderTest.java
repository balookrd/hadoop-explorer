package org.apache.hadoop.explorer.spark.service.engine.livy;

import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.dto.CreateSessionRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LivySessionPayloadBuilderTest {

    static SparkProperties.SparkClusterConfig cluster() {
        SparkProperties.SparkClusterConfig cluster = new SparkProperties.SparkClusterConfig();
        cluster.setId("livy-test");
        cluster.setName("Livy test");
        SparkProperties.ResourceProfileConfig small = new SparkProperties.ResourceProfileConfig();
        small.setId("small");
        small.setDriverMemory("1g");
        small.setDriverCores(1);
        small.setExecutorMemory("2g");
        small.setExecutorCores(1);
        small.setNumExecutors(1);
        cluster.setResourceProfiles(List.of(small));
        SparkProperties.PythonEnvConfig py = new SparkProperties.PythonEnvConfig();
        py.setId("py310");
        py.setSparkConf(Map.of("spark.pyspark.python", "/opt/py310/bin/python"));
        SparkProperties.SparkVersionConfig version = new SparkProperties.SparkVersionConfig();
        version.setId("spark-3.5.1");
        version.setSparkConf(Map.of("spark.yarn.archive", "hdfs:///apps/spark-3.5.1.zip"));
        version.setPythonVersions(List.of(py));
        cluster.setSparkVersions(List.of(version));
        return cluster;
    }

    static CreateSessionRequest request(String kind, Map<String, String> conf) {
        return new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.analytics", "small", kind, "py310",
                null, null, List.of("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"), List.of(), List.of(), conf);
    }

    private final LivySessionPayloadBuilder builder = new LivySessionPayloadBuilder(new SparkProperties());

    @Test
    void mapsUserQueueProfileAndConf() {
        Map<String, Object> body = builder.build(cluster(), "analyst", request("pyspark", Map.of("spark.sql.shuffle.partitions", "8")));

        assertEquals("pyspark", body.get("kind"));
        assertEquals("analyst", body.get("proxyUser"));
        assertEquals("root.analytics", body.get("queue"));
        assertEquals("1g", body.get("driverMemory"));
        assertEquals(1, body.get("numExecutors"));
        @SuppressWarnings("unchecked")
        Map<String, String> conf = (Map<String, String>) body.get("conf");
        assertEquals("hdfs:///apps/spark-3.5.1.zip", conf.get("spark.yarn.archive"));
        assertEquals("/opt/py310/bin/python", conf.get("spark.pyspark.python"));
        assertEquals("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1", conf.get("spark.jars.packages"));
        assertEquals("8", conf.get("spark.sql.shuffle.partitions"));
    }

    @Test
    void scalaSessionKindIsSpark() {
        assertEquals("spark", builder.build(cluster(), "analyst", request("spark", Map.of())).get("kind"));
    }

    @Test
    void proxyUserCanBeDisabled() {
        SparkProperties.SparkClusterConfig cluster = cluster();
        cluster.getLivy().setProxyUser(false);
        assertFalse(builder.build(cluster, "analyst", request("pyspark", Map.of())).containsKey("proxyUser"));
    }

    @Test
    void deniedConfIsRejected() {
        assertThrows(InvalidSessionRequestException.class,
                () -> builder.build(cluster(), "analyst", request("pyspark", Map.of("spark.yarn.principal", "hdfs"))));
    }

    @Test
    void livyLevelProxyUserIsRejectedEvenWithProxyUserDisabled() {
        // Livy copies session conf into the RSC launcher config, where livy.rsc.proxy-user becomes --proxy-user
        SparkProperties.SparkClusterConfig cluster = cluster();
        cluster.getLivy().setProxyUser(false);
        assertThrows(InvalidSessionRequestException.class,
                () -> builder.build(cluster, "analyst", request("pyspark", Map.of("livy.rsc.proxy-user", "hdfs"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "spark.yarn.principal", "spark.yarn.keytab", "spark.kerberos.keytab", "spark.kerberos.principal",
            "livy.rsc.proxy-user",
            // local/client mode would run user code on the Livy host as the livy OS account
            "spark.master", "spark.submit.deployMode",
            // spark-submit uploads these from the Livy host into the user's container (keytab exfiltration)
            "spark.yarn.dist.files", "spark.yarn.dist.jars", "spark.yarn.dist.archives", "spark.yarn.dist.pyFiles",
            "spark.yarn.jars", "spark.yarn.archive",
            // Ivy runs on the Livy host as livy: local settings file, cache directory, arbitrary repositories
            "spark.jars.ivy", "spark.jars.ivySettings", "spark.jars.repositories",
            // the queue comes from the request field checked against yarn-queues; the conf key would bypass it
            "spark.yarn.queue", "SPARK.YARN.QUEUE"})
    void keyStaysDeniedWhenAdminClearsTheList(String key) {
        SparkProperties properties = new SparkProperties();
        properties.setDeniedSparkConfPrefixes(List.of());
        LivySessionPayloadBuilder lenient = new LivySessionPayloadBuilder(properties);
        assertThrows(InvalidSessionRequestException.class,
                () -> lenient.build(cluster(), "analyst", request("pyspark", Map.of(key, "file:///etc/security/keytabs/livy.service.keytab"))));
    }

    @Test
    void otherKeysAreUpToTheAdminList() {
        SparkProperties properties = new SparkProperties();
        properties.setDeniedSparkConfPrefixes(List.of());
        LivySessionPayloadBuilder lenient = new LivySessionPayloadBuilder(properties);
        assertEquals("200", ((Map<?, ?>) lenient.build(cluster(), "analyst",
                request("pyspark", Map.of("spark.sql.shuffle.partitions", "200"))).get("conf")).get("spark.sql.shuffle.partitions"));
    }

    @Test
    void packagesKeyIsDeniedByDefaultButUpToTheAdmin() {
        // Resolved by spark-submit on the Livy host
        assertThrows(InvalidSessionRequestException.class,
                () -> builder.build(cluster(), "analyst", request("pyspark", Map.of("spark.jars.packages", "g:a:1"))));
        SparkProperties properties = new SparkProperties();
        properties.setDeniedSparkConfPrefixes(List.of());
        Map<?, ?> conf = (Map<?, ?>) new LivySessionPayloadBuilder(properties)
                .build(cluster(), "analyst", request("pyspark", Map.of("spark.jars.packages", "g:a:1"))).get("conf");
        assertEquals("g:a:1", conf.get("spark.jars.packages"));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "spark.jars|https://repo.example/lib.jar",
            "spark.files|http://intranet.example/lookup.csv"})
    void httpIsRejectedByDefault(String key, String value) {
        // With http(s), spark-submit may download the file on the Livy host (requests into the internal network)
        assertThrows(InvalidSessionRequestException.class,
                () -> builder.build(cluster(), "analyst", request("pyspark", Map.of(key, value))));
        CreateSessionRequest jars = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.analytics", "small",
                "pyspark", null, null, null, List.of(), List.of(value), List.of(), Map.of());
        assertThrows(InvalidSessionRequestException.class, () -> builder.build(cluster(), "analyst", jars));
    }

    @Test
    void httpPassesWhenTheAdminAllowsIt() {
        SparkProperties properties = new SparkProperties();
        properties.setAllowedRemoteSchemes(List.of("hdfs", " HTTPS "));
        LivySessionPayloadBuilder withHttps = new LivySessionPayloadBuilder(properties);
        Map<?, ?> conf = (Map<?, ?>) withHttps.build(cluster(), "analyst",
                request("pyspark", Map.of("spark.jars", "https://repo.example/lib.jar"))).get("conf");
        assertEquals("https://repo.example/lib.jar", conf.get("spark.jars"));
        // Only the listed schemes: s3a is not among them now
        assertThrows(InvalidSessionRequestException.class, () -> withHttps.build(cluster(), "analyst",
                request("pyspark", Map.of("spark.jars", "s3a://bucket/lib.jar"))));
    }

    @Test
    void spaceSeparatedFileListIsRejected() {
        // Spark splits on commas only: the whole value would be one bogus path
        assertThrows(InvalidSessionRequestException.class, () -> builder.build(cluster(), "analyst",
                request("pyspark", Map.of("spark.jars", "hdfs:///lib/a.jar hdfs:///lib/b.jar"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Spark.Master", "SPARK.YARN.KEYTAB", "Livy.Rsc.Proxy-User", "spark.yarn.DIST.files"})
    void deniedKeysAreMatchedIgnoringCase(String key) {
        assertThrows(InvalidSessionRequestException.class,
                () -> builder.build(cluster(), "analyst", request("pyspark", Map.of(key, "x"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {" spark.master", "spark.master ", "spark.\tmaster", "spark.master\n", "spark.sql.\u0000x", ""})
    void keysWithWhitespaceOrControlCharactersAreRejected(String key) {
        assertThrows(InvalidSessionRequestException.class,
                () -> builder.build(cluster(), "analyst", request("pyspark", Map.of(key, "x"))));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "spark.files|file:///etc/security/keytabs/livy.service.keytab",
            "spark.jars|local:/opt/livy/conf/livy.conf",
            "spark.submit.pyFiles|/etc/passwd",
            "spark.archives|conf/env.tar.gz#env",
            "spark.files|hdfs:///data/a.txt,file:///etc/shadow",
            "spark.jars|ftp://host/x.jar",
            "SPARK.FILES|/etc/passwd"})
    void localPathsInFileListsAreRejected(String key, String value) {
        assertThrows(InvalidSessionRequestException.class,
                () -> builder.build(cluster(), "analyst", request("pyspark", Map.of(key, value))));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "spark.files|hdfs:///data/lookup.csv",
            "spark.jars|s3a://bucket/lib.jar, hdfs:///lib/b.jar",
            "spark.files|viewfs://cluster/data/a.csv,abfss://c@acc.dfs.core.windows.net/b.csv",
            "spark.submit.pyFiles|abfs://c@acc.dfs.core.windows.net/deps.zip",
            "spark.archives|hdfs://nn:8020/envs/py310.tar.gz#env"})
    void remoteFileListsAreAccepted(String key, String value) {
        Map<?, ?> conf = (Map<?, ?>) builder.build(cluster(), "analyst", request("pyspark", Map.of(key, value))).get("conf");
        assertEquals(value, conf.get(key));
    }

    @Test
    void localPathsInSessionFieldsAreRejected() {
        CreateSessionRequest jars = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.analytics", "small",
                "pyspark", null, null, null, List.of(), List.of("file:///etc/security/keytabs/livy.service.keytab"), List.of(), Map.of());
        CreateSessionRequest pyFiles = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.analytics", "small",
                "pyspark", null, null, null, List.of(), List.of(), List.of("/etc/passwd"), Map.of());
        CreateSessionRequest archive = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.analytics", "small",
                "pyspark", null, "local:/opt/env.tar.gz#environment", null, List.of(), List.of(), List.of(), Map.of());
        for (CreateSessionRequest req : List.of(jars, pyFiles, archive)) {
            assertThrows(InvalidSessionRequestException.class, () -> builder.build(cluster(), "analyst", req));
        }
    }

    @Test
    void adminSparkConfIsNotChecked() {
        SparkProperties.SparkClusterConfig cluster = cluster();
        cluster.getSparkVersions().get(0).setSparkConf(Map.of(
                "spark.yarn.archive", "hdfs:///apps/spark-3.5.1.zip",
                "spark.yarn.dist.archives", "hdfs:///apps/python/py310.tar.gz#py310"));
        Map<?, ?> conf = (Map<?, ?>) builder.build(cluster, "analyst", request("pyspark", Map.of())).get("conf");
        assertEquals("hdfs:///apps/python/py310.tar.gz#py310", conf.get("spark.yarn.dist.archives"));
    }

    @Test
    void queueMustBeOneOfTheClusterQueues() {
        SparkProperties.SparkClusterConfig cluster = cluster();
        cluster.setYarnQueues(List.of("root.default", "root.analytics"));
        assertEquals("root.analytics", builder.build(cluster, "analyst", request("pyspark", Map.of())).get("queue"));

        CreateSessionRequest other = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.etl", "small",
                "pyspark", null, null, null, List.of(), List.of(), List.of(), Map.of());
        assertThrows(InvalidSessionRequestException.class, () -> builder.build(cluster, "analyst", other));
    }

    @Test
    void blankQueueFallsBackToDefaultQueue() {
        SparkProperties.SparkClusterConfig cluster = cluster();
        cluster.setYarnQueues(List.of("root.default", "root.analytics"));
        cluster.setDefaultQueue("root.default");
        CreateSessionRequest blank = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", " ", "small",
                "pyspark", null, null, null, List.of(), List.of(), List.of(), Map.of());
        assertEquals("root.default", builder.build(cluster, "analyst", blank).get("queue"));
    }

    @Test
    void anyQueueIsPassedWhenClusterListsNone() {
        CreateSessionRequest any = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.etl", "small",
                "pyspark", null, null, null, List.of(), List.of(), List.of(), Map.of());
        assertEquals("root.etl", builder.build(cluster(), "analyst", any).get("queue"));
    }

    @Test
    void proxyUserIsTheAuthenticatedUserOnly() {
        Map<String, Object> body = builder.build(cluster(), "analyst", request("pyspark", Map.of("proxyUser", "hdfs")));
        assertEquals("analyst", body.get("proxyUser"));
    }

    @Test
    void unknownResourceProfileIsRejected() {
        CreateSessionRequest req = new CreateSessionRequest("livy-test", "spark-3.5.1", "hive-prod", "root.analytics", "huge",
                "pyspark", null, null, null, List.of(), List.of(), List.of(), Map.of());
        assertThrows(InvalidSessionRequestException.class, () -> builder.build(cluster(), "analyst", req));
    }
}
