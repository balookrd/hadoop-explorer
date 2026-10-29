package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.QueueNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("NativeYarnClient: разбор ответа RM и переключение между RM")
class NativeYarnClientTest {

    private final List<HttpServer> servers = new ArrayList<>();

    @AfterEach
    void stopServers() {
        servers.forEach(s -> s.stop(0));
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = NativeYarnClientTest.class.getResourceAsStream("/rm/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static ClusterConfig cluster(List<String> rmUrls) {
        return ClusterConfig.builder()
                .id("c1")
                .name("C1")
                .resourceManagerUrls(rmUrls)
                .resourceMode("percentage")
                .defaultPartition("DEFAULT")
                .impersonationEnabled(true)
                .build();
    }

    /**
     * Fake RM. {@code active == false} answers like a standby RM: info says STANDBY and every other
     * request is redirected.
     */
    private String startRm(AtomicBoolean active, AtomicReference<String> lastQuery) throws IOException {
        String scheduler = resource("scheduler-hadoop-3.1.3.json");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (lastQuery != null) {
                lastQuery.set(exchange.getRequestURI().getRawQuery());
            }
            byte[] body;
            int status = 200;
            if (path.equals("/ws/v1/cluster/info")) {
                body = ("{\"clusterInfo\":{\"haState\":\"" + (active.get() ? "ACTIVE" : "STANDBY") + "\"}}").getBytes(StandardCharsets.UTF_8);
            } else if (!active.get()) {
                exchange.getResponseHeaders().add("Location", "http://elsewhere" + path);
                status = 307;
                body = new byte[0];
            } else if (path.equals("/ws/v1/cluster/scheduler")) {
                body = scheduler.getBytes(StandardCharsets.UTF_8);
            } else {
                status = 404;
                body = new byte[0];
            }
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
        servers.add(server);
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    @DisplayName("Реальный ответ Hadoop 3.1.3: раздел \"\" -> DEFAULT, ulf, метки, без выдуманных значений")
    void parsesRealSchedulerResponse() throws Exception {
        ClusterConfig c = cluster(List.of("http://unused"));
        QueueNode root = NativeYarnClient.parseQueueTree(
                new ObjectMapper().readTree(resource("scheduler-hadoop-3.1.3.json")).path("scheduler").path("schedulerInfo"), c);

        QueueNode prod = root.getChildren().stream().filter(q -> q.getName().equals("prod")).findFirst().orElseThrow();
        QueueNode spark = prod.getChildren().stream().filter(q -> q.getName().equals("spark")).findFirst().orElseThrow();
        QueueNode dev = root.getChildren().stream().filter(q -> q.getName().equals("dev")).findFirst().orElseThrow();

        assertFalse(spark.getPartitions().containsKey(""), "default partition must be normalized");
        assertEquals(60.0, spark.getPartitions().get("DEFAULT").getCapacity());
        assertEquals(100.0, spark.getPartitions().get("GPU").getCapacity());
        assertEquals(30.0, dev.getPartitions().get("DEFAULT").getCapacity(), "float noise 30.000002 must be rounded");
        assertEquals(3.0, spark.getUserLimitFactor());
        assertEquals(0.5, spark.getMaxAmResourcePercent());
        assertEquals(List.of("GPU"), spark.getAccessibleNodeLabels());
        assertEquals("GPU", spark.getDefaultNodeLabelExpression());
        // Hadoop 3.1 does not report the ordering policy: it must stay unknown, not "fifo"
        assertNull(spark.getOrderingPolicy());
        assertNull(prod.getUserLimitFactor());
        assertTrue(spark.isLeaf());
        assertFalse(prod.isLeaf());
    }

    @Test
    @DisplayName("После failover клиент переходит на новый активный RM")
    void followsFailover() throws Exception {
        AtomicBoolean rm1Active = new AtomicBoolean(true);
        AtomicBoolean rm2Active = new AtomicBoolean(false);
        String rm1 = startRm(rm1Active, null);
        String rm2 = startRm(rm2Active, null);
        NativeYarnClient client = new NativeYarnClient(cluster(List.of(rm1, rm2)), new ObjectMapper());

        assertEquals(rm1, client.getActiveRmUrl());
        assertEquals("root", client.getQueueTree("alice").getName());

        rm1Active.set(false);
        rm2Active.set(true);

        assertEquals("root", client.getQueueTree("alice").getName());
        assertEquals(rm2, client.getActiveRmUrl());
    }

    @Test
    @DisplayName("Недоступный RM: понятная ошибка YarnRmException, а не 500")
    void unreachableRm() {
        NativeYarnClient client = new NativeYarnClient(cluster(List.of("http://127.0.0.1:1")), new ObjectMapper());
        YarnRmException e = assertThrows(YarnRmException.class, () -> client.getQueueTree("alice"));
        assertTrue(e.getMessage().contains("c1"), e.getMessage());
    }

    @Test
    @DisplayName("doAs кодируется в URL")
    void doAsIsEncoded() throws Exception {
        AtomicReference<String> query = new AtomicReference<>();
        String rm = startRm(new AtomicBoolean(true), query);
        NativeYarnClient client = new NativeYarnClient(cluster(List.of(rm)), new ObjectMapper());

        client.getQueueTree("svc user&x=1");

        assertEquals("doAs=svc+user%26x%3D1", query.get());
    }

    @Test
    @DisplayName("Kerberos-кластер без клиентского keytab не ходит в RM без аутентификации")
    void kerberosWithoutLoginFails() {
        ClusterConfig c = cluster(List.of("http://127.0.0.1:1"));
        c.setKerberosEnabled(true);
        NativeYarnClient client = new NativeYarnClient(c, new ObjectMapper(), null, null);
        assertThrows(YarnRmException.class, () -> client.getQueueTree("alice"));
    }
}
