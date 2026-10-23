package org.apache.hadoop.explorer.replicator.client;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.generated.control.*;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class OrchestratorGrpcClientMultiEndpointTest {

    private Server server1;
    private Server server2;
    private OrchestratorGrpcClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server1 != null) {
            server1.shutdownNow();
        }
        if (server2 != null) {
            server2.shutdownNow();
        }
    }

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @DisplayName("parseEndpoints: корректный парсинг списков адресов, префиксов протоколов и разделителей")
    void testParseEndpoints() {
        List<String> single = OrchestratorGrpcClient.parseEndpoints("orch-1:50050");
        assertEquals(List.of("orch-1:50050"), single);

        List<String> multipleComma = OrchestratorGrpcClient.parseEndpoints("orch-1:50050,orch-2:50050,orch-3:50050");
        assertEquals(List.of("orch-1:50050", "orch-2:50050", "orch-3:50050"), multipleComma);

        List<String> withSpacesAndSemicolons = OrchestratorGrpcClient.parseEndpoints("  orch-1:50050 ; orch-2:50050 , orch-3:50050  ");
        assertEquals(List.of("orch-1:50050", "orch-2:50050", "orch-3:50050"), withSpacesAndSemicolons);

        List<String> withProtocols = OrchestratorGrpcClient.parseEndpoints("grpc://orch-1:50050, http://orch-2:50050, https://orch-3:50050");
        assertEquals(List.of("orch-1:50050", "orch-2:50050", "orch-3:50050"), withProtocols);

        List<String> withoutPorts = OrchestratorGrpcClient.parseEndpoints("orch-1, orch-2");
        assertEquals(List.of("orch-1:50050", "orch-2:50050"), withoutPorts);

        List<String> nullOrEmpty = OrchestratorGrpcClient.parseEndpoints(null);
        assertEquals(List.of("localhost:50050"), nullOrEmpty);
    }

    @Test
    @DisplayName("Failover: при падении первого узла клиент автоматически переключается на второй живой оркестратор")
    void testAutomaticFailoverBetweenNodes() throws Exception {
        int port1 = findFreePort();
        int port2 = findFreePort();

        AtomicInteger server1Calls = new AtomicInteger(0);
        AtomicInteger server2Calls = new AtomicInteger(0);

        // Поднимаем Server 1
        server1 = NettyServerBuilder.forPort(port1)
                .addService(new OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceImplBase() {
                    @Override
                    public void registerAgent(org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterRequest request,
                                              StreamObserver<org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterResponse> responseObserver) {
                        server1Calls.incrementAndGet();
                        responseObserver.onNext(org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterResponse.newBuilder()
                                .setSuccess(true)
                                .setStatus("REGISTERED_ON_SERVER_1")
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build().start();

        // Поднимаем Server 2
        server2 = NettyServerBuilder.forPort(port2)
                .addService(new OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceImplBase() {
                    @Override
                    public void registerAgent(org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterRequest request,
                                              StreamObserver<org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterResponse> responseObserver) {
                        server2Calls.incrementAndGet();
                        responseObserver.onNext(org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterResponse.newBuilder()
                                .setSuccess(true)
                                .setStatus("REGISTERED_ON_SERVER_2")
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build().start();

        // Создаем клиента с двумя эндпоинтами без рандомизации для детерминированного теста
        String addressList = "localhost:" + port1 + ",localhost:" + port2;
        client = new OrchestratorGrpcClient(List.of("localhost:" + port1, "localhost:" + port2), "test-secret", false, null, false, false);

        // 1. Первый запрос на живой Server 1
        boolean reg1 = client.register(new AgentRegisterRequest("agent-test", "dc1", "all", "localhost:50051", 100.0));
        assertTrue(reg1, "Первая регистрация должна быть успешной");
        assertEquals("localhost:" + port1, client.getTargetAddress());
        assertEquals(1, server1Calls.get());
        assertEquals(0, server2Calls.get());

        // 2. Аварийно гасим Server 1 (имитация падения ноды оркестратора)
        server1.shutdownNow();
        server1.awaitTermination();

        // 3. Следующий запрос: клиент должен поймать ошибку UNAVAILABLE, переключиться на Server 2 и выполнить вызов успешно!
        boolean reg2 = client.register(new AgentRegisterRequest("agent-test-2", "dc1", "all", "localhost:50051", 100.0));
        assertTrue(reg2, "Регистрация после падения Server 1 должна успешно завершиться через Server 2");
        assertEquals("localhost:" + port2, client.getTargetAddress(), "Клиент должен переключиться на адрес Server 2");
        assertEquals(1, server2Calls.get(), "Запрос должен был обработан Server 2");
    }

    @Test
    @DisplayName("Load Balancing: при включенном shuffle стартовые эндпоинты распределяются случайно")
    void testClientSideRandomLoadBalancing() {
        List<String> endpoints = List.of("orch-1:50050", "orch-2:50050");
        int count1 = 0;
        int count2 = 0;

        for (int i = 0; i < 100; i++) {
            // Создаем клиента без сетевого подключения (проверяем getTargetAddress)
            OrchestratorGrpcClient c = new OrchestratorGrpcClient(endpoints, "secret", false, null, false, true);
            try {
                if ("orch-1:50050".equals(c.getTargetAddress())) {
                    count1++;
                } else if ("orch-2:50050".equals(c.getTargetAddress())) {
                    count2++;
                }
            } finally {
                c.close();
            }
        }

        assertTrue(count1 > 15, "orch-1 должен быть выбран хотя бы в 15% случаев (было: " + count1 + ")");
        assertTrue(count2 > 15, "orch-2 должен быть выбран хотя бы в 15% случаев (было: " + count2 + ")");
    }
}
