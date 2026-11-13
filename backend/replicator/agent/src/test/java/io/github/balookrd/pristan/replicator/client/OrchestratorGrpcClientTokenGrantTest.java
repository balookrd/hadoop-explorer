package io.github.balookrd.pristan.replicator.client;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.github.balookrd.pristan.replicator.generated.control.OrchestratorControlPlaneServiceGrpc;
import io.github.balookrd.pristan.replicator.generated.control.TokenResponse;
import io.github.balookrd.pristan.replicator.model.TokenRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A failed token request is not a grant.
 *
 * <p>The client used to turn any gRPC failure into "wait 0 seconds", so with the orchestrator
 * down agents sent over the WAN without the central limit.
 */
@DisplayName("Клиент оркестратора: запрос токенов полосы")
class OrchestratorGrpcClientTokenGrantTest {

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
    }

    private OrchestratorGrpcClient clientAnswering(
            Consumer<StreamObserver<TokenResponse>> answer) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceImplBase() {
                    @Override
                    public void requestTokens(
                            io.github.balookrd.pristan.replicator.generated.control.TokenRequest request,
                            StreamObserver<TokenResponse> responseObserver) {
                        answer.accept(responseObserver);
                    }
                })
                .build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return new OrchestratorGrpcClient(channel, "test-secret-0123456789");
    }

    @Test
    @DisplayName("Сбой оркестратора не превращается в разрешение на отправку")
    void orchestratorFailureIsNotAGrant() throws Exception {
        OrchestratorGrpcClient client = clientAnswering(
                obs -> obs.onError(Status.UNAVAILABLE.withDescription("orchestrator down").asRuntimeException()));

        assertThrows(StatusRuntimeException.class,
                () -> client.requestTokenGrant(new TokenRequest("w1", 1024L, "c1", "c2")));
    }

    @Test
    @DisplayName("Признак перекрытого канала доходит до агента")
    void blockedFlagIsPassedThrough() throws Exception {
        OrchestratorGrpcClient client = clientAnswering(obs -> {
            obs.onNext(TokenResponse.newBuilder().setWaitSeconds(60.0).setBlocked(true).build());
            obs.onCompleted();
        });

        assertTrue(client.requestTokenGrant(new TokenRequest("w1", 1024L, "c1", "c2")).blocked());
    }

    @Test
    @DisplayName("Обычное ожидание не считается перекрытым каналом")
    void throttledAnswerIsNotBlocked() throws Exception {
        OrchestratorGrpcClient client = clientAnswering(obs -> {
            obs.onNext(TokenResponse.newBuilder().setWaitSeconds(0.5).build());
            obs.onCompleted();
        });

        assertFalse(client.requestTokenGrant(new TokenRequest("w1", 1024L, "c1", "c2")).blocked());
    }
}
