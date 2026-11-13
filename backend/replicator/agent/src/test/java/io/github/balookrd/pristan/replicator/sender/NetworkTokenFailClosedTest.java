package io.github.balookrd.pristan.replicator.sender;

import io.grpc.Status;
import io.github.balookrd.pristan.replicator.client.OrchestratorGrpcClient;
import io.github.balookrd.pristan.replicator.client.OrchestratorGrpcClient.TokenGrant;
import io.github.balookrd.pristan.replicator.model.TokenRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The sender does not put bytes on the WAN without tokens from the orchestrator.
 *
 * <p>Two holes: an unreachable orchestrator was read as "link is free", and a closed link
 * (limit 0, Kill-Switch fencing) only delayed the batch by the returned wait instead of holding it.
 */
@DisplayName("Отправитель: без токенов оркестратора данные не уходят")
class NetworkTokenFailClosedTest {

    private static final long WIRE_BYTES = 64 * 1024L;
    private static final long MIN_BATCH = ReplicationSender.DEFAULT_MIN_TOKEN_BATCH_BYTES;
    private static final long MAX_BATCH = ReplicationSender.DEFAULT_MAX_TOKEN_BATCH_BYTES;

    private static void acquire(ConcurrentHashMap<String, AtomicLong> leases, OrchestratorGrpcClient client) {
        ReplicationSender.acquireNetworkTokens(leases, new ConcurrentHashMap<>(), client, "w1",
                WIRE_BYTES, "c1", "c2", MIN_BATCH, MAX_BATCH);
    }

    @Test
    @Timeout(20)
    @DisplayName("Недоступный оркестратор: отправитель ждет и повторяет запрос")
    void waitsUntilOrchestratorAnswers() {
        OrchestratorGrpcClient client = mock(OrchestratorGrpcClient.class);
        when(client.requestTokenGrant(any(TokenRequest.class)))
                .thenThrow(Status.UNAVAILABLE.asRuntimeException())
                .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())
                .thenReturn(TokenGrant.immediate());
        ConcurrentHashMap<String, AtomicLong> leases = new ConcurrentHashMap<>();

        acquire(leases, client);

        verify(client, times(3)).requestTokenGrant(any(TokenRequest.class));
        assertEquals(MIN_BATCH - WIRE_BYTES, leases.get("c1->c2").get());
    }

    @Test
    @Timeout(20)
    @DisplayName("Перекрытый канал: отправитель не шлет после ожидания, а спрашивает снова")
    void blockedLinkIsAskedAgainInsteadOfSending() {
        OrchestratorGrpcClient client = mock(OrchestratorGrpcClient.class);
        when(client.requestTokenGrant(any(TokenRequest.class)))
                .thenReturn(new TokenGrant(0.1, true))
                .thenReturn(new TokenGrant(0.1, true))
                .thenReturn(TokenGrant.immediate());
        ConcurrentHashMap<String, AtomicLong> leases = new ConcurrentHashMap<>();

        acquire(leases, client);

        verify(client, times(3)).requestTokenGrant(any(TokenRequest.class));
    }

    @Test
    @Timeout(20)
    @DisplayName("Перекрытый канал: остаток ранее выданных токенов сгорает")
    void blockedLinkDropsLocalLease() {
        OrchestratorGrpcClient client = mock(OrchestratorGrpcClient.class);
        when(client.requestTokenGrant(any(TokenRequest.class)))
                .thenReturn(new TokenGrant(0.1, true))
                .thenReturn(TokenGrant.immediate());
        ConcurrentHashMap<String, AtomicLong> leases = new ConcurrentHashMap<>();
        leases.put("c1->c2", new AtomicLong(WIRE_BYTES / 2));

        acquire(leases, client);

        assertEquals(MIN_BATCH - WIRE_BYTES, leases.get("c1->c2").get(),
                "Токены, выданные до перекрытия канала, не должны тратиться после него");
    }
}
