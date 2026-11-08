package org.apache.hadoop.explorer.sql.service;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryEventHubTest {

    private final QueryEventHub hub = new QueryEventHub();

    @Test
    void lateSubscriberReceivesEventsPublishedBeforeSubscription() {
        hub.open("q1");
        hub.publish("q1", Map.of("type", "status", "status", "RUNNING"));
        hub.publish("q1", Map.of("type", "rows", "total_rows", 1));
        List<Map<String, Object>> received = new ArrayList<>();

        assertTrue(hub.subscribe("q1", received::add));
        hub.publish("q1", Map.of("type", "finished", "status", "FINISHED"));
        hub.close("q1");

        assertEquals(List.of("status", "rows", "finished"), received.stream().map(e -> e.get("type")).toList());
    }

    @Test
    void subscriberAfterCloseGetsReplayAndLaterEventsAreIgnored() {
        hub.open("q2");
        hub.publish("q2", Map.of("type", "finished", "status", "FINISHED"));
        hub.close("q2");
        hub.publish("q2", Map.of("type", "status", "status", "RUNNING"));
        List<Map<String, Object>> received = new ArrayList<>();

        assertTrue(hub.subscribe("q2", received::add));

        assertEquals(1, received.size());
        assertEquals("finished", received.get(0).get("type"));
    }

    @Test
    void failingSinkIsDropped() {
        hub.open("q3");
        AtomicInteger calls = new AtomicInteger();
        hub.subscribe("q3", e -> {
            calls.incrementAndGet();
            return false;
        });
        hub.publish("q3", Map.of("type", "status"));
        hub.publish("q3", Map.of("type", "status"));

        assertEquals(1, calls.get());
    }

    @Test
    void throwingSinkDoesNotStopOtherSubscribersOrThePublisher() {
        hub.open("q5");
        AtomicInteger throwingCalls = new AtomicInteger();
        List<Map<String, Object>> received = new ArrayList<>();
        hub.subscribe("q5", e -> {
            throwingCalls.incrementAndGet();
            throw new IllegalStateException("emitter already completed");
        });
        hub.subscribe("q5", received::add);

        hub.publish("q5", Map.of("type", "status"));
        hub.publish("q5", Map.of("type", "finished", "status", "FINISHED"));

        assertEquals(1, throwingCalls.get());
        assertEquals(List.of("status", "finished"), received.stream().map(e -> e.get("type")).toList());
    }

    @Test
    void sinkUnsubscribingItselfDuringPublishIsSafe() {
        hub.open("q6");
        List<Map<String, Object>> received = new ArrayList<>();
        QueryEventHub.Sink[] self = new QueryEventHub.Sink[1];
        // SseEmitter.complete() may run the completion callback (unsubscribe) on the publishing thread
        self[0] = e -> {
            hub.unsubscribe("q6", self[0]);
            return false;
        };
        hub.subscribe("q6", self[0]);
        hub.subscribe("q6", received::add);

        hub.publish("q6", Map.of("type", "status"));

        assertEquals(1, received.size());
    }

    @Test
    void unknownQueryIsReported() {
        assertFalse(hub.subscribe("missing", e -> true));
    }

    @Test
    void evictRemovesClosedChannels() {
        hub.open("q4");
        hub.close("q4");
        hub.evictClosedBefore(Instant.now().plusSeconds(1));

        assertFalse(hub.subscribe("q4", e -> true));
    }
}
