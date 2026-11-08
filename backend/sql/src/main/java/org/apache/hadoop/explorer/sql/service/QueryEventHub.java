package org.apache.hadoop.explorer.sql.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-query SSE fan-out with replay. The UI subscribes to /queries/{id}/stream only after
 * POST /queries/execute returned, so every subscriber first receives all events published so far.
 * Rows events reference the same lists as the cached result, the log adds only references.
 */
public class QueryEventHub {

    @FunctionalInterface
    public interface Sink {
        /** Returns false when the subscriber is gone and must be dropped. */
        boolean send(Map<String, Object> event);
    }

    private static final class Channel {
        private final ReentrantLock lock = new ReentrantLock();
        private final List<Map<String, Object>> log = new ArrayList<>();
        private final List<Sink> sinks = new ArrayList<>();
        private boolean closed;
        private volatile Instant closedAt;
    }

    private static final Logger log = LoggerFactory.getLogger(QueryEventHub.class);

    private final Map<String, Channel> channels = new ConcurrentHashMap<>();

    public void open(String queryId) {
        channels.putIfAbsent(queryId, new Channel());
    }

    public void publish(String queryId, Map<String, Object> event) {
        Channel channel = channels.computeIfAbsent(queryId, id -> new Channel());
        channel.lock.lock();
        try {
            if (channel.closed) {
                return;
            }
            channel.log.add(event);
            // Iterate over a copy: a sink may unsubscribe itself (SseEmitter completion callback) while sending
            for (Sink sink : List.copyOf(channel.sinks)) {
                if (!deliver(sink, event)) {
                    channel.sinks.remove(sink);
                }
            }
        } finally {
            channel.lock.unlock();
        }
    }

    /** Stops accepting events; the terminal event must be published before. */
    public void close(String queryId) {
        Channel channel = channels.computeIfAbsent(queryId, id -> new Channel());
        channel.lock.lock();
        try {
            channel.closed = true;
            channel.closedAt = Instant.now();
            channel.sinks.clear();
        } finally {
            channel.lock.unlock();
        }
    }

    /** Replays the log into the sink; returns false if the query is unknown (evicted or before a restart). */
    public boolean subscribe(String queryId, Sink sink) {
        Channel channel = channels.get(queryId);
        if (channel == null) {
            return false;
        }
        channel.lock.lock();
        try {
            for (Map<String, Object> event : channel.log) {
                if (!deliver(sink, event)) {
                    return true;
                }
            }
            if (!channel.closed) {
                channel.sinks.add(sink);
            }
            return true;
        } finally {
            channel.lock.unlock();
        }
    }

    public void unsubscribe(String queryId, Sink sink) {
        Channel channel = channels.get(queryId);
        if (channel == null) {
            return;
        }
        channel.lock.lock();
        try {
            channel.sinks.remove(sink);
        } finally {
            channel.lock.unlock();
        }
    }

    /**
     * A failing subscriber is dropped; it must never interrupt delivery to the others or the query itself
     * (publish runs on the query thread, before the terminal status and metrics are recorded).
     */
    private static boolean deliver(Sink sink, Map<String, Object> event) {
        try {
            return sink.send(event);
        } catch (RuntimeException e) {
            log.debug("SSE subscriber failed, dropping it: {}", e.getMessage());
            return false;
        }
    }

    public void evictClosedBefore(Instant threshold) {
        channels.entrySet().removeIf(entry -> {
            Instant closedAt = entry.getValue().closedAt;
            return closedAt != null && closedAt.isBefore(threshold);
        });
    }
}
