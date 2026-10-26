package org.apache.hadoop.explorer.replicator.hms.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Чекпоинт bootstrap: какие таблицы перенесены целиком (и сколько у них партиций) и докуда
 * дошла каждая незавершенная — имя последней примененной партиции.
 *
 * <p>Партиции обходятся в порядке имен (сортировка в Java, не в базе метастора: collation
 * MySQL/PostgreSQL/Derby разная), поэтому «последнее имя» однозначно задает, с чего продолжать.
 * Отправляется оркестратору с heartbeat; порции, примененные после последнего heartbeat, при
 * продолжении применятся повторно — это безопасно, применение идемпотентно.
 */
final class BootstrapCheckpoint {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Map<String, Integer> done = new ConcurrentHashMap<>();
    private final Map<String, String> partial = new ConcurrentHashMap<>();

    static BootstrapCheckpoint parse(String json) {
        BootstrapCheckpoint cp = new BootstrapCheckpoint();
        if (json == null || json.isBlank()) {
            return cp;
        }
        try {
            Map<String, Object> map = MAPPER.readValue(json, new TypeReference<>() { });
            if (map.get("done") instanceof Map<?, ?> d) {
                d.forEach((k, v) -> cp.done.put(k.toString(), v instanceof Number n ? n.intValue() : 0));
            }
            if (map.get("partial") instanceof Map<?, ?> p) {
                p.forEach((k, v) -> cp.partial.put(k.toString(), v.toString()));
            }
        } catch (Exception e) {
            // Неразобранный чекпоинт — продолжения нет, bootstrap пойдет с начала
        }
        return cp;
    }

    boolean isEmpty() {
        return done.isEmpty() && partial.isEmpty();
    }

    boolean isDone(String table) {
        return done.containsKey(table);
    }

    int partitionsOf(String table) {
        return done.getOrDefault(table, 0);
    }

    /** Имя последней примененной партиции незавершенной таблицы или {@code null}. */
    String resumeAfter(String table) {
        return partial.get(table);
    }

    void advance(String table, String lastAppliedPartition) {
        if (lastAppliedPartition != null) {
            partial.put(table, lastAppliedPartition);
        }
    }

    void markDone(String table, int partitions) {
        done.put(table, partitions);
        partial.remove(table);
    }

    String toJson() {
        try {
            return MAPPER.writeValueAsString(Map.of("done", Map.copyOf(done), "partial", Map.copyOf(partial)));
        } catch (Exception e) {
            return null;
        }
    }
}
