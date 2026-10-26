package org.apache.hadoop.explorer.replicator.hms.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Значения партиций из сообщения события NOTIFICATION_LOG.
 *
 * <p>Форматы сообщений метастора:
 * <ul>
 *   <li>{@code json-0.2} (Hive 3, HDP 3.1) — JSON, партиции перечислены как
 *       {@code "partitions": [{"dt": "2026-10-01", "region": "eu"}]}, у ALTER_PARTITION —
 *       {@code "keyValues": {...}}; порядок ключей совпадает с порядком ключей партиционирования;</li>
 *   <li>{@code gzip(json-2.0)} (Hive 4 по умолчанию) — тот же JSON, сжатый gzip и
 *       закодированный Base64.</li>
 * </ul>
 * Прежний разбор искал только ключ {@code values}, которого в этих форматах нет, и на каждое
 * событие уходил в запасной путь: читал все партиции таблицы (200k) и брал «последнюю по
 * имени» — то есть не ту.
 *
 * <p>Классы десериализации самого Hive живут в серверном модуле метастора, тянущем за собой
 * DataNucleus и половину сервера, поэтому разбор свой: форматы стабильны и описаны выше.
 */
final class HmsEventMessageDecoder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HmsEventMessageDecoder() {
    }

    /** Текст сообщения: для gzip-форматов — распакованный. */
    static String body(String message, String messageFormat) throws IOException {
        if (message == null) {
            return null;
        }
        if (messageFormat != null && messageFormat.toLowerCase().startsWith("gzip")) {
            byte[] compressed = Base64.getDecoder().decode(message.trim());
            try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        return message;
    }

    /**
     * Значения партиций события; пустой список, если сообщение их не содержит или не разобрано.
     */
    static List<List<String>> partitionValues(String message, String messageFormat) {
        if (message == null || message.isBlank()) {
            return Collections.emptyList();
        }
        try {
            Map<?, ?> map = MAPPER.readValue(body(message, messageFormat), Map.class);
            List<List<String>> result = new ArrayList<>();

            // json-0.2 / json-2.0: ADD_PARTITION и DROP_PARTITION
            if (map.get("partitions") instanceof List<?> parts) {
                for (Object item : parts) {
                    if (item instanceof Map<?, ?> partMap) {
                        // Старый вид {"values": [...]} или карта ключ -> значение
                        if (partMap.get("values") instanceof List<?> vals) {
                            result.add(stringList(vals));
                        } else if (!partMap.isEmpty()) {
                            result.add(stringList(partMap.values()));
                        }
                    }
                }
                if (!result.isEmpty()) {
                    return result;
                }
            }
            // json-0.2 / json-2.0: ALTER_PARTITION
            if (map.get("keyValues") instanceof Map<?, ?> keyValues && !keyValues.isEmpty()) {
                result.add(stringList(keyValues.values()));
                return result;
            }
            // Упрощенные сообщения (тестовые клиенты, старые агенты)
            if (map.get("values") instanceof List<?> vals) {
                result.add(stringList(vals));
                return result;
            }
            if (map.get("partition") instanceof Map<?, ?> part && part.get("values") instanceof List<?> vals) {
                result.add(stringList(vals));
                return result;
            }
            if (map.get("partitionValues") instanceof List<?> pvList) {
                for (Object item : pvList) {
                    if (item instanceof List<?> l) {
                        result.add(stringList(l));
                    } else if (item != null) {
                        result.add(List.of(item.toString()));
                    }
                }
                return result;
            }
        } catch (Exception ignored) {
            // неразобранное сообщение — пустой результат, вызывающий решает, что с ним делать
        }
        return Collections.emptyList();
    }

    /**
     * Событие INSERT: в какую партицию писали (пусто — таблица без партиций), какие файлы теперь
     * лежат в каталоге (имена без пути) и было ли это перезаписью (INSERT OVERWRITE).
     */
    record InsertEvent(List<String> partitionValues, java.util.Set<String> fileNames, boolean replace) {
    }

    /**
     * Разбор INSERT в форматах {@code json-0.2} и {@code gzip(json-2.0)}.
     *
     * <p>Партиция берется из {@code ptnObjJson} (Thrift-JSON, значения — поле 1:
     * {@code {"1":{"lst":["str",2,"2026-10-01","eu"]}}}) или из {@code partKeyVals} старых версий.
     * Файлы — список {@code files}, где после пути через {@code #} идут контрольная сумма и подкаталог.
     */
    static java.util.Optional<InsertEvent> insert(String message, String messageFormat) {
        if (message == null || message.isBlank()) {
            return java.util.Optional.empty();
        }
        try {
            Map<?, ?> map = MAPPER.readValue(body(message, messageFormat), Map.class);
            List<String> values = new ArrayList<>();
            if (map.get("ptnObjJson") instanceof String ptnJson && !ptnJson.isBlank()) {
                var lst = MAPPER.readTree(ptnJson).path("1").path("lst");
                // ["str", N, v1, v2, ...]: первые два элемента — тип и размер списка
                for (int i = 2; i < lst.size(); i++) {
                    values.add(lst.get(i).asText());
                }
            } else if (map.get("partKeyVals") instanceof Map<?, ?> kv && !kv.isEmpty()) {
                values.addAll(stringList(kv.values()));
            }
            java.util.Set<String> files = new java.util.LinkedHashSet<>();
            if (map.get("files") instanceof List<?> fileList) {
                for (Object f : fileList) {
                    if (f == null) continue;
                    String uri = f.toString();
                    int hash = uri.indexOf('#');
                    if (hash >= 0) uri = uri.substring(0, hash);
                    int slash = uri.lastIndexOf('/');
                    files.add(slash >= 0 ? uri.substring(slash + 1) : uri);
                }
            }
            Object replace = map.get("replace");
            boolean isReplace = replace != null && Boolean.parseBoolean(replace.toString());
            return java.util.Optional.of(new InsertEvent(values, files, isReplace));
        } catch (Exception e) {
            return java.util.Optional.empty();
        }
    }

    /** Имена таблицы до и после ALTER_TABLE; пусто, если их не удалось извлечь. */
    record TableRename(String before, String after) {
        boolean isRename() {
            return before != null && after != null && !before.equalsIgnoreCase(after);
        }
    }

    /**
     * Имена таблицы до и после из сообщения ALTER_TABLE.
     *
     * <p>В {@code json-0.2}/{@code json-2.0} объекты таблицы лежат в полях
     * {@code tableObjBeforeJson}/{@code tableObjAfterJson} строкой в Thrift-JSON, где имя таблицы —
     * поле 1: {@code {"1":{"str":"orders"},...}}. Без этого RENAME выглядел как изменение таблицы
     * со старым именем, которой в источнике уже нет: событие пропускалось, а новая таблица на
     * приемник не попадала.
     */
    static java.util.Optional<TableRename> tableRename(String message, String messageFormat) {
        if (message == null || message.isBlank()) {
            return java.util.Optional.empty();
        }
        try {
            Map<?, ?> map = MAPPER.readValue(body(message, messageFormat), Map.class);
            String before = thriftTableName(map.get("tableObjBeforeJson"));
            String after = thriftTableName(map.get("tableObjAfterJson"));
            if (before == null && after == null) {
                return java.util.Optional.empty();
            }
            return java.util.Optional.of(new TableRename(before, after));
        } catch (Exception e) {
            return java.util.Optional.empty();
        }
    }

    private static String thriftTableName(Object thriftJson) throws IOException {
        if (!(thriftJson instanceof String json) || json.isBlank()) {
            return null;
        }
        var node = MAPPER.readTree(json).path("1").path("str");
        return node.isTextual() ? node.asText() : null;
    }

    private static List<String> stringList(java.util.Collection<?> values) {
        List<String> out = new ArrayList<>(values.size());
        for (Object v : values) {
            out.add(v != null ? v.toString() : null);
        }
        return out;
    }
}
