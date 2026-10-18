package org.apache.hadoop.explorer.replicator.kafka.filter;

import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecordHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Фильтр предотвращения бесконечных циклов («эхо») при двунаправленной (Active-Active) репликации Kafka.
 * <p>
 * Использует заголовки {@code X-Repl-Origin} и {@code X-Repl-Hop} для отслеживания маршрута сообщения
 * и отсечения записей, вернувшихся в кластер-источник.
 */
public class LoopPreventionFilter {

    private static final Logger log = LoggerFactory.getLogger(LoopPreventionFilter.class);

    public static final String HEADER_ORIGIN = "X-Repl-Origin";
    public static final String HEADER_HOP = "X-Repl-Hop";
    public static final int DEFAULT_MAX_HOPS = 3;

    private final String localDcId;
    private final String targetDcId;
    private final int maxHops;

    public LoopPreventionFilter(String localDcId) {
        this(localDcId, null, DEFAULT_MAX_HOPS);
    }

    public LoopPreventionFilter(String localDcId, int maxHops) {
        this(localDcId, null, maxHops);
    }

    public LoopPreventionFilter(String localDcId, String targetDcId) {
        this(localDcId, targetDcId, DEFAULT_MAX_HOPS);
    }

    public LoopPreventionFilter(String localDcId, String targetDcId, int maxHops) {
        if (localDcId == null || localDcId.isBlank()) {
            throw new IllegalArgumentException("localDcId must not be blank");
        }
        this.localDcId = localDcId.trim();
        this.targetDcId = (targetDcId != null && !targetDcId.isBlank()) ? targetDcId.trim() : null;
        this.maxHops = maxHops > 0 ? maxHops : DEFAULT_MAX_HOPS;
    }

    /**
     * Проверяет, должно ли сообщение реплицироваться в другой ДЦ.
     *
     * @param record запись Kafka
     * @return {@code true}, если запись должна быть отреплицирована, {@code false} если это эхо или превышен лимит прыжков
     */
    public boolean shouldReplicate(ConsumerRecord<?, ?> record) {
        Headers headers = record.headers();
        if (headers == null) {
            return true;
        }

        Header originHeader = headers.lastHeader(HEADER_ORIGIN);
        if (originHeader != null && originHeader.value() != null) {
            String origin = new String(originHeader.value(), StandardCharsets.UTF_8).trim();
            if (localDcId.equalsIgnoreCase(origin)) {
                log.trace("Отброшена эхо-запись для топика {} партиции {}: Origin matches local DC ({})",
                        record.topic(), record.partition(), localDcId);
                return false;
            }
            if (targetDcId != null && targetDcId.equalsIgnoreCase(origin)) {
                log.trace("Отброшена запись для топика {} партиции {}: Origin matches target DC ({}) (предотвращение паразитного обратного цикла)",
                        record.topic(), record.partition(), targetDcId);
                return false;
            }
        }

        Header hopHeader = headers.lastHeader(HEADER_HOP);
        if (hopHeader != null && hopHeader.value() != null) {
            try {
                int hop = Integer.parseInt(new String(hopHeader.value(), StandardCharsets.UTF_8).trim());
                if (hop >= maxHops) {
                    log.warn("Превышен лимит прыжков репликации (hop={}) для записи {}-{}",
                            hop, record.topic(), record.partition());
                    return false;
                }
            } catch (NumberFormatException ignored) {
                // Если заголовок поврежден, считаем hop = 0
            }
        }

        return true;
    }

    /**
     * Формирует список Protobuf-заголовков с добавлением/обновлением служебных меток происхождения.
     *
     * @param headers оригинальные заголовки Kafka
     * @return обогащенный список заголовков для отправки через gRPC
     */
    public List<KafkaRecordHeader> enrichHeaders(Headers headers) {
        List<KafkaRecordHeader> result = new ArrayList<>();
        boolean originPresent = false;
        int currentHop = 0;

        if (headers != null) {
            for (Header h : headers) {
                if (HEADER_ORIGIN.equalsIgnoreCase(h.key())) {
                    originPresent = true;
                    // Сохраняем исходный Origin
                    result.add(KafkaRecordHeader.newBuilder()
                            .setKey(HEADER_ORIGIN)
                            .setValue(com.google.protobuf.ByteString.copyFrom(h.value()))
                            .build());
                } else if (HEADER_HOP.equalsIgnoreCase(h.key())) {
                    try {
                        currentHop = Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8).trim());
                    } catch (NumberFormatException ignored) {
                    }
                } else {
                    result.add(KafkaRecordHeader.newBuilder()
                            .setKey(h.key())
                            .setValue(h.value() != null ? com.google.protobuf.ByteString.copyFrom(h.value())
                                    : com.google.protobuf.ByteString.EMPTY)
                            .build());
                }
            }
        }

        // Если Origin еще не был установлен — устанавливаем текущий ЦОД как источник
        if (!originPresent) {
            result.add(KafkaRecordHeader.newBuilder()
                    .setKey(HEADER_ORIGIN)
                    .setValue(com.google.protobuf.ByteString.copyFrom(localDcId, StandardCharsets.UTF_8))
                    .build());
        }

        // Инкрементируем счетчик прыжков
        result.add(KafkaRecordHeader.newBuilder()
                .setKey(HEADER_HOP)
                .setValue(com.google.protobuf.ByteString.copyFrom(String.valueOf(currentHop + 1), StandardCharsets.UTF_8))
                .build());

        return result;
    }

    public String getLocalDcId() {
        return localDcId;
    }

    public String getTargetDcId() {
        return targetDcId;
    }
}
