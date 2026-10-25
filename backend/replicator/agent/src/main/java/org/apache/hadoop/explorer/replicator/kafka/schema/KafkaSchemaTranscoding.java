package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.generated.PayloadFormat;
import org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Выбор резолвера идентификаторов схем и форматов полезной нагрузки для демона Kafka.
 *
 * <p>Демон подставлял в транскодер заглушку: все четыре метода {@link SchemaIdResolver}
 * возвращали исходный идентификатор, форматы источника и приемника были жестко заданы как
 * {@code PAYLOAD_PASSTHROUGH}, а настоящий {@link CachedSchemaRegistryResolver} в рабочем коде не
 * создавался. Параметры Schema Registry при этом ни на что не влияли, и при разных
 * идентификаторах схем в кластерах потребители на целевой стороне не разобрали бы сообщения.
 *
 * <p>Passthrough остается законным режимом: если реестры не настроены, перенос байт как есть —
 * единственное безопасное поведение, а не деградация.
 */
public final class KafkaSchemaTranscoding {

    private static final Logger log = LoggerFactory.getLogger(KafkaSchemaTranscoding.class);

    private KafkaSchemaTranscoding() {
    }

    /**
     * Резолвер идентификаторов схем.
     *
     * <p>Трансляция включается только когда известны оба реестра: с одним известным адресом в
     * целевой кластер уходили бы идентификаторы чужого реестра.
     */
    public static SchemaIdResolver resolver(String sourceSrUrl, String sourceSrType,
                                            String targetSrUrl, String targetSrType,
                                            KafkaKerberosConfig sourceKrb, KafkaKerberosConfig targetKrb) {
        if (isBlank(sourceSrUrl) || isBlank(targetSrUrl)) {
            log.info("Трансляция идентификаторов схем выключена: заданы не оба реестра "
                    + "(источник: {}, приемник: {})", describe(sourceSrUrl), describe(targetSrUrl));
            return passthroughResolver();
        }
        try {
            CachedSchemaRegistryResolver resolver = CachedSchemaRegistryResolver.create(
                    sourceSrUrl.trim(), normalizeType(sourceSrType),
                    targetSrUrl.trim(), normalizeType(targetSrType),
                    sourceKrb, targetKrb);
            log.info("Трансляция идентификаторов схем включена: {} ({}) -> {} ({})",
                    sourceSrUrl.trim(), normalizeType(sourceSrType),
                    targetSrUrl.trim(), normalizeType(targetSrType));
            return resolver;
        } catch (Exception e) {
            // Сбой инициализации реестров не повод останавливать репликацию: байты переносятся
            // как есть, но в журнале остается явная причина, почему трансляции нет.
            log.error("Не удалось инициализировать реестры схем ({}), трансляция выключена: {}",
                    sourceSrUrl + " -> " + targetSrUrl, e.getMessage());
            return passthroughResolver();
        }
    }

    /** Перенос идентификаторов схем без изменений. */
    public static SchemaIdResolver passthroughResolver() {
        return new SchemaIdResolver() {
            @Override public int resolveConfluentToConfluent(String topic, int sourceId) { return sourceId; }
            @Override public int resolveHwxToConfluent(String topic, long hwxVersionId) { return (int) hwxVersionId; }
            @Override public long resolveConfluentToHwx(String topic, int confluentId) { return confluentId; }
            @Override public long resolveHwxToHwx(String topic, long sourceHwxId) { return sourceHwxId; }
        };
    }

    /**
     * Формат полезной нагрузки из конфигурации.
     *
     * <p>При выключенном транскодинге всегда {@code PAYLOAD_PASSTHROUGH}. При включенном
     * неизвестное или незаданное имя отдается автоопределению: классификатор разберет формат по
     * заголовку сообщения, и это лучше, чем молча потерять транскодинг.
     */
    public static PayloadFormat format(String configured, boolean transcodingEnabled) {
        if (!transcodingEnabled) {
            return PayloadFormat.PAYLOAD_PASSTHROUGH;
        }
        if (isBlank(configured)) {
            return PayloadFormat.PAYLOAD_AUTO_DETECT;
        }
        String value = configured.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        if (!value.startsWith("PAYLOAD_")) {
            value = "PAYLOAD_" + value;
        }
        switch (value) {
            case "PAYLOAD_PASSTHROUGH":
                return PayloadFormat.PAYLOAD_PASSTHROUGH;
            case "PAYLOAD_AUTO_DETECT":
            case "PAYLOAD_AUTO":
                return PayloadFormat.PAYLOAD_AUTO_DETECT;
            case "PAYLOAD_CONFLUENT_AVRO":
            case "PAYLOAD_CONFLUENT":
                return PayloadFormat.PAYLOAD_CONFLUENT_AVRO;
            case "PAYLOAD_HWX_AVRO":
            case "PAYLOAD_HWX":
            case "PAYLOAD_HORTONWORKS":
                return PayloadFormat.PAYLOAD_HWX_AVRO;
            default:
                log.warn("Неизвестный формат полезной нагрузки '{}', включено автоопределение", configured);
                return PayloadFormat.PAYLOAD_AUTO_DETECT;
        }
    }

    private static String normalizeType(String type) {
        if (isBlank(type)) {
            return "CONFLUENT";
        }
        String value = type.trim().toUpperCase(java.util.Locale.ROOT);
        return ("HWX".equals(value) || "HORTONWORKS".equals(value)) ? "HORTONWORKS" : value;
    }

    private static String describe(String url) {
        return isBlank(url) ? "не задан" : url.trim();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
