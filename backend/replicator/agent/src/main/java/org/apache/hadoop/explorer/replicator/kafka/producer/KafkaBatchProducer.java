package org.apache.hadoop.explorer.replicator.kafka.producer;

import com.google.protobuf.ByteString;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchAck;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaBatchRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecord;
import org.apache.hadoop.explorer.replicator.kafka.generated.KafkaRecordHeader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Высокопроизводительный продюсер для атомарной фиксации батчей реплицированных записей в локальной Kafka.
 */
public class KafkaBatchProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchProducer.class);

    private final KafkaProducer<byte[], byte[]> producer;

    public KafkaBatchProducer(KafkaProducer<byte[], byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
    }

    /**
     * Записывает батч KafkaRecord в локальный кластер с сохранением номеров партиций,
     * ключей, временных меток и Tombstone-маркеров.
     *
     * @param batch входящий пакет записей из gRPC
     * @return подтверждение фиксации пакета (Ack)
     */
    public KafkaBatchAck writeBatch(KafkaBatchRequest batch) {
        String topic = batch.getTopic();
        List<KafkaRecord> records = batch.getRecordsList();
        long receiveTime = System.currentTimeMillis();

        if (records.isEmpty()) {
            return KafkaBatchAck.newBuilder()
                    .setBatchId(batch.getBatchId())
                    .setTopic(topic)
                    .setSuccess(true)
                    .setCommittedRecords(0)
                    .setReceiveTimestampMs(receiveTime)
                    .build();
        }

        try {
            List<Future<RecordMetadata>> futures = new ArrayList<>(records.size());
            long maxOffset = -1;

            for (KafkaRecord r : records) {
                byte[] key = r.getKey().isEmpty() ? null : r.getKey().toByteArray();
                byte[] val = r.getIsTombstone() || !r.hasPayload() ? null : r.getPayload().toByteArray();
                Integer partition = r.getPartition() >= 0 ? r.getPartition() : null;
                Long timestamp = r.getTimestamp() > 0 ? r.getTimestamp() : null;

                ProducerRecord<byte[], byte[]> prodRecord = new ProducerRecord<>(
                        topic,
                        partition,
                        timestamp,
                        key,
                        val
                );

                // Добавляем заголовки
                for (KafkaRecordHeader h : r.getHeadersList()) {
                    prodRecord.headers().add(new RecordHeader(
                            h.getKey(),
                            h.getValue().toByteArray()
                    ));
                }

                futures.add(producer.send(prodRecord));
            }

            // Дожидаемся фиксации всех сообщений батча в брокере
            for (Future<RecordMetadata> f : futures) {
                RecordMetadata rm = f.get(10, TimeUnit.SECONDS);
                if (rm.offset() > maxOffset) {
                    maxOffset = rm.offset();
                }
            }

            return KafkaBatchAck.newBuilder()
                    .setBatchId(batch.getBatchId())
                    .setTopic(topic)
                    .setSuccess(true)
                    .setCommittedRecords(records.size())
                    .setMaxTargetOffset(maxOffset)
                    .setReceiveTimestampMs(receiveTime)
                    .build();

        } catch (Exception e) {
            log.error("Ошибка фиксации пакета {} в топике {}: {}", batch.getBatchId(), topic, e.getMessage(), e);
            return KafkaBatchAck.newBuilder()
                    .setBatchId(batch.getBatchId())
                    .setTopic(topic)
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .setReceiveTimestampMs(receiveTime)
                    .build();
        }
    }

    public void flush() {
        producer.flush();
    }
}
