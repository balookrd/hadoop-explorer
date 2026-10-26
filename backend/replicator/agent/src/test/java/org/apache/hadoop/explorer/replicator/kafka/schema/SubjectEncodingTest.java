package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Имя subject в пути запроса к Schema Registry.
 *
 * <p>Имя приходит извне — из топика в gRPC-запросе — и подставлялось в URL как есть. Косая
 * черта или {@code ..} в нем уводили запрос на другой путь API реестра, а {@code ?} и
 * {@code #} обрезали путь и подменяли параметры.
 */
@DisplayName("SEC: имя subject экранируется в пути запроса")
class SubjectEncodingTest {

    @Test
    @DisplayName("Разделители пути и параметров экранируются")
    void pathAndQuerySeparatorsShouldBeEncoded() {
        assertEquals("a%2F..%2Fconfig", ConfluentRegistryClient.encodeSubject("a/../config"));
        assertEquals("topic%3Fid%3D1", ConfluentRegistryClient.encodeSubject("topic?id=1"));
        assertEquals("topic%23fragment", ConfluentRegistryClient.encodeSubject("topic#fragment"));
    }

    @Test
    @DisplayName("Обычное имя subject не искажается")
    void ordinarySubjectShouldStayReadable() {
        assertEquals("sales-events-value", ConfluentRegistryClient.encodeSubject("sales-events-value"));
        assertEquals("db.public.orders-key", ConfluentRegistryClient.encodeSubject("db.public.orders-key"));
    }

    @Test
    @DisplayName("Пробел кодируется как %20, а не как плюс")
    void spaceShouldBePercentEncoded() {
        assertEquals("topic%20name", ConfluentRegistryClient.encodeSubject("topic name"),
                "В пути URL плюс означает плюс, а не пробел");
    }
}
