package org.apache.hadoop.explorer.replicator.kafka.service;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.kafka.generated.SchemaSyncRequest;
import org.apache.hadoop.explorer.replicator.kafka.generated.SchemaSyncResponse;
import org.apache.hadoop.explorer.replicator.kafka.producer.KafkaBatchProducer;
import org.apache.hadoop.explorer.replicator.kafka.schema.CachedSchemaRegistryResolver;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Unit Test: KafkaDataTransferServiceImpl (Schema Registration & Sync)")
class KafkaDataTransferServiceImplTest {

    private KafkaBatchProducer batchProducer;
    private ConfluentRegistryClient confluentClient;
    private HwxRegistryClient hwxClient;
    private CachedSchemaRegistryResolver resolver;

    @BeforeEach
    void setUp() {
        batchProducer = mock(KafkaBatchProducer.class);
        confluentClient = mock(ConfluentRegistryClient.class);
        hwxClient = mock(HwxRegistryClient.class);
        resolver = new CachedSchemaRegistryResolver();
    }

    @Test
    @DisplayName("Авторегистрация схемы Confluent: успешная регистрация и сохранение маппинга в кэш резолвера")
    void shouldRegisterSchemaInConfluentRegistryAndPopulateCache() throws Exception {
        String subject = "orders-value";
        String schemaJson = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
        int targetId = 101;
        int sourceId = 5;

        when(confluentClient.registerSchema(subject, schemaJson)).thenReturn(targetId);

        KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(
                batchProducer, null, "DC2", 16, resolver, confluentClient, null);

        SchemaSyncRequest request = SchemaSyncRequest.newBuilder()
                .setSubject(subject)
                .setSchemaType("AVRO")
                .setSchemaText(schemaJson)
                .setSourceSchemaId(sourceId)
                .setSourceRegistryType("CONFLUENT")
                .setTargetRegistryType("CONFLUENT")
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<SchemaSyncResponse> observer = mock(StreamObserver.class);
        service.syncSchemaMetadata(request, observer);

        ArgumentCaptor<SchemaSyncResponse> responseCaptor = ArgumentCaptor.forClass(SchemaSyncResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();

        SchemaSyncResponse response = responseCaptor.getValue();
        assertTrue(response.getSuccess());
        assertEquals(subject, response.getSubject());
        assertEquals(targetId, response.getTargetSchemaId());

        // Проверяем, что ID закэширован в резолвере
        assertEquals(targetId, resolver.resolveConfluentToConfluent("orders", sourceId));
    }

    @Test
    @DisplayName("Авторегистрация схемы Hortonworks: успешная регистрация и сохранение маппинга в кэш")
    void shouldRegisterSchemaInHwxRegistryAndPopulateCache() throws Exception {
        String subject = "sensors-value";
        String schemaJson = "{\"type\":\"record\",\"name\":\"Sensor\",\"fields\":[{\"name\":\"temp\",\"type\":\"double\"}]}";
        long targetVersionId = 8888L;
        long sourceId = 12L;

        when(hwxClient.registerSchemaVersion(subject, schemaJson)).thenReturn(targetVersionId);

        KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(
                batchProducer, null, "DC2", 16, resolver, null, hwxClient);

        SchemaSyncRequest request = SchemaSyncRequest.newBuilder()
                .setSubject(subject)
                .setSchemaType("AVRO")
                .setSchemaText(schemaJson)
                .setSourceSchemaId(sourceId)
                .setSourceRegistryType("HORTONWORKS")
                .setTargetRegistryType("HORTONWORKS")
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<SchemaSyncResponse> observer = mock(StreamObserver.class);
        service.syncSchemaMetadata(request, observer);

        ArgumentCaptor<SchemaSyncResponse> responseCaptor = ArgumentCaptor.forClass(SchemaSyncResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();

        SchemaSyncResponse response = responseCaptor.getValue();
        assertTrue(response.getSuccess());
        assertEquals(targetVersionId, response.getTargetSchemaId());

        // Проверяем кэширование в HWX-кэше
        assertEquals(targetVersionId, resolver.resolveHwxToHwx("sensors", sourceId));
    }

    @Test
    @DisplayName("Обработка сбоя регистрации: возврат ошибки в ответе без падения gRPC вызова")
    void shouldHandleRegistryErrorGracefully() throws Exception {
        String subject = "failing-value";
        when(confluentClient.registerSchema(anyString(), anyString()))
                .thenThrow(new IOException("Connection refused to Schema Registry"));

        KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(
                batchProducer, null, "DC2", 16, resolver, confluentClient, null);

        SchemaSyncRequest request = SchemaSyncRequest.newBuilder()
                .setSubject(subject)
                .setSchemaText("{\"type\":\"string\"}")
                .setSourceSchemaId(1)
                .setTargetRegistryType("CONFLUENT")
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<SchemaSyncResponse> observer = mock(StreamObserver.class);
        service.syncSchemaMetadata(request, observer);

        ArgumentCaptor<SchemaSyncResponse> responseCaptor = ArgumentCaptor.forClass(SchemaSyncResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();

        SchemaSyncResponse response = responseCaptor.getValue();
        assertFalse(response.getSuccess());
        assertEquals(-1, response.getTargetSchemaId());
        assertTrue(response.getErrorMessage().contains("Connection refused"));
    }

    @Test
    @DisplayName("Graceful Fallback: если клиенты реестров не заданы, возвращается sourceSchemaId")
    void shouldFallbackToSourceSchemaIdWhenNoRegistryClientConfigured() {
        KafkaDataTransferServiceImpl service = new KafkaDataTransferServiceImpl(
                batchProducer, null, "DC2", 16, null, null, null);

        SchemaSyncRequest request = SchemaSyncRequest.newBuilder()
                .setSubject("passthrough-topic-value")
                .setSchemaText("{\"type\":\"string\"}")
                .setSourceSchemaId(99)
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<SchemaSyncResponse> observer = mock(StreamObserver.class);
        service.syncSchemaMetadata(request, observer);

        ArgumentCaptor<SchemaSyncResponse> responseCaptor = ArgumentCaptor.forClass(SchemaSyncResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();

        SchemaSyncResponse response = responseCaptor.getValue();
        assertTrue(response.getSuccess());
        assertEquals(99, response.getTargetSchemaId());
    }
}
