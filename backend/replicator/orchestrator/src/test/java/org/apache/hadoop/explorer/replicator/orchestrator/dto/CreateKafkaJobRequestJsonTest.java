package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Контракт тела POST /replicator/kafka/jobs с интерфейсом.
 *
 * <p>Оркестратор читает JSON в snake_case ({@code spring.jackson.property-naming-strategy}), а
 * интерфейс слал camelCase. Незнакомые поля Jackson молча отбрасывал, и задача создавалась с
 * дефолтами DTO: DC1 -> DC2, Kerberos выключен, консьюмер-группы и приемники пустые. Проверка
 * идет через ObjectMapper приложения, то есть с той же стратегией имен, что у контроллера.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Kafka: JSON создания задачи из интерфейса")
class CreateKafkaJobRequestJsonTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Поля в snake_case доходят до DTO, включая Kerberos и вложенные топики")
    void testSnakeCasePayloadIsFullyBound() throws Exception {
        String json = """
                {
                  "name": "orders-to-dr",
                  "source_dc": "MSK-1",
                  "target_dc": "MSK-2",
                  "direction": "UNIDIRECTIONAL",
                  "topics": [{"topic": "orders", "payload_mode": "PASSTHROUGH"}],
                  "consumer_groups": ["billing"],
                  "source_kerberos_enabled": true,
                  "source_principal": "replicator@PROD.LOCAL",
                  "target_kerberos_enabled": true,
                  "target_principal": "replicator@DR.LOCAL",
                  "bandwidth_limit_mb": 800.0,
                  "consumer_threads": 4,
                  "batch_size": 5000,
                  "target_endpoints": ["agent-dr-1:50052", "agent-dr-2:50052"]
                }
                """;

        CreateKafkaJobRequest request = objectMapper.readValue(json, CreateKafkaJobRequest.class);

        assertEquals("MSK-1", request.getSourceDc());
        assertEquals("MSK-2", request.getTargetDc());
        assertEquals("UNIDIRECTIONAL", request.getDirection());
        assertEquals("PASSTHROUGH", request.getTopics().get(0).getPayloadMode());
        assertEquals(List.of("billing"), request.getConsumerGroups());
        assertTrue(request.isSourceKerberosEnabled());
        assertEquals("replicator@PROD.LOCAL", request.getSourcePrincipal());
        assertTrue(request.isTargetKerberosEnabled());
        assertEquals("replicator@DR.LOCAL", request.getTargetPrincipal());
        assertEquals(800.0, request.getBandwidthLimitMb());
        assertEquals(4, request.getConsumerThreads());
        assertEquals(5000, request.getBatchSize());
        assertEquals(2, request.getTargetEndpoints().size());
    }
}
