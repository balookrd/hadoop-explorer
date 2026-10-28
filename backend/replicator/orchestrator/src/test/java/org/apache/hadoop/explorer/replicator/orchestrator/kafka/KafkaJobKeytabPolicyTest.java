package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.controller.KafkaReplicationController;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateKafkaJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.KafkaReplicationJobDto;
import org.apache.hadoop.explorer.replicator.orchestrator.service.KafkaReplicationOrchestratorService;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Путь keytab в задаче от не-администратора.
 *
 * <p>Интерфейс подставлял путь keytab по умолчанию, и любой не-администратор получал 403 при
 * создании задачи. Запрещен только путь, отличный от keytab ЦОД в топологии.
 */
@DisplayName("Kafka: keytab из топологии не мешает создавать задачи не-администраторам")
class KafkaJobKeytabPolicyTest {

    private static final String KEYTAB = "/etc/security/keytabs/replicator-dc1.keytab";
    private KafkaReplicationOrchestratorService service;
    private KafkaReplicationController controller;

    @BeforeEach
    void setUp() {
        ReplicatorProperties.ClusterConfig dc1 = new ReplicatorProperties.ClusterConfig("dc1", "dc1", "dc1", null, "h", 1);
        dc1.setKerberosEnabled(true);
        dc1.setPrincipal("replicator@PROD.LOCAL");
        dc1.setKeytabPath(KEYTAB);
        dc1.setKafkaSecurityProtocol("SASL_SSL");
        dc1.setKafkaServiceName("kafka-prod");
        dc1.setKafkaTruststoreLocation("/etc/security/kafka/truststore.jks");
        dc1.setKafkaTruststorePassword("secret");
        ReplicatorProperties props = new ReplicatorProperties();
        props.setClusters(List.of(dc1));
        service = mock(KafkaReplicationOrchestratorService.class);
        when(service.createJob(any())).thenReturn(new KafkaReplicationJobDto());
        controller = new KafkaReplicationController(service, new TopologyRegistry(props));
        UserSession writer = new UserSession("analyst", "Analyst", "a@corp", List.of("kafka-writers"),
                "TEST", true, Role.WRITER);
        SecurityContextHolder.getContext().setAuthentication(new CommonAuthenticationToken(writer));
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    private static CreateKafkaJobRequest request(String sourceKeytab) {
        CreateKafkaJobRequest request = new CreateKafkaJobRequest();
        request.setName("orders");
        request.setSourceDc("dc1");
        request.setTargetDc("dc2");
        request.setSourceKeytabPath(sourceKeytab);
        return request;
    }

    @Test
    @DisplayName("Пустой путь и путь из топологии принимаются")
    void testTopologyKeytabIsAccepted() {
        assertEquals(HttpStatus.CREATED, controller.createJob(request(null)).getStatusCode());
        assertEquals(HttpStatus.CREATED, controller.createJob(request(KEYTAB)).getStatusCode());
        verify(service, times(2)).createJob(argThat(r -> KEYTAB.equals(r.getSourceKeytabPath())));
    }

    @Test
    @DisplayName("Произвольный путь keytab по-прежнему запрещен")
    void testCustomKeytabIsForbidden() {
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> controller.createJob(request("/home/analyst/stolen.keytab")));
        assertEquals(HttpStatus.FORBIDDEN, e.getStatusCode());
        verify(service, never()).createJob(any());
    }

    @Test
    @DisplayName("Discovery показывает протокол, service name и наличие truststore, но не путь и пароль")
    void testDiscoveryExposesSecuritySummary() {
        var cluster = controller.discoverClusters().get(0);
        assertEquals("SASL_SSL", cluster.get("securityProtocol"));
        assertEquals("kafka-prod", cluster.get("serviceName"));
        assertEquals(true, cluster.get("truststoreConfigured"));
        assertEquals("[PROTECTED]", cluster.get("keytabPath"));
        assertFalse(cluster.toString().contains("truststore.jks"), cluster.toString());
        assertFalse(cluster.toString().contains("secret"), cluster.toString());
    }

    @Test
    @DisplayName("Service name брокеров подставляется из топологии")
    void testServiceNameFromTopology() {
        controller.createJob(request(null));
        verify(service).createJob(argThat(r -> "kafka-prod".equals(r.getSourceServiceName())));
    }
}
