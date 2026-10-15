package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.security.SpnegoAuthenticator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("Тестирование SPNEGO аутентификации в REST клиентах Schema Registry")
class RegistryClientSpnegoTest {

    @Test
    @DisplayName("ConfluentRegistryClient превентивно отправляет заголовок Authorization: Negotiate")
    void confluentClientShouldSendSpnegoHeaderPreemptively() throws Exception {
        HttpClient mockHttp = mock(HttpClient.class);
        HttpResponse<String> mockResp = mock(HttpResponse.class);
        when(mockResp.statusCode()).thenReturn(200);
        when(mockResp.body()).thenReturn("{\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}");

        SpnegoAuthenticator mockAuth = mock(SpnegoAuthenticator.class);
        when(mockAuth.generateNegotiateHeader(any(URI.class))).thenReturn("Negotiate TEST_SPNEGO_TOKEN_123");

        doReturn(mockResp).when(mockHttp).send(any(HttpRequest.class), any());

        ConfluentRegistryClient client = new ConfluentRegistryClient("http://confluent-sr:8081", mockHttp, mockAuth);
        String schema = client.getSchemaById(42);

        assertNotNull(schema);
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockHttp).send(captor.capture(), any());

        HttpRequest sentReq = captor.getValue();
        assertEquals("Negotiate TEST_SPNEGO_TOKEN_123", sentReq.headers().firstValue("Authorization").orElse(null));
    }

    @Test
    @DisplayName("HwxRegistryClient отправляет заголовок Authorization: Negotiate")
    void hwxClientShouldSendSpnegoHeader() throws Exception {
        HttpClient mockHttp = mock(HttpClient.class);
        HttpResponse<String> mockResp = mock(HttpResponse.class);
        when(mockResp.statusCode()).thenReturn(200);
        when(mockResp.body()).thenReturn("{\"schemaText\":\"{\\\"type\\\":\\\"record\\\"}\"}");

        SpnegoAuthenticator mockAuth = mock(SpnegoAuthenticator.class);
        when(mockAuth.generateNegotiateHeader(any(URI.class))).thenReturn("Negotiate HWX_SPNEGO_TOKEN");

        doReturn(mockResp).when(mockHttp).send(any(HttpRequest.class), any());

        HwxRegistryClient client = new HwxRegistryClient("http://hwx-sr:7788", mockHttp, mockAuth);
        String schema = client.getSchemaByVersionId(101L);

        assertNotNull(schema);
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockHttp).send(captor.capture(), any());

        HttpRequest sentReq = captor.getValue();
        assertEquals("Negotiate HWX_SPNEGO_TOKEN", sentReq.headers().firstValue("Authorization").orElse(null));
    }

    @Test
    @DisplayName("ConfluentRegistryClient повторяет запрос при получении 401 Negotiate challenge")
    void confluentClientShouldRetryOn401NegotiateChallenge() throws Exception {
        HttpClient mockHttp = mock(HttpClient.class);

        // 1-й ответ: 401 с challenge
        HttpResponse<String> resp401 = mock(HttpResponse.class);
        when(resp401.statusCode()).thenReturn(401);
        HttpHeaders headers401 = HttpHeaders.of(Map.of("WWW-Authenticate", List.of("Negotiate")), (k, v) -> true);
        when(resp401.headers()).thenReturn(headers401);

        // 2-й ответ: 200 OK
        HttpResponse<String> resp200 = mock(HttpResponse.class);
        when(resp200.statusCode()).thenReturn(200);
        when(resp200.body()).thenReturn("{\"schema\":\"{}\"}");

        doReturn(resp401).doReturn(resp200).when(mockHttp).send(any(HttpRequest.class), any());

        SpnegoAuthenticator mockAuth = mock(SpnegoAuthenticator.class);
        when(mockAuth.generateNegotiateHeader(any(URI.class))).thenReturn("Negotiate RETRIED_TOKEN");

        ConfluentRegistryClient client = new ConfluentRegistryClient("http://sr:8081", mockHttp, mockAuth);
        String res = client.getSchemaById(1);

        assertNotNull(res);
        // Должно быть 2 вызова send (исходный + retry)
        verify(mockHttp, times(2)).send(any(HttpRequest.class), any());
    }
}
