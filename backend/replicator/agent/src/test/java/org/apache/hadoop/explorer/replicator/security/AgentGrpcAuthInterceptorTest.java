package org.apache.hadoop.explorer.replicator.security;

import io.grpc.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DisplayName("AgentGrpcAuthInterceptor Tests")
class AgentGrpcAuthInterceptorTest {

    @Test
    @DisplayName("Пропуск вызова при совпадении x-agent-secret")
    void testAuthorizedWhenSecretMatches() {
        AgentGrpcAuthInterceptor interceptor = new AgentGrpcAuthInterceptor("super-secret-token");
        ServerCall<String, String> serverCall = mock(ServerCall.class);
        ServerCallHandler<String, String> nextHandler = mock(ServerCallHandler.class);
        ServerCall.Listener<String> expectedListener = mock(ServerCall.Listener.class);
        when(nextHandler.startCall(any(), any())).thenReturn(expectedListener);

        Metadata headers = new Metadata();
        headers.put(AgentGrpcAuthInterceptor.AGENT_SECRET_HEADER, "super-secret-token");

        ServerCall.Listener<String> result = interceptor.interceptCall(serverCall, headers, nextHandler);

        assertSame(expectedListener, result);
        verify(nextHandler).startCall(serverCall, headers);
        verify(serverCall, never()).close(any(), any());
    }

    @Test
    @DisplayName("Отклонение вызова со статусом UNAUTHENTICATED при неверном x-agent-secret")
    void testRejectedWhenSecretMismatch() {
        AgentGrpcAuthInterceptor interceptor = new AgentGrpcAuthInterceptor("correct-secret");
        ServerCall<String, String> serverCall = mock(ServerCall.class);
        MethodDescriptor<String, String> methodDesc = mock(MethodDescriptor.class);
        when(methodDesc.getFullMethodName()).thenReturn("DataTransferService/transferFile");
        when(serverCall.getMethodDescriptor()).thenReturn(methodDesc);
        when(serverCall.getAttributes()).thenReturn(Attributes.EMPTY);

        ServerCallHandler<String, String> nextHandler = mock(ServerCallHandler.class);

        Metadata headers = new Metadata();
        headers.put(AgentGrpcAuthInterceptor.AGENT_SECRET_HEADER, "wrong-secret");

        ServerCall.Listener<String> result = interceptor.interceptCall(serverCall, headers, nextHandler);

        assertNotNull(result);
        verify(nextHandler, never()).startCall(any(), any());

        ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(serverCall).close(statusCaptor.capture(), any(Metadata.class));
        assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
    }

    @Test
    @DisplayName("Отклонение вызова при отсутствии заголовка x-agent-secret")
    void testRejectedWhenSecretMissing() {
        AgentGrpcAuthInterceptor interceptor = new AgentGrpcAuthInterceptor("correct-secret");
        ServerCall<String, String> serverCall = mock(ServerCall.class);
        MethodDescriptor<String, String> methodDesc = mock(MethodDescriptor.class);
        when(methodDesc.getFullMethodName()).thenReturn("DataTransferService/transferFile");
        when(serverCall.getMethodDescriptor()).thenReturn(methodDesc);
        when(serverCall.getAttributes()).thenReturn(Attributes.EMPTY);

        ServerCallHandler<String, String> nextHandler = mock(ServerCallHandler.class);
        Metadata headers = new Metadata();

        interceptor.interceptCall(serverCall, headers, nextHandler);

        verify(nextHandler, never()).startCall(any(), any());
        ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(serverCall).close(statusCaptor.capture(), any(Metadata.class));
        assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
    }

    @Test
    @DisplayName("Пропуск вызова если ожидаемый секрет не сконфигурирован (null или пустой)")
    void testAllowedWhenNoSecretConfigured() {
        AgentGrpcAuthInterceptor interceptor = new AgentGrpcAuthInterceptor(null);
        ServerCall<String, String> serverCall = mock(ServerCall.class);
        ServerCallHandler<String, String> nextHandler = mock(ServerCallHandler.class);
        ServerCall.Listener<String> expectedListener = mock(ServerCall.Listener.class);
        when(nextHandler.startCall(any(), any())).thenReturn(expectedListener);

        Metadata headers = new Metadata();
        ServerCall.Listener<String> result = interceptor.interceptCall(serverCall, headers, nextHandler);

        assertSame(expectedListener, result);
        verify(nextHandler).startCall(serverCall, headers);
    }

    @Test
    @DisplayName("createClientInterceptor автоматически добавляет x-agent-secret в исходящие вызовы")
    void testClientInterceptorAttachesSecret() {
        ClientInterceptor clientInterceptor = AgentGrpcAuthInterceptor.createClientInterceptor("my-agent-token");
        Channel channel = mock(Channel.class);
        ClientCall rawCall = mock(ClientCall.class);
        doReturn(rawCall).when(channel).newCall(any(), any());

        MethodDescriptor<String, String> method = mock(MethodDescriptor.class);
        ClientCall<String, String> forwardedCall = clientInterceptor.interceptCall(method, CallOptions.DEFAULT, channel);

        Metadata outgoingHeaders = new Metadata();
        ClientCall.Listener<String> listener = mock(ClientCall.Listener.class);
        forwardedCall.start(listener, outgoingHeaders);

        assertEquals("my-agent-token", outgoingHeaders.get(AgentGrpcAuthInterceptor.AGENT_SECRET_HEADER));
        verify(rawCall).start(eq(listener), eq(outgoingHeaders));
    }
}
