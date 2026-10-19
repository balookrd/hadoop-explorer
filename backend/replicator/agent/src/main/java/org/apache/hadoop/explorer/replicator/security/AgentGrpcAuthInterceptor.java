package org.apache.hadoop.explorer.replicator.security;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * gRPC L7 интерцептор аутентификации между агентами репликации с использованием X-Agent-Secret.
 * Защищает удаленные вызовы gRPC сервисов (DataTransferService, HmsTransferService)
 * от несанкционированного доступа.
 */
public class AgentGrpcAuthInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AgentGrpcAuthInterceptor.class);

    public static final Metadata.Key<String> AGENT_SECRET_HEADER =
            Metadata.Key.of("x-agent-secret", Metadata.ASCII_STRING_MARSHALLER);

    public static final Context.Key<String> AGENT_SECRET_CONTEXT_KEY =
            Context.key("x-agent-secret");

    /**
     * Возвращает секрет агента, переданный в текущем gRPC запросе, из gRPC Context.
     */
    public static String getAuthenticatedSecret() {
        return AGENT_SECRET_CONTEXT_KEY.get();
    }

    private final String expectedSecret;
    private final boolean allowAnonymous;

    public AgentGrpcAuthInterceptor(String expectedSecret) {
        this(expectedSecret, false);
    }

    public AgentGrpcAuthInterceptor(String expectedSecret, boolean allowAnonymous) {
        this.expectedSecret = expectedSecret;
        this.allowAnonymous = allowAnonymous;
    }

    public boolean isAllowAnonymous() {
        return allowAnonymous;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String clientSecret = headers.get(AGENT_SECRET_HEADER);
        if (expectedSecret == null || expectedSecret.isBlank()) {
            if (!allowAnonymous) {
                log.error("Отклонен gRPC вызов '{}': интерцептор настроен в защищенном режиме Fail-Closed, но x-agent-secret не сконфигурирован",
                        call.getMethodDescriptor().getFullMethodName());
                call.close(Status.UNAUTHENTICATED.withDescription("Agent authentication is unconfigured (Fail-Closed)"), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }
            Context ctx = Context.current().withValue(AGENT_SECRET_CONTEXT_KEY, clientSecret);
            return Contexts.interceptCall(ctx, call, headers, next);
        }

        if (clientSecret == null || !MessageDigest.isEqual(
                expectedSecret.getBytes(StandardCharsets.UTF_8),
                clientSecret.getBytes(StandardCharsets.UTF_8))) {
            log.warn("Отклонен неавторизованный gRPC вызов '{}' от '{}': неверный или отсутствующий x-agent-secret",
                    call.getMethodDescriptor().getFullMethodName(),
                    call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid or missing X-Agent-Secret"), new Metadata());
            return new ServerCall.Listener<ReqT>() {};
        }

        Context ctx = Context.current().withValue(AGENT_SECRET_CONTEXT_KEY, clientSecret);
        return Contexts.interceptCall(ctx, call, headers, next);
    }

    /**
     * Создает ClientInterceptor для автоматического добавления заголовка x-agent-secret во все исходящие вызовы.
     */
    public static ClientInterceptor createClientInterceptor(String agentSecret) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        if (agentSecret != null && !agentSecret.isBlank()) {
                            headers.put(AGENT_SECRET_HEADER, agentSecret);
                        }
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }
}
