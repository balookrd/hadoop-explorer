package org.apache.hadoop.explorer.replicator.telemetry;

import io.grpc.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.MDC;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Сквозная трассировка вызовов gRPC между оркестратором и агентами.
 *
 * <p>Операция репликации проходит через несколько процессов: агент запрашивает задачу у
 * оркестратора, затем агент-отправитель передает данные агенту-приемнику, а схемы Hive и батчи
 * Kafka идут своими вызовами. Связать эти участки между собой было нечем. Клиентский перехватчик
 * кладет в метаданные W3C traceparent, серверный — поднимает его и продолжает тот же trace,
 * дополнительно выставляя trace_id в MDC, чтобы по логам разных процессов собиралась одна операция.
 *
 * <p>Без подключенного SDK {@link OpenTelemetry} работает как no-op: спаны невалидны, заголовок
 * не проставляется, накладных расходов нет. Телеметрия никогда не мешает обслуживанию вызова —
 * любая ошибка внутри перехватчика не должна его ронять.
 */
public final class GrpcTracing {

    /** Имя инструментирующей библиотеки в спанах. */
    public static final String INSTRUMENTATION_NAME = "org.apache.hadoop.explorer.replicator";

    public static final String MDC_TRACE_ID = "trace_id";
    public static final String MDC_SPAN_ID = "span_id";

    private static final TextMapSetter<Metadata> SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
        }
    };

    private static final TextMapGetter<Metadata> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Metadata carrier) {
            return carrier.keys();
        }

        @Override
        public String get(Metadata carrier, String key) {
            if (carrier == null) {
                return null;
            }
            return carrier.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
        }
    };

    private static volatile OpenTelemetry explicitInstance;

    private GrpcTracing() {
    }

    /**
     * Явная установка экземпляра OpenTelemetry. Оркестратор передает сюда бин Spring Boot,
     * агент — собранный SDK; без вызова используется глобальный экземпляр (по умолчанию no-op).
     */
    public static void setOpenTelemetry(OpenTelemetry instance) {
        explicitInstance = instance;
    }

    private static OpenTelemetry openTelemetry() {
        OpenTelemetry instance = explicitInstance;
        return instance != null ? instance : GlobalOpenTelemetry.get();
    }

    private static Tracer tracer() {
        return openTelemetry().getTracer(INSTRUMENTATION_NAME);
    }

    public static ClientInterceptor clientInterceptor() {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        Span span;
                        try {
                            span = tracer().spanBuilder(method.getFullMethodName())
                                    .setSpanKind(SpanKind.CLIENT)
                                    .startSpan();
                        } catch (RuntimeException e) {
                            super.start(responseListener, headers);
                            return;
                        }

                        AtomicBoolean ended = new AtomicBoolean(false);
                        try (Scope ignored = span.makeCurrent()) {
                            openTelemetry().getPropagators().getTextMapPropagator()
                                    .inject(Context.current(), headers, SETTER);
                            super.start(new ForwardingClientCallListener
                                    .SimpleForwardingClientCallListener<>(responseListener) {
                                @Override
                                public void onClose(Status status, Metadata trailers) {
                                    if (ended.compareAndSet(false, true)) {
                                        if (!status.isOk()) {
                                            span.setStatus(StatusCode.ERROR, String.valueOf(status.getCode()));
                                        }
                                        span.end();
                                    }
                                    super.onClose(status, trailers);
                                }
                            }, headers);
                        } catch (RuntimeException | Error e) {
                            if (ended.compareAndSet(false, true)) {
                                span.setStatus(StatusCode.ERROR, String.valueOf(e.getMessage()));
                                span.end();
                            }
                            throw e;
                        }
                    }
                };
            }
        };
    }

    public static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                Context parent;
                Span span;
                try {
                    parent = openTelemetry().getPropagators().getTextMapPropagator()
                            .extract(Context.current(), headers, GETTER);
                    span = tracer().spanBuilder(call.getMethodDescriptor().getFullMethodName())
                            .setSpanKind(SpanKind.SERVER)
                            .setParent(parent)
                            .startSpan();
                } catch (RuntimeException e) {
                    // Испорченный заголовок или сбой телеметрии не повод отказывать в вызове
                    return next.startCall(call, headers);
                }

                AtomicBoolean ended = new AtomicBoolean(false);
                ServerCall<ReqT, RespT> tracedCall =
                        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                            @Override
                            public void close(Status status, Metadata trailers) {
                                if (ended.compareAndSet(false, true)) {
                                    if (!status.isOk()) {
                                        span.setStatus(StatusCode.ERROR, String.valueOf(status.getCode()));
                                    }
                                    span.end();
                                }
                                super.close(status, trailers);
                            }
                        };

                ServerCall.Listener<ReqT> delegate;
                try (Scope ignored = span.makeCurrent()) {
                    putMdc(span);
                    try {
                        delegate = next.startCall(tracedCall, headers);
                    } finally {
                        clearMdc();
                    }
                } catch (RuntimeException | Error e) {
                    if (ended.compareAndSet(false, true)) {
                        span.setStatus(StatusCode.ERROR, String.valueOf(e.getMessage()));
                        span.end();
                    }
                    throw e;
                }

                // Обратные вызовы gRPC приходят в других потоках, поэтому контекст и MDC
                // восстанавливаются на каждом из них
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
                    @Override
                    public void onMessage(ReqT message) {
                        inSpan(() -> super.onMessage(message));
                    }

                    @Override
                    public void onHalfClose() {
                        inSpan(super::onHalfClose);
                    }

                    @Override
                    public void onReady() {
                        inSpan(super::onReady);
                    }

                    @Override
                    public void onComplete() {
                        inSpan(super::onComplete);
                        endSpan(null);
                    }

                    @Override
                    public void onCancel() {
                        inSpan(super::onCancel);
                        endSpan("CANCELLED");
                    }

                    private void inSpan(Runnable action) {
                        try (Scope ignored = span.makeCurrent()) {
                            putMdc(span);
                            try {
                                action.run();
                            } finally {
                                clearMdc();
                            }
                        }
                    }

                    private void endSpan(String errorCode) {
                        if (ended.compareAndSet(false, true)) {
                            if (errorCode != null) {
                                span.setStatus(StatusCode.ERROR, errorCode);
                            }
                            span.end();
                        }
                    }
                };
            }
        };
    }

    private static void putMdc(Span span) {
        var context = span.getSpanContext();
        if (context.isValid()) {
            MDC.put(MDC_TRACE_ID, context.getTraceId());
            MDC.put(MDC_SPAN_ID, context.getSpanId());
        }
    }

    private static void clearMdc() {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_SPAN_ID);
    }
}
