package org.apache.hadoop.explorer.replicator.telemetry;

import io.grpc.*;
import io.grpc.ManagedChannel;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Трассировка должна продолжаться через границу процесса.
 *
 * <p>Трассировки в системе не было вовсе — ни OpenTelemetry, ни micrometer-tracing. Для схемы
 * «оркестратор и N агентов × три подсистемы» это главный пробел наблюдаемости: передача файла
 * проходит через вызов агента к оркестратору, затем от агента-отправителя к агенту-приемнику, и
 * связать эти участки между собой было нечем — в логах нет общего идентификатора.
 *
 * <p>Проверяется контракт распространения: клиент кладет в метаданные W3C traceparent, сервер
 * его поднимает и продолжает тот же trace, а отсутствие или порча заголовка вызов не ломают.
 */
@DisplayName("Телеметрия: сквозной trace через gRPC")
class GrpcTracePropagationTest {

    private static final Metadata.Key<String> TRACEPARENT =
            Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);

    private InMemorySpanExporter spans;
    private OpenTelemetrySdk sdk;

    @BeforeEach
    void setUp() {
        spans = InMemorySpanExporter.create();
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spans))
                        .build())
                .setPropagators(io.opentelemetry.context.propagation.ContextPropagators.create(
                        io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance()))
                .build();
        GrpcTracing.setOpenTelemetry(sdk);
    }

    @AfterEach
    void tearDown() {
        GrpcTracing.setOpenTelemetry(OpenTelemetry.noop());
        sdk.close();
    }

    /** Канал-заглушка: запоминает метаданные, с которыми ушел вызов. */
    private Channel capturingChannel(AtomicReference<Metadata> captured) {
        return new Channel() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
                return new ClientCall<>() {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        captured.set(headers);
                    }

                    @Override
                    public void request(int numMessages) {
                    }

                    @Override
                    public void cancel(String message, Throwable cause) {
                    }

                    @Override
                    public void halfClose() {
                    }

                    @Override
                    public void sendMessage(ReqT message) {
                    }
                };
            }

            @Override
            public String authority() {
                return "test-authority";
            }
        };
    }

    private static final MethodDescriptor<String, String> METHOD = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("replicator.control.OrchestratorControlPlane/ClaimTasks")
            .setRequestMarshaller(new StringMarshaller())
            .setResponseMarshaller(new StringMarshaller())
            .build();

    private static class StringMarshaller implements MethodDescriptor.Marshaller<String> {
        @Override
        public java.io.InputStream stream(String value) {
            return new java.io.ByteArrayInputStream(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String parse(java.io.InputStream stream) {
            try {
                return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
    }

    @Test
    @DisplayName("Клиент кладет traceparent в метаданные вызова")
    void testClientInjectsTraceparent() {
        AtomicReference<Metadata> captured = new AtomicReference<>();

        ClientInterceptor interceptor = GrpcTracing.clientInterceptor();
        ClientCall<String, String> call = interceptor.interceptCall(
                METHOD, CallOptions.DEFAULT, capturingChannel(captured));
        call.start(new ClientCall.Listener<>() {
        }, new Metadata());

        Metadata sent = captured.get();
        assertNotNull(sent, "Вызов не ушел в канал");
        String traceparent = sent.get(TRACEPARENT);
        assertNotNull(traceparent,
                "В метаданных нет traceparent: участок вызова не связать с остальной трассой");
        assertTrue(traceparent.startsWith("00-"),
                "traceparent должен быть в формате W3C, получено: " + traceparent);
    }

    @Test
    @DisplayName("Сервер продолжает trace вызывающей стороны")
    void testServerContinuesCallerTrace() {
        // Исходящий вызов клиента: получаем traceparent и trace id
        AtomicReference<Metadata> captured = new AtomicReference<>();
        ClientCall<String, String> call = GrpcTracing.clientInterceptor()
                .interceptCall(METHOD, CallOptions.DEFAULT, capturingChannel(captured));
        call.start(new ClientCall.Listener<>() {
        }, new Metadata());
        String clientTraceId = traceIdOf(captured.get().get(TRACEPARENT));

        // Тот же вызов на стороне сервера
        AtomicReference<SpanContext> serverSpan = new AtomicReference<>();
        GrpcTracing.serverInterceptor().interceptCall(
                serverCall(), captured.get(), (serverCallArg, headers) -> {
                    serverSpan.set(Span.current().getSpanContext());
                    return new ServerCall.Listener<>() {
                    };
                });

        assertTrue(serverSpan.get().isValid(),
                "На сервере нет активного спана: вызов не попадет в трассу");
        assertEquals(clientTraceId, serverSpan.get().getTraceId(),
                "Сервер начал собственный trace вместо продолжения вызывающего: "
                        + "участки одной операции в разных процессах останутся несвязанными");
    }

    @Test
    @DisplayName("Активный спан становится родителем исходящего вызова")
    void testOutgoingCallInheritsActiveSpan() {
        var tracer = sdk.getTracer("test");
        Span parent = tracer.spanBuilder("parent").startSpan();
        AtomicReference<Metadata> captured = new AtomicReference<>();
        try (Scope scope = parent.makeCurrent()) {
            ClientCall<String, String> call = GrpcTracing.clientInterceptor()
                    .interceptCall(METHOD, CallOptions.DEFAULT, capturingChannel(captured));
            call.start(new ClientCall.Listener<>() {
            }, new Metadata());
        } finally {
            parent.end();
        }

        assertEquals(parent.getSpanContext().getTraceId(), traceIdOf(captured.get().get(TRACEPARENT)),
                "Исходящий вызов ушел с собственным trace id, хотя выполнялся внутри операции: "
                        + "трасса распадется на несвязанные куски");
    }

    @Test
    @DisplayName("Вызов без traceparent обслуживается и начинает новый trace")
    void testCallWithoutTraceparentStartsNewTrace() {
        AtomicReference<SpanContext> serverSpan = new AtomicReference<>();
        AtomicReference<Boolean> handled = new AtomicReference<>(false);

        GrpcTracing.serverInterceptor().interceptCall(
                serverCall(), new Metadata(), (callArg, headers) -> {
                    handled.set(true);
                    serverSpan.set(Span.current().getSpanContext());
                    return new ServerCall.Listener<>() {
                    };
                });

        assertTrue(handled.get(), "Вызов без traceparent обязан обслуживаться как обычно");
        assertTrue(serverSpan.get().isValid(), "Должен начаться новый trace");
    }

    @Test
    @DisplayName("Испорченный traceparent не ломает вызов")
    void testBrokenTraceparentDoesNotBreakCall() {
        Metadata broken = new Metadata();
        broken.put(TRACEPARENT, "не-traceparent-вовсе");
        AtomicReference<Boolean> handled = new AtomicReference<>(false);

        assertDoesNotThrow(() -> GrpcTracing.serverInterceptor().interceptCall(
                serverCall(), broken, (callArg, headers) -> {
                    handled.set(true);
                    return new ServerCall.Listener<>() {
                    };
                }), "Телеметрия не должна ронять обслуживание вызова");
        assertTrue(handled.get(), "Вызов обязан быть обслужен несмотря на битый заголовок");
    }

    @Test
    @DisplayName("Идентификатор трассы попадает в MDC для логов")
    void testTraceIdIsExposedToLogs() {
        AtomicReference<String> mdcTraceId = new AtomicReference<>();
        AtomicReference<SpanContext> serverSpan = new AtomicReference<>();

        GrpcTracing.serverInterceptor().interceptCall(
                serverCall(), new Metadata(), (callArg, headers) -> {
                    mdcTraceId.set(org.slf4j.MDC.get("trace_id"));
                    serverSpan.set(Span.current().getSpanContext());
                    return new ServerCall.Listener<>() {
                    };
                });

        assertEquals(serverSpan.get().getTraceId(), mdcTraceId.get(),
                "В MDC нет trace_id: по логам нельзя собрать одну операцию из разных процессов");
    }

    @Test
    @DisplayName("Сквозной вызов через gRPC: спан сервера — ребенок спана клиента")
    void testEndToEndCallLinksClientAndServerSpans() throws Exception {
        String name = io.grpc.inprocess.InProcessServerBuilder.generateName();
        io.grpc.Server server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(ServerServiceDefinition.builder("replicator.control.OrchestratorControlPlane")
                        .addMethod(METHOD, io.grpc.stub.ServerCalls.asyncUnaryCall(
                                (request, responseObserver) -> {
                                    responseObserver.onNext("ok");
                                    responseObserver.onCompleted();
                                }))
                        .build())
                .intercept(GrpcTracing.serverInterceptor())
                .build()
                .start();
        ManagedChannel channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name)
                .directExecutor()
                .intercept(GrpcTracing.clientInterceptor())
                .build();

        try {
            io.grpc.stub.ClientCalls.blockingUnaryCall(channel, METHOD, CallOptions.DEFAULT, "ping");
        } finally {
            channel.shutdownNow().awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        }

        var exported = spans.getFinishedSpanItems();
        assertEquals(2, exported.size(),
                "Ожидались два спана — клиентский и серверный, получено: " + exported.size());

        var client = exported.stream().filter(sp -> sp.getKind() == io.opentelemetry.api.trace.SpanKind.CLIENT)
                .findFirst().orElseThrow(() -> new AssertionError("Нет клиентского спана"));
        var serverSpan = exported.stream().filter(sp -> sp.getKind() == io.opentelemetry.api.trace.SpanKind.SERVER)
                .findFirst().orElseThrow(() -> new AssertionError("Нет серверного спана"));

        assertEquals(client.getTraceId(), serverSpan.getTraceId(),
                "Клиент и сервер оказались в разных трассах: передача не связывается в одну операцию");
        assertEquals(client.getSpanId(), serverSpan.getParentSpanId(),
                "Серверный спан не является ребенком клиентского: в трассе не видно, кто кого вызвал");
        assertEquals("replicator.control.OrchestratorControlPlane/ClaimTasks", serverSpan.getName(),
                "Спан должен называться по методу gRPC, иначе в трассе не видно, что происходило");
    }

    private static String traceIdOf(String traceparent) {
        assertNotNull(traceparent, "traceparent отсутствует");
        return traceparent.split("-")[1];
    }

    private ServerCall<String, String> serverCall() {
        return new ServerCall<>() {
            @Override
            public void request(int numMessages) {
            }

            @Override
            public void sendHeaders(Metadata headers) {
            }

            @Override
            public void sendMessage(String message) {
            }

            @Override
            public void close(Status status, Metadata trailers) {
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public MethodDescriptor<String, String> getMethodDescriptor() {
                return METHOD;
            }
        };
    }
}
