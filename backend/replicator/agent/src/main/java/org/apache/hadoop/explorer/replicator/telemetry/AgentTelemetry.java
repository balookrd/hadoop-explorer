package org.apache.hadoop.explorer.replicator.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Телеметрия агента: сборка OpenTelemetry SDK и экспорт спанов по OTLP.
 *
 * <p>Агент — обычный Java-процесс без Spring, поэтому SDK собирается здесь руками. Экспорт
 * включается только заданием адреса коллектора ({@code OTEL_EXPORTER_OTLP_ENDPOINT} или
 * {@code --otlp-endpoint}): без него перехватчики gRPC остаются no-op и ничего не стоят, а
 * traceparent не проставляется.
 *
 * <p>Доля трассируемых вызовов задается {@code OTEL_TRACES_SAMPLER_ARG}: на пути передачи
 * данных вызовов на порядки больше, чем в control plane, и трассировать все подряд незачем.
 */
public final class AgentTelemetry implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AgentTelemetry.class);

    private static final String DEFAULT_SAMPLING_RATIO = "0.05";

    private final OpenTelemetrySdk sdk;

    private AgentTelemetry(OpenTelemetrySdk sdk) {
        this.sdk = sdk;
    }

    /**
     * Запуск телеметрии. Возвращает {@code null}, если адрес коллектора не задан:
     * в этом случае трассировка остается выключенной.
     */
    public static AgentTelemetry startIfConfigured(String serviceName, String agentId, String dcId) {
        String endpoint = env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", env("OTEL_EXPORTER_OTLP_ENDPOINT", null));
        if (endpoint == null || endpoint.isBlank()) {
            logger.info("Трассировка выключена: адрес OTLP-коллектора не задан "
                    + "(OTEL_EXPORTER_OTLP_ENDPOINT)");
            return null;
        }

        String tracesEndpoint = endpoint.endsWith("/v1/traces") ? endpoint
                : endpoint.replaceAll("/+$", "") + "/v1/traces";

        double ratio;
        try {
            ratio = Double.parseDouble(env("OTEL_TRACES_SAMPLER_ARG", DEFAULT_SAMPLING_RATIO));
        } catch (NumberFormatException e) {
            ratio = Double.parseDouble(DEFAULT_SAMPLING_RATIO);
        }

        try {
            Resource resource = Resource.getDefault().merge(Resource.create(Attributes.builder()
                    .put("service.name", serviceName)
                    .put("service.instance.id", agentId != null ? agentId : "unknown")
                    .put("replicator.dc_id", dcId != null ? dcId : "unknown")
                    .build()));

            OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                    .setTracerProvider(SdkTracerProvider.builder()
                            .setResource(resource)
                            .setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(ratio)))
                            .addSpanProcessor(BatchSpanProcessor.builder(
                                    OtlpHttpSpanExporter.builder().setEndpoint(tracesEndpoint).build()).build())
                            .build())
                    .setPropagators(io.opentelemetry.context.propagation.ContextPropagators.create(
                            io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance()))
                    .buildAndRegisterGlobal();

            GrpcTracing.setOpenTelemetry(sdk);
            logger.info("Трассировка включена: сервис '{}', коллектор {}, доля трассируемых вызовов {}",
                    serviceName, tracesEndpoint, ratio);
            return new AgentTelemetry(sdk);
        } catch (Exception e) {
            // Телеметрия не причина не запускать агент
            logger.warn("Не удалось включить трассировку ({}): агент продолжает работу без нее",
                    e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {
        try {
            GrpcTracing.setOpenTelemetry(OpenTelemetry.noop());
            sdk.close();
        } catch (Exception e) {
            logger.debug("Ошибка остановки телеметрии: {}", e.getMessage());
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            value = System.getProperty(name.toLowerCase().replace('_', '.'));
        }
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
