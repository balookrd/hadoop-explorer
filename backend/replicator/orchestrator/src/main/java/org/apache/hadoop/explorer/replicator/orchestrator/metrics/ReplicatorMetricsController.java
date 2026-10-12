package org.apache.hadoop.explorer.replicator.orchestrator.metrics;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST контроллер эндпоинта /metrics для опроса Prometheus сервером.
 * Отдает объединенные метрики Spring Boot Actuator, Micrometer и динамические метрики флота агентов.
 */
@RestController
public class ReplicatorMetricsController {

    private final PrometheusMeterRegistry prometheusMeterRegistry;
    private final ReplicatorMetricsService replicatorMetricsService;

    public ReplicatorMetricsController(
            PrometheusMeterRegistry prometheusMeterRegistry,
            ReplicatorMetricsService replicatorMetricsService
    ) {
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.replicatorMetricsService = replicatorMetricsService;
    }

    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4; charset=utf-8")
    public ResponseEntity<String> scrapeMetrics() {
        String baseMetrics = prometheusMeterRegistry.scrape();
        String replicatorMetrics = replicatorMetricsService.renderReplicatorPrometheusMetrics();

        StringBuilder fullOutput = new StringBuilder(baseMetrics);
        if (!replicatorMetrics.isBlank()) {
            if (!baseMetrics.endsWith("\n")) {
                fullOutput.append("\n");
            }
            fullOutput.append(replicatorMetrics);
        }

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/plain; version=0.0.4; charset=utf-8"))
                .body(fullOutput.toString());
    }
}
