package io.github.balookrd.pristan.parom.orchestrator.metrics;

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
public class ParomMetricsController {

    private final PrometheusMeterRegistry prometheusMeterRegistry;
    private final ParomMetricsService paromMetricsService;

    public ParomMetricsController(
            PrometheusMeterRegistry prometheusMeterRegistry,
            ParomMetricsService paromMetricsService
    ) {
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.paromMetricsService = paromMetricsService;
    }

    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4; charset=utf-8")
    public ResponseEntity<String> scrapeMetrics() {
        String baseMetrics = prometheusMeterRegistry.scrape();
        String paromMetrics = paromMetricsService.renderParomPrometheusMetrics();

        StringBuilder fullOutput = new StringBuilder(baseMetrics);
        if (!paromMetrics.isBlank()) {
            if (!baseMetrics.endsWith("\n")) {
                fullOutput.append("\n");
            }
            fullOutput.append(paromMetrics);
        }

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/plain; version=0.0.4; charset=utf-8"))
                .body(fullOutput.toString());
    }
}
