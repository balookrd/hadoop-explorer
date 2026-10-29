package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.security.TlsContextFactory;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.ConfigPatchOp;
import org.apache.hadoop.explorer.yarn.model.DeployResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Launches the capacity-scheduler Job Template in AWX and tracks the job to its real outcome.
 *
 * <p>A deployment is reported as {@code SUCCESS} only when AWX says the job finished
 * successfully. When AWX is not configured the deployment fails; a simulated run happens only
 * with the explicit {@code yarn.awx.simulate=true} used by demo stands.
 */
@Service
public class AwxClientService {

    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_RUNNING = "RUNNING";

    private static final Logger log = LoggerFactory.getLogger(AwxClientService.class);
    private static final Set<String> AWX_FINISHED_OK = Set.of("successful");
    private static final Set<String> AWX_FINISHED_BAD = Set.of("failed", "error", "canceled");
    private static final int STDOUT_LIMIT = 20_000;

    private final YarnProperties yarnProperties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public AwxClientService(YarnProperties yarnProperties, ObjectMapper objectMapper) {
        this(yarnProperties, objectMapper, null);
    }

    @Autowired
    public AwxClientService(YarnProperties yarnProperties, ObjectMapper objectMapper,
                            @Autowired(required = false) CommonSecurityProperties securityProperties) {
        this.yarnProperties = yarnProperties;
        this.objectMapper = objectMapper;
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NORMAL);
        if (!yarnProperties.getAwx().isVerifySsl()) {
            builder.sslContext(TlsContextFactory.createTrustAllSslContext());
        } else if (securityProperties != null && securityProperties.getTls() != null && securityProperties.getTls().isEnabled()) {
            TlsContextFactory.configureHttpClient(builder, securityProperties.getTls());
        }
        this.httpClient = builder.build();
    }

    /**
     * Deploys a change request: the Ansible role applies {@code ops} to the deployed file.
     */
    public DeployResponse deployPatch(ClusterConfig cluster, Long crId, List<ConfigPatchOp> ops,
                                      String appliedBy, String comment, boolean wait) {
        String patchJson;
        try {
            patchJson = objectMapper.writeValueAsString(ops);
        } catch (IOException e) {
            throw new IllegalStateException("Не удалось сериализовать изменения: " + e.getMessage(), e);
        }
        Map<String, Object> vars = new LinkedHashMap<>();
        vars.put("capacity_scheduler_patch_b64", b64(patchJson));
        return launch(cluster, crId, vars, appliedBy, comment, wait);
    }

    /**
     * Replaces the whole capacity-scheduler.xml (emergency path, not subject to review).
     */
    public DeployResponse deployXml(ClusterConfig cluster, String xmlContent, String appliedBy, String comment, boolean wait) {
        Map<String, Object> vars = new LinkedHashMap<>();
        vars.put("capacity_scheduler_xml_b64", b64(xmlContent));
        return launch(cluster, null, vars, appliedBy, comment, wait);
    }

    /**
     * Current status of an AWX job, with the job output once it has finished.
     */
    public DeployResponse jobStatus(ClusterConfig cluster, Long crId, int jobId) {
        YarnProperties.AwxConfig awx = yarnProperties.getAwx();
        if (awx.isSimulate()) {
            return response(cluster, crId, jobId, STATUS_SUCCESS, "Симуляция AWX: задача #" + jobId + " завершена", null);
        }
        try {
            JsonNode job = objectMapper.readTree(get("/api/v2/jobs/" + jobId + "/"));
            String awxStatus = job.path("status").asText("unknown");
            String status = mapStatus(awxStatus);
            String stdout = STATUS_RUNNING.equals(status) ? null : fetchStdout(jobId);
            String message = switch (status) {
                case STATUS_SUCCESS -> "AWX Job #" + jobId + " успешно завершен";
                case STATUS_FAILED -> "AWX Job #" + jobId + " завершился с ошибкой (" + awxStatus + ")";
                default -> "AWX Job #" + jobId + " выполняется (" + awxStatus + ")";
            };
            return response(cluster, crId, jobId, status, message, stdout);
        } catch (Exception e) {
            log.error("Не удалось получить статус AWX Job #{}: {}", jobId, e.getMessage());
            return response(cluster, crId, jobId, STATUS_RUNNING,
                    "Статус AWX Job #" + jobId + " временно недоступен: " + e.getMessage(), null);
        }
    }

    private DeployResponse launch(ClusterConfig cluster, Long crId, Map<String, Object> payloadVars,
                                  String appliedBy, String comment, boolean wait) {
        YarnProperties.AwxConfig awx = yarnProperties.getAwx();
        if (awx.isSimulate()) {
            return simulate(cluster, crId);
        }
        boolean clusterDisabled = cluster.getAwx() != null && !cluster.getAwx().isEnabled();
        if (!awx.isEnabled() || clusterDisabled || awx.getBaseUrl() == null || awx.getBaseUrl().isBlank()) {
            return response(cluster, crId, null, STATUS_FAILED,
                    "Развертывание через AWX отключено для кластера '" + cluster.getId()
                            + "' (yarn.awx.enabled / base-url): изменения не применены", null);
        }

        int jobTemplateId = (cluster.getAwx() != null && cluster.getAwx().getJobTemplateId() != null)
                ? cluster.getAwx().getJobTemplateId()
                : awx.getDefaultJobTemplateId();

        Map<String, Object> extraVars = new LinkedHashMap<>(payloadVars);
        extraVars.put("cluster_id", cluster.getId());
        extraVars.put("change_request_id", crId != null ? String.valueOf(crId) : "direct");
        extraVars.put("applied_by", appliedBy != null ? appliedBy : "system");
        extraVars.put("deploy_comment", comment != null ? comment : "");

        try {
            String body = objectMapper.writeValueAsString(Map.of("extra_vars", extraVars));
            HttpResponse<String> resp = httpClient.send(request("/api/v2/job_templates/" + jobTemplateId + "/launch/")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(), HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() != 201 && resp.statusCode() != 200) {
                log.error("AWX вернул HTTP {} при запуске шаблона {}: {}", resp.statusCode(), jobTemplateId, resp.body());
                return response(cluster, crId, null, STATUS_FAILED,
                        "Ошибка запуска шаблона #" + jobTemplateId + " в AWX: HTTP " + resp.statusCode(), truncate(resp.body()));
            }

            JsonNode node = objectMapper.readTree(resp.body());
            int jobId = node.hasNonNull("job") ? node.path("job").asInt() : node.path("id").asInt(0);
            if (jobId <= 0) {
                return response(cluster, crId, null, STATUS_FAILED, "AWX не вернул идентификатор задачи", truncate(resp.body()));
            }
            log.info("AWX Job #{} запущен для кластера {} (CR {})", jobId, cluster.getId(), crId);
            if (!wait) {
                return response(cluster, crId, jobId, STATUS_RUNNING, "AWX Job #" + jobId + " поставлен в очередь", null);
            }
            return pollUntilFinished(cluster, crId, jobId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return response(cluster, crId, null, STATUS_FAILED, "Запуск AWX прерван", null);
        } catch (Exception e) {
            log.error("Сбой обращения к AWX API: {}", e.toString());
            return response(cluster, crId, null, STATUS_FAILED, "Сбой обращения к AWX API: " + describe(e), null);
        }
    }

    private DeployResponse pollUntilFinished(ClusterConfig cluster, Long crId, int jobId) throws InterruptedException {
        YarnProperties.AwxConfig awx = yarnProperties.getAwx();
        long deadline = System.nanoTime() + Duration.ofSeconds(Math.max(1, awx.getTimeoutSeconds())).toNanos();
        DeployResponse last;
        do {
            last = jobStatus(cluster, crId, jobId);
            if (!STATUS_RUNNING.equals(last.getStatus())) {
                return last;
            }
            Thread.sleep(Duration.ofSeconds(Math.max(1, awx.getPollIntervalSeconds())).toMillis());
        } while (System.nanoTime() < deadline);
        last.setMessage("AWX Job #" + jobId + " еще выполняется; статус можно запросить позже");
        return last;
    }

    private String fetchStdout(int jobId) {
        try {
            return truncate(get("/api/v2/jobs/" + jobId + "/stdout/?format=txt"));
        } catch (Exception e) {
            return "Не удалось получить вывод задачи: " + describe(e);
        }
    }

    private String get(String path) throws IOException, InterruptedException {
        HttpResponse<String> resp = httpClient.send(request(path).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new IOException("HTTP " + resp.statusCode() + " от AWX " + path);
        }
        return resp.body();
    }

    private HttpRequest.Builder request(String path) {
        YarnProperties.AwxConfig awx = yarnProperties.getAwx();
        return HttpRequest.newBuilder()
                .uri(URI.create(awx.getBaseUrl().replaceAll("/+$", "") + path))
                .header("Authorization", "Bearer " + awx.getToken())
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(15));
    }

    static String mapStatus(String awxStatus) {
        if (AWX_FINISHED_OK.contains(awxStatus)) {
            return STATUS_SUCCESS;
        }
        if (AWX_FINISHED_BAD.contains(awxStatus)) {
            return STATUS_FAILED;
        }
        return STATUS_RUNNING;
    }

    private DeployResponse simulate(ClusterConfig cluster, Long crId) {
        int jobId = ThreadLocalRandom.current().nextInt(1000, 9999);
        return response(cluster, crId, jobId, STATUS_SUCCESS,
                "[СИМУЛЯЦИЯ] yarn.awx.simulate=true: AWX не вызывался, кластер не изменен (Job #" + jobId + ")",
                "SIMULATION: no AWX job was launched and no ResourceManager was changed.");
    }

    private DeployResponse response(ClusterConfig cluster, Long crId, Integer jobId, String status, String message, String stdout) {
        return DeployResponse.builder()
                .crId(crId)
                .clusterId(cluster.getId())
                .awxJobId(jobId)
                .status(status)
                .message(message)
                .deployedAt(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(ZoneOffset.UTC).format(Instant.now()))
                .stdout(stdout)
                .build();
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String truncate(String s) {
        if (s == null || s.length() <= STDOUT_LIMIT) {
            return s;
        }
        return "...\n" + s.substring(s.length() - STDOUT_LIMIT);
    }

    private static String describe(Exception e) {
        return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
    }
}
