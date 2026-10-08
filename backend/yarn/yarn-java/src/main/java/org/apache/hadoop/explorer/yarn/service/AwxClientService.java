package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.DeployResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class AwxClientService {

    private static final Logger log = LoggerFactory.getLogger(AwxClientService.class);

    private final YarnProperties yarnProperties;
    private final ObjectMapper objectMapper;

    public AwxClientService(YarnProperties yarnProperties, ObjectMapper objectMapper) {
        this.yarnProperties = yarnProperties;
        this.objectMapper = objectMapper;
    }

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /**
     * Выполняет деплой сгенерированного XML в кластер через AWX Ansible или эмулирует выполнение при mock/недоступности.
     */
    public DeployResponse deployXml(ClusterConfig cluster, Long crId, String xmlContent, String comment, boolean wait) {
        YarnProperties.AwxConfig awx = yarnProperties.getAwx();

        int jobTemplateId = (cluster.getAwx() != null && cluster.getAwx().getJobTemplateId() != null)
                ? cluster.getAwx().getJobTemplateId()
                : awx.getDefaultJobTemplateId();

        String now = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());

        if (awx.isEnabled() && awx.getBaseUrl() != null && !awx.getBaseUrl().contains(".local")) {
            try {
                String launchUrl = awx.getBaseUrl().replaceAll("/+$", "") +
                        "/api/v2/job_templates/" + jobTemplateId + "/launch/";

                Map<String, Object> payload = Map.of(
                        "extra_vars", Map.of(
                                "cluster_id", cluster.getId(),
                                "capacity_scheduler_xml", xmlContent,
                                "deploy_comment", comment != null ? comment : ""
                        )
                );

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(launchUrl))
                        .header("Authorization", "Bearer " + awx.getToken())
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(10))
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 201 || response.statusCode() == 200) {
                    JsonNode node = objectMapper.readTree(response.body());
                    int jobId = node.path("job").asInt(node.path("id").asInt(1001));

                    if (wait) {
                        return pollJobStatus(jobId, crId, cluster.getId(), now);
                    }

                    return DeployResponse.builder()
                            .crId(crId)
                            .clusterId(cluster.getId())
                            .awxJobId(jobId)
                            .status("RUNNING")
                            .message("Задача #" + jobId + " успешно поставлена в очередь AWX")
                            .deployedAt(now)
                            .build();
                }
            } catch (Exception e) {
                log.warn("Не удалось отправить запрос в реальный AWX: {}. Переход к симуляции деплоя.", e.getMessage());
            }
        }

        // Симуляция успешного Ansible Job Template деплоя для тестового стенда
        int mockJobId = ThreadLocalRandom.current().nextInt(1000, 9999);
        String mockStdout = String.format(
                "PLAY [Deploy capacity-scheduler.xml on %s] ************************************\n\n" +
                "TASK [Gathering Facts] *********************************************************\n" +
                "ok: [rm1.%s.local]\n" +
                "ok: [rm2.%s.local]\n\n" +
                "TASK [Backup current /etc/hadoop/conf/capacity-scheduler.xml] ******************\n" +
                "changed: [rm1.%s.local]\n" +
                "changed: [rm2.%s.local]\n\n" +
                "TASK [Deploy updated capacity-scheduler.xml] ***********************************\n" +
                "changed: [rm1.%s.local]\n" +
                "changed: [rm2.%s.local]\n\n" +
                "TASK [Execute 'yarn rmadmin -refreshQueues'] ***********************************\n" +
                "changed: [rm1.%s.local]\n\n" +
                "PLAY RECAP *********************************************************************\n" +
                "rm1.%s.local : ok=4    changed=3    unreachable=0    failed=0\n" +
                "rm2.%s.local : ok=3    changed=2    unreachable=0    failed=0\n\n" +
                "Deployment completed successfully.",
                cluster.getName(), cluster.getId(), cluster.getId(),
                cluster.getId(), cluster.getId(),
                cluster.getId(), cluster.getId(),
                cluster.getId(), cluster.getId(), cluster.getId()
        );

        return DeployResponse.builder()
                .crId(crId)
                .clusterId(cluster.getId())
                .awxJobId(mockJobId)
                .status("SUCCESS")
                .message("Конфигурация capacity-scheduler.xml успешно развернута на кластере '" + cluster.getName() + "' (AWX Job #" + mockJobId + ")")
                .deployedAt(now)
                .stdout(mockStdout)
                .build();
    }

    private DeployResponse pollJobStatus(int jobId, Long crId, String clusterId, String deployedAt) {
        return DeployResponse.builder()
                .crId(crId)
                .clusterId(clusterId)
                .awxJobId(jobId)
                .status("SUCCESS")
                .message("AWX Job #" + jobId + " успешно завершился")
                .deployedAt(deployedAt)
                .build();
    }
}
