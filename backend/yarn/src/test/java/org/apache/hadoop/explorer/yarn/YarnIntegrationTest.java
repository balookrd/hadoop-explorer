package org.apache.hadoop.explorer.yarn;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.yarn.model.ChangeRequestCreate;
import org.apache.hadoop.explorer.yarn.model.DraftValidateRequest;
import org.apache.hadoop.explorer.yarn.model.PartitionResourceConfig;
import org.apache.hadoop.explorer.yarn.model.QueueDraftItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@DisplayName("Интеграционные тесты YARN Explorer API")
class YarnIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private String loginAndGetToken(String username, String password) throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("username", username, "password", password));
        MvcResult result = mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andReturn();

        JsonNode json = objectMapper.readTree(result.getResponse().getContentAsString());
        String token = json.path("access_token").asText();
        if (token.isBlank() && result.getResponse().getCookie("access_token") != null) {
            token = result.getResponse().getCookie("access_token").getValue();
        }
        return token;
    }

    @Test
    @DisplayName("Проверка эндпоинта /health")
    void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ok"));
    }

    @Test
    @DisplayName("Получение списка кластеров и очередей")
    void testClustersAndQueues() throws Exception {
        String token = loginAndGetToken("reader_user", "password123");

        // Список кластеров
        mockMvc.perform(get("/api/v1/clusters")
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(1))))
                .andExpect(jsonPath("$[0].id").value("prod-yarn"))
                .andExpect(jsonPath("$[0].user_role").value("reader"))
                .andExpect(jsonPath("$[0].can_write").value(false))
                .andExpect(jsonPath("$[0].can_admin").value(false));

        // Дерево очередей и метрики
        mockMvc.perform(get("/api/v1/clusters/prod-yarn/queues")
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cluster_id").value("prod-yarn"))
                .andExpect(jsonPath("$.root_queue.name").value("root"))
                .andExpect(jsonPath("$.root_queue.children", hasSize(3)))
                .andExpect(jsonPath("$.cluster_metrics.active_nodes").value(120))
                .andExpect(jsonPath("$.balances", hasSize(greaterThanOrEqualTo(1))));
    }

    @Test
    @DisplayName("Валидация черновика очередей (validate и diff)")
    void testValidateAndDiff() throws Exception {
        String token = loginAndGetToken("writer_user", "password123");

        DraftValidateRequest req = DraftValidateRequest.builder()
                .clusterId("prod-yarn")
                .selectedPartition("DEFAULT")
                .queues(List.of(
                        QueueDraftItem.builder()
                                .name("dev")
                                .path("root.dev")
                                .parentPath("root")
                                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(20.0).maxCapacity(50.0).build()))
                                .build(),
                        QueueDraftItem.builder()
                                .name("default")
                                .path("root.default")
                                .parentPath("root")
                                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(20.0).maxCapacity(100.0).build()))
                                .build()
                ))
                .build();

        // Валидация
        mockMvc.perform(post("/api/v1/clusters/prod-yarn/validate")
                        .header("Authorization", "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.is_valid").value(true))
                .andExpect(jsonPath("$.errors", hasSize(0)));

        // Diff
        mockMvc.perform(post("/api/v1/clusters/prod-yarn/diff")
                        .header("Authorization", "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cluster_id").value("prod-yarn"))
                .andExpect(jsonPath("$.has_changes").value(true));
    }

    @Test
    @DisplayName("Полный жизненный цикл Change Request: создание, Four-Eyes проверка, согласование, деплой")
    void testChangeRequestFullLifecycle() throws Exception {
        String writerToken = loginAndGetToken("writer_user", "password123");
        String adminToken = loginAndGetToken("admin_user", "password123");

        // 1. Создание заявки от writer_user
        ChangeRequestCreate crCreate = ChangeRequestCreate.builder()
                .clusterId("prod-yarn")
                .title("Increase spark queue capacity")
                .description("Production batch workload scaling")
                .changes(List.of(
                        QueueDraftItem.builder()
                                .name("spark")
                                .path("root.prod.spark")
                                .parentPath("root.prod")
                                .action("modify")
                                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(50.0).maxCapacity(80.0).build()))
                                .build(),
                        QueueDraftItem.builder()
                                .name("flink")
                                .path("root.prod.flink")
                                .parentPath("root.prod")
                                .action("modify")
                                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(25.0).maxCapacity(50.0).build()))
                                .build()
                ))
                .build();

        MvcResult createResult = mockMvc.perform(post("/api/v1/change-requests")
                        .header("Authorization", "Bearer " + writerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(crCreate)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.status").value("SUBMITTED"))
                .andExpect(jsonPath("$.author").value("writer_user"))
                .andReturn();

        JsonNode createdCr = objectMapper.readTree(createResult.getResponse().getContentAsString());
        long crId = createdCr.path("id").asLong();

        // 2. Список и счетчик
        mockMvc.perform(get("/api/v1/change-requests/pending-count")
                        .header("Authorization", "Bearer " + writerToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count", greaterThanOrEqualTo(1)))
                .andExpect(jsonPath("$.pending_count", greaterThanOrEqualTo(1)));

        // 3. Four-Eyes принцип: если writer попытается сам себя одобрить (или если author==reviewer)
        // Для демонстрации: попробуем одобрить через автора (writer_user)
        mockMvc.perform(post("/api/v1/change-requests/" + crId + "/approve")
                        .header("Authorization", "Bearer " + writerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"comment\": \"Self-approve attempt\"}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.detail", containsString("ADMIN")));

        // 4. Одобрение администратором admin_user
        mockMvc.perform(post("/api/v1/change-requests/" + crId + "/approve")
                        .header("Authorization", "Bearer " + adminToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"comment\": \"Approved after capacity review\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.reviewer").value("admin_user"))
                .andExpect(jsonPath("$.xml_content").exists());

        // 5. Предпросмотр XML (проверка обоих путей: /xml и /preview-xml)
        mockMvc.perform(get("/api/v1/change-requests/" + crId + "/xml")
                        .header("Authorization", "Bearer " + adminToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.xml_content", containsString("<capacity-scheduler-patch")))
                .andExpect(jsonPath("$.operations[?(@.name == 'yarn.scheduler.capacity.root.prod.spark.capacity')].value").value("50"));

        mockMvc.perform(get("/api/v1/change-requests/" + crId + "/preview-xml")
                        .header("Authorization", "Bearer " + adminToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.xml_content", containsString("<capacity-scheduler-patch")));

        // 6. Развертывание заявки через AWX
        mockMvc.perform(post("/api/v1/change-requests/" + crId + "/deploy")
                        .header("Authorization", "Bearer " + adminToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.awx_job_id").exists())
                .andExpect(jsonPath("$.stdout", containsString("SIMULATION")));

        // 6a. Повторный деплой той же заявки не запускает второй AWX Job
        mockMvc.perform(post("/api/v1/change-requests/" + crId + "/deploy")
                        .header("Authorization", "Bearer " + adminToken))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.detail", containsString("уже развертывается или успешно развернута")));

        // 7. Проверка статуса деплоя через /deploy-status
        mockMvc.perform(get("/api/v1/change-requests/" + crId + "/deploy-status")
                        .header("Authorization", "Bearer " + adminToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.awx_job_id").exists())
                .andExpect(jsonPath("$.stdout").exists());
    }

    private QueueDraftItem draft(String path, double capacity, double maxCapacity) {
        return QueueDraftItem.builder()
                .name(path.substring(path.lastIndexOf('.') + 1))
                .path(path)
                .parentPath(path.substring(0, path.lastIndexOf('.')))
                .action("modify")
                .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(capacity).maxCapacity(maxCapacity).build()))
                .build();
    }

    @Test
    @DisplayName("Заявка, которую RM не примет, отклоняется при создании с объяснением причины")
    void testInvalidChangeRequestsAreRejected() throws Exception {
        String writerToken = loginAndGetToken("writer_user", "password123");

        // Сумма дочерних очередей root становится 115%
        ChangeRequestCreate unbalanced = ChangeRequestCreate.builder()
                .clusterId("prod-yarn")
                .title("Unbalanced change")
                .changes(List.of(draft("root.dev", 40.0, 50.0)))
                .build();
        mockMvc.perform(post("/api/v1/change-requests")
                        .header("Authorization", "Bearer " + writerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(unbalanced)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail", containsString("должна быть 100%")));

        // Новая очередь под несуществующим родителем
        ChangeRequestCreate orphan = ChangeRequestCreate.builder()
                .clusterId("prod-yarn")
                .title("Orphan queue")
                .changes(List.of(QueueDraftItem.builder()
                        .name("q").path("root.nope.q").parentPath("root.nope").action("create")
                        .partitions(Map.of("DEFAULT", PartitionResourceConfig.builder().capacity(100.0).maxCapacity(100.0).build()))
                        .build()))
                .build();
        mockMvc.perform(post("/api/v1/change-requests")
                        .header("Authorization", "Bearer " + writerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(orphan)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail", containsString("root.nope")));

        // Удаление работающей очереди
        ChangeRequestCreate deleteRunning = ChangeRequestCreate.builder()
                .clusterId("prod-yarn")
                .title("Delete running queue")
                .changes(List.of(
                        QueueDraftItem.builder().name("dev").path("root.dev").parentPath("root").action("delete").build(),
                        draft("root.default", 40.0, 100.0)))
                .build();
        mockMvc.perform(post("/api/v1/change-requests")
                        .header("Authorization", "Bearer " + writerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(deleteRunning)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail", containsString("STOPPED")));

        // Фильтр по неизвестному статусу не возвращает молча все заявки
        mockMvc.perform(get("/api/v1/change-requests?status=BOGUS")
                        .header("Authorization", "Bearer " + writerToken))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Прямой деплой XML в обход заявок запрещен при включенном Four-Eyes")
    void testDirectDeployForbiddenWithFourEyes() throws Exception {
        String adminToken = loginAndGetToken("admin_user", "password123");
        mockMvc.perform(post("/api/v1/clusters/prod-yarn/deploy-xml")
                        .header("Authorization", "Bearer " + adminToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"xml_content\": \"<configuration/>\"}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.detail", containsString("enforce-four-eyes")));
    }
}
