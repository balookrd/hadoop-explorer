package org.apache.hadoop.explorer.hdfs;

import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class HdfsIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void fullHdfsWorkflowTest() throws Exception {
        // 1. Проверка Health
        mockMvc.perform(get("/health"))
            .andExpect(status().isOk())
            .andExpect(jsonResp -> {
                // simple check
            })
            .andExpect(jsonPath("$.status", is("healthy")))
            .andExpect(jsonPath("$.service", is("hadoop-hdfs-explorer")));

        // 2. Вход под учетной записью администратора (Mock auth)
        MvcResult loginResult = mockMvc.perform(post("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Requested-With", "XMLHttpRequest")
                .content("{\"username\":\"admin\",\"password\":\"secret\"}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.user.username", is("admin")))
            .andReturn();

        Cookie authCookie = loginResult.getResponse().getCookie("access_token");
        org.junit.jupiter.api.Assertions.assertNotNull(authCookie, "Auth cookie must not be null");

        // 3. Получение списка кластеров
        mockMvc.perform(get("/api/v1/clusters")
                .cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(2))))
            .andExpect(jsonPath("$[0].id", notNullValue()))
            .andExpect(jsonPath("$[0].name", notNullValue()));

        // 4. Листинг корневой директории dev-cluster
        mockMvc.perform(get("/api/v1/clusters/dev-cluster/files")
                .param("path", "/")
                .cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.cluster_id", is("dev-cluster")))
            .andExpect(jsonPath("$.path", is("/")))
            .andExpect(jsonPath("$.files", hasSize(greaterThanOrEqualTo(1))))
            .andExpect(jsonPath("$.can_write", is(true)));

        // 5. Создание директории
        mockMvc.perform(post("/api/v1/clusters/dev-cluster/files/mkdir")
                .param("path", "/test_suite_dir")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success", is(true)))
            .andExpect(jsonPath("$.path", is("/test_suite_dir")));

        // 6. Загрузка файла
        MockMultipartFile file = new MockMultipartFile(
            "file",
            "hello.txt",
            MediaType.TEXT_PLAIN_VALUE,
            "Apache Hadoop HDFS Java 21 integration test content!".getBytes(StandardCharsets.UTF_8)
        );

        mockMvc.perform(multipart("/api/v1/clusters/dev-cluster/files/upload", "dev-cluster")
                .file(file)
                .param("path", "/test_suite_dir")
                .param("overwrite", "true")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success", is(true)))
            .andExpect(jsonPath("$.path", is("/test_suite_dir/hello.txt")));

        // 7. Предпросмотр загруженного файла
        mockMvc.perform(get("/api/v1/clusters/dev-cluster/files/preview")
                .param("path", "/test_suite_dir/hello.txt")
                .cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.cluster_id", is("dev-cluster")))
            .andExpect(jsonPath("$.path", is("/test_suite_dir/hello.txt")))
            .andExpect(jsonPath("$.content", containsString("Apache Hadoop HDFS Java 21")))
            .andExpect(jsonPath("$.truncated", is(false)));

        // 8. Скачивание файла
        mockMvc.perform(get("/api/v1/clusters/dev-cluster/files/download")
                .param("path", "/test_suite_dir/hello.txt")
                .cookie(authCookie))
            .andExpect(status().isOk())
            .andExpect(header().string("Content-Disposition", containsString("hello.txt")))
            .andExpect(content().string(containsString("Apache Hadoop HDFS Java 21")));

        // 9. Переименование файла
        mockMvc.perform(post("/api/v1/clusters/dev-cluster/files/rename")
                .param("src", "/test_suite_dir/hello.txt")
                .param("dst", "/test_suite_dir/renamed.txt")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success", is(true)))
            .andExpect(jsonPath("$.path", is("/test_suite_dir/renamed.txt")));

        // 10. Межкластерное копирование
        String crossCopyJson = """
            {
              "source_cluster_id": "dev-cluster",
              "source_path": "/test_suite_dir/renamed.txt",
              "target_cluster_id": "prod-datalake",
              "target_path": "/data/imported_from_dev.txt",
              "overwrite": true
            }
            """;

        mockMvc.perform(post("/api/v1/clusters/cross-copy")
                .contentType(MediaType.APPLICATION_JSON)
                .content(crossCopyJson)
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success", is(true)))
            .andExpect(jsonPath("$.copied_files", is(1)))
            .andExpect(jsonPath("$.copied_bytes", greaterThan(0)));

        // 11. Пакетное удаление
        String batchDeleteJson = """
            {
              "paths": ["/test_suite_dir/renamed.txt"],
              "recursive": false
            }
            """;

        mockMvc.perform(post("/api/v1/clusters/dev-cluster/files/batch-delete")
                .contentType(MediaType.APPLICATION_JSON)
                .content(batchDeleteJson)
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success", is(true)))
            .andExpect(jsonPath("$.deleted", hasSize(1)));

        // 12. Рекурсивное удаление директории
        mockMvc.perform(delete("/api/v1/clusters/dev-cluster/files/delete")
                .param("path", "/test_suite_dir")
                .param("recursive", "true")
                .cookie(authCookie)
                .header("X-Requested-With", "XMLHttpRequest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success", is(true)));
    }
}
