package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobListingSummary;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.CapturingStatementInspector;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Фильтры и сводка списка заданий на стороне сервера.
 *
 * <p>Пока список отдавался целиком, интерфейс фильтровал и считал у себя. Со страничной
 * выборкой так нельзя: фильтр по странице отбирает из случайной сотни записей, а счетчики по
 * статусам описывают не задания, а текущую страницу. Поэтому отбор и сводка считаются запросом
 * по всему набору.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Задания: фильтры и сводка считаются запросом")
class JobListingFiltersTest {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @BeforeEach
    void setUp() {
        jobRepository.deleteAll();
        jobRepository.saveAll(List.of(
                job("flt-running", "alice", "RUNNING", "MANUAL", "/warehouse/sales"),
                job("flt-queued", "alice", "QUEUED", "MANUAL", "/warehouse/orders"),
                job("flt-failed", "bob", "FAILED", "MANUAL", "/warehouse/sales_archive"),
                job("flt-completed", "bob", "COMPLETED", "MANUAL", "/warehouse/logs"),
                job("flt-streaming-status", "carol", "STREAMING", "MANUAL", "/warehouse/events"),
                job("flt-streaming-mode", "carol", "RUNNING", "STREAMING_INOTIFY", "/warehouse/cdc")
        ));
    }

    private JobEntity job(String id, String createdBy, String status, String syncMode, String sourcePath) {
        JobEntity job = new JobEntity();
        job.setId(id);
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourcePath(sourcePath);
        job.setTargetPath("/backup" + sourcePath);
        job.setStatus(status);
        job.setSyncMode(syncMode);
        job.setCreatedBy(createdBy);
        job.setExecutionPrincipal(createdBy + "@REALM.LOCAL");
        job.setJobType("STANDARD");
        return job;
    }

    private List<String> ids(String status, String author, String search) {
        return jobService.listJobs(status, author, search, "admin", true, false, false, PageRequest.of(0, 50))
                .getContent().stream().map(JobResponse::id).sorted().toList();
    }

    @Test
    @DisplayName("Фильтр STREAMING берет и статус, и потоковый режим — как в интерфейсе")
    void streamingFilterShouldCoverSyncMode() {
        assertEquals(List.of("flt-streaming-mode", "flt-streaming-status"), ids("STREAMING", null, null),
                "Потоковое задание в статусе RUNNING тоже относится к стримингу");
    }

    @Test
    @DisplayName("Фильтр по автору отбирает запросом")
    void authorFilterShouldBeApplied() {
        assertEquals(List.of("flt-completed", "flt-failed"), ids(null, "bob", null));
    }

    @Test
    @DisplayName("Поиск идет по идентификатору, путям, кластерам, автору и принципалу")
    void searchShouldCoverTheSameFieldsAsTheUi() {
        assertEquals(List.of("flt-failed", "flt-running"), ids(null, null, "sales"),
                "Поиск по части пути должен находить оба задания");
        assertEquals(List.of("flt-queued"), ids(null, null, "FLT-QUEUED"),
                "Поиск регистронезависим и работает по идентификатору");
        assertEquals(List.of("flt-streaming-mode", "flt-streaming-status"), ids(null, null, "carol"),
                "Поиск должен находить по автору");
        assertEquals(6, ids(null, null, "prod-a").size(),
                "Поиск должен находить по идентификатору кластера");
    }

    @Test
    @DisplayName("Фильтры складываются")
    void filtersShouldCombine() {
        assertEquals(List.of("flt-failed"), ids("FAILED", "bob", "sales"));
    }

    @Test
    @DisplayName("Сводка по статусам считается по всему набору, а не по странице")
    void summaryShouldCountWholeFilteredSet() {
        JobListingSummary summary = jobService.summarizeJobs(null, null, "admin", true, false, false);

        assertEquals(6, summary.total());
        assertEquals(2, summary.running(), "RUNNING: обычное и потоковое задание в статусе RUNNING");
        assertEquals(1, summary.queued());
        assertEquals(1, summary.failed());
        assertEquals(1, summary.completed());
        assertEquals(2, summary.streaming(), "Стриминг — по статусу и по режиму синхронизации");
    }

    @Test
    @DisplayName("Сводка учитывает фильтры автора и поиска")
    void summaryShouldRespectFilters() {
        JobListingSummary summary = jobService.summarizeJobs("bob", null, "admin", true, false, false);

        assertEquals(2, summary.total());
        assertEquals(1, summary.failed());
        assertEquals(1, summary.completed());
        assertEquals(0, summary.running());
    }

    @Test
    @DisplayName("Сводка не вычитывает таблицу заданий целиком")
    void summaryShouldNotScanWholeTable() {
        CapturingStatementInspector.start();
        jobService.summarizeJobs(null, null, "admin", true, false, false);
        List<String> sql = CapturingStatementInspector.stop();

        assertEquals(0, CapturingStatementInspector.countFullScans(sql, "replication_jobs"),
                "Сводка должна считаться агрегатом. SQL: " + sql);
    }

    @Test
    @DisplayName("Список авторов для фильтра берется из базы, а не из загруженной страницы")
    void authorsShouldComeFromTheDatabase() {
        assertEquals(List.of("alice", "bob", "carol"),
                jobService.listJobAuthors("admin", true, false),
                "Выпадающий список авторов должен охватывать все задания");
    }
}
