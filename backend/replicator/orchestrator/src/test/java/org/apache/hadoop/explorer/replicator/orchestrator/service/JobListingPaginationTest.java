package org.apache.hadoop.explorer.replicator.orchestrator.service;

import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.CapturingStatementInspector;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Выборка списка заданий страницами.
 *
 * <p>Список заданий читался целиком, а фильтры по владельцу и типу применялись в памяти. На
 * пофайловых подзадачах метастора таких записей десятки тысяч, и каждый запрос списка (консоль,
 * опрос агента) тянул их все в heap оркестратора.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Задания: список отдается страницами")
class JobListingPaginationTest {

    private static final int TOTAL_JOBS = 1200;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @BeforeEach
    void setUp() {
        jobRepository.deleteAll();
        List<JobEntity> jobs = new ArrayList<>(TOTAL_JOBS);
        for (int i = 0; i < TOTAL_JOBS; i++) {
            jobs.add(job("job-page-" + i, "alice", null, "STANDARD", "RUNNING"));
        }
        jobRepository.saveAll(jobs);
    }

    private JobEntity job(String id, String createdBy, String principal, String type, String status) {
        JobEntity job = new JobEntity();
        job.setId(id);
        job.setSourceClusterId("prod-a");
        job.setTargetClusterId("prod-b");
        job.setSourcePath("/warehouse/" + id);
        job.setTargetPath("/backup/warehouse/" + id);
        job.setStatus(status);
        job.setCreatedBy(createdBy);
        job.setExecutionPrincipal(principal != null ? principal : createdBy + "@REALM.LOCAL");
        job.setJobType(type);
        return job;
    }

    @Test
    @DisplayName("Страница отдает запрошенный размер и общее количество")
    void pageShouldReportSizeAndTotal() {
        Page<JobResponse> page = jobService.listJobs(null, "admin", true, false, false, PageRequest.of(0, 50));

        assertEquals(50, page.getContent().size(), "Страница должна содержать запрошенное число записей");
        assertEquals(TOTAL_JOBS, page.getTotalElements(), "Общее количество должно быть известно вызывающему");
        assertEquals(24, page.getTotalPages());
    }

    @Test
    @DisplayName("Выборка страницы не читает таблицу заданий целиком")
    void pagedListingShouldNotScanWholeTable() {
        CapturingStatementInspector.start();
        Page<JobResponse> page = jobService.listJobs(null, "admin", true, false, false, PageRequest.of(0, 50));
        List<String> sql = CapturingStatementInspector.stop();

        assertEquals(50, page.getContent().size());
        assertEquals(0, CapturingStatementInspector.countFullScans(sql, "replication_jobs"),
                "Список заданий не должен вычитывать таблицу целиком. SQL: " + sql);
    }

    @Test
    @DisplayName("Фильтр владельца на странице совпадает с правилом matchesJobOwner")
    void ownerFilterShouldMatchDomainRule() {
        jobRepository.deleteAll();
        List<JobEntity> fixture = List.of(
                job("own-created-by-short", "alice", null, "STANDARD", "RUNNING"),
                job("own-created-by-realm", "alice@REALM.LOCAL", null, "STANDARD", "RUNNING"),
                job("own-created-by-case", "ALICE", null, "STANDARD", "RUNNING"),
                job("own-by-principal", "bob", "alice@REALM.LOCAL", "STANDARD", "RUNNING"),
                job("own-system", "system", null, "STANDARD", "RUNNING"),
                job("own-blank", "", null, "STANDARD", "RUNNING"),
                job("own-foreign", "bob", "bob@REALM.LOCAL", "STANDARD", "RUNNING"),
                job("own-foreign-principal", "carol", "dave@REALM.LOCAL", "STANDARD", "RUNNING")
        );
        jobRepository.saveAll(fixture);

        List<String> expected = fixture.stream()
                .filter(j -> JobService.matchesJobOwner(j, "alice@REALM.LOCAL"))
                .map(JobEntity::getId)
                .sorted()
                .toList();

        List<String> actual = jobService.listJobs(null, "alice@REALM.LOCAL", false, false, false,
                        PageRequest.of(0, 100))
                .getContent().stream()
                .map(JobResponse::id)
                .sorted()
                .toList();

        assertEquals(expected, actual,
                "Запрос страницы должен отбирать ровно те задания, что и правило в коде");
    }

    @Test
    @DisplayName("Служебные подзадачи метастора в общий список не попадают")
    void subjobsShouldStayOutOfTheListing() {
        jobRepository.deleteAll();
        jobRepository.saveAll(List.of(
                job("listing-standard", "alice", null, "STANDARD", "RUNNING"),
                job("listing-hms-subjob", "system", null, "HMS_SUBJOB", "RUNNING"),
                job("listing-hms-streaming", "system", null, "HMS_COMPANION_STREAMING", "STREAMING")
        ));

        List<String> regular = jobService.listJobs(null, "admin", true, false, false, PageRequest.of(0, 50))
                .getContent().stream().map(JobResponse::id).sorted().toList();
        List<String> withSubjobs = jobService.listJobs(null, "admin", true, false, true, PageRequest.of(0, 50))
                .getContent().stream().map(JobResponse::id).sorted().toList();

        assertEquals(List.of("listing-standard"), regular, "В регламентном списке только обычные задания");
        assertEquals(3, withSubjobs.size(), "С явным запросом подзадачи видны");
    }

    @Test
    @DisplayName("Фильтр по статусу применяется запросом")
    void statusFilterShouldBeApplied() {
        jobRepository.deleteAll();
        jobRepository.saveAll(List.of(
                job("status-running", "alice", null, "STANDARD", "RUNNING"),
                job("status-failed", "alice", null, "STANDARD", "FAILED")
        ));

        Page<JobResponse> failed = jobService.listJobs("failed", "admin", true, false, false, PageRequest.of(0, 50));

        assertEquals(1, failed.getTotalElements());
        assertEquals("status-failed", failed.getContent().get(0).id());
    }
}
