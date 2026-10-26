package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.core.Authentication;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Фильтры списка заданий метастора применяются к набору, а не к загруженной странице.
 *
 * <p>Отбор в браузере работал ровно до первой сотни записей: нужное задание просто не попадало
 * на загруженную страницу, и поиск отвечал «ничего не найдено», хотя задание существовало.
 * Счетчики при этом считались по той же странице.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("HMS: отбор и счетчики считаются по всем заданиям")
class HmsJobListingFiltersTest {

    @Autowired
    private HmsReplicationJobRepository jobRepository;

    @Autowired
    private HmsReplicationController controller;

    private final Authentication admin = new CommonAuthenticationToken(new UserSession(
            "admin_user", "Admin", "admin@corp.org", List.of("admins"), "KERBEROS", true, Role.ADMIN));

    @BeforeEach
    void setUp() {
        jobRepository.deleteAll();
        // Активных заданий больше страницы, и нужное лежит в самом конце набора
        for (int i = 0; i < 120; i++) {
            jobRepository.save(job("hms-active-" + i, "ACTIVE", "sales_" + i, "alice"));
        }
        jobRepository.save(job("hms-paused-1", "PAUSED", "finance_archive", "bob"));
        jobRepository.save(job("hms-error-1", "ERROR", "logs_raw", "bob"));
    }

    private HmsReplicationJobEntity job(String id, String status, String sourceDb, String owner) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(id);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName(sourceDb);
        job.setTargetDbName(sourceDb + "_replica");
        job.setStatus(status);
        job.setCreatedBy(owner);
        job.setExecutionPrincipal(owner + "@COMPANY.LOCAL");
        job.setCreatedAt(Instant.now());
        return job;
    }

    @Test
    @DisplayName("Фильтр по статусу отбирает из всего набора, а не из первой страницы")
    void statusFilterSelectsFromTheWholeSet() {
        var page = controller.listJobs("PAUSED", null, null, 0, 50, admin).getBody();

        assertNotNull(page);
        assertEquals(1, page.getTotalElements(), "задание на паузе одно на весь набор");
        assertEquals("hms-paused-1", page.getContent().get(0).getId());
    }

    @Test
    @DisplayName("Поиск находит задание, которое не попало на первую страницу")
    void searchReachesBeyondTheFirstPage() {
        var page = controller.listJobs(null, "logs_raw", null, 0, 50, admin).getBody();

        assertNotNull(page);
        assertEquals(1, page.getTotalElements(), "поиск идет по базе, а не по загруженной сотне");
        assertEquals("hms-error-1", page.getContent().get(0).getId());
    }

    @Test
    @DisplayName("Фильтр по автору отбирает задания одного владельца")
    void authorFilterSelectsOneOwner() {
        var page = controller.listJobs(null, null, "bob", 0, 50, admin).getBody();

        assertNotNull(page);
        assertEquals(2, page.getTotalElements(), "у bob два задания");
        assertTrue(page.getContent().stream().allMatch(j -> "bob".equals(j.getCreatedBy())));
    }

    @Test
    @DisplayName("Сводка считает весь набор и учитывает активный поиск")
    void summaryFollowsTheSameFilters() {
        var all = controller.jobsSummary(null, null, admin).getBody();
        assertNotNull(all);
        assertEquals(122, all.total(), "в сводке весь набор");
        assertEquals(120L, all.byStatus().get("ACTIVE"));
        assertTrue(all.authors().containsAll(List.of("alice", "bob")),
                "авторы берутся из базы, а не из страницы: " + all.authors());

        var filtered = controller.jobsSummary("finance", null, admin).getBody();
        assertNotNull(filtered);
        assertEquals(1, filtered.total(), "сводка отбирает по тому же условию, что и страница");
    }

    @Test
    @DisplayName("Без параметров страница ограничена сотней заданий")
    void defaultPageIsCapped() {
        var page = controller.listJobs(null, null, null, null, null, admin).getBody();

        assertNotNull(page);
        assertEquals(122, page.getTotalElements(), "в наборе все задания");
        assertEquals(100, page.getContent().size(), "а на странице — сотня");
    }

    @Test
    @DisplayName("Размер страницы ограничен сверху")
    void pageSizeIsCapped() {
        var page = controller.listJobs(null, null, null, 0, 100_000, admin).getBody();

        assertNotNull(page);
        assertEquals(1000, page.getSize(), "размер страницы не задается клиентом без предела");
    }

    @Test
    @DisplayName("Непривилегированный пользователь не видит чужих заданий ни в списке, ни в сводке")
    void strangerSeesOnlyHisOwn() {
        Authentication bob = new CommonAuthenticationToken(new UserSession(
                "bob", "Bob", "bob@corp.org", List.of("engineers"), "LDAP", false, Role.WRITER));

        var page = controller.listJobs(null, null, null, 0, 50, bob).getBody();
        assertNotNull(page);
        assertEquals(2, page.getTotalElements(), "у bob два задания");

        var summary = controller.jobsSummary(null, null, bob).getBody();
        assertNotNull(summary);
        assertEquals(2, summary.total(), "сводка считает то же, что видно в списке");
        assertEquals(List.of("bob"), summary.authors(), "чужие авторы в фильтре не показываются");
    }
}
