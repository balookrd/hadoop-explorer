package io.github.balookrd.pristan.parom.orchestrator.service;

import io.github.balookrd.pristan.parom.orchestrator.config.ParomProperties;
import io.github.balookrd.pristan.parom.orchestrator.dto.CreateJobRequest;
import io.github.balookrd.pristan.parom.orchestrator.entity.JobEntity;
import io.github.balookrd.pristan.parom.orchestrator.repository.JobRepository;
import io.github.balookrd.pristan.parom.orchestrator.repository.JobRunRepository;
import io.github.balookrd.pristan.parom.orchestrator.repository.TaskRepository;
import io.github.balookrd.pristan.parom.orchestrator.scheduler.InvalidCronExpressionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobServiceCronTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobRunRepository jobRunRepository;

    @Autowired
    private TaskRepository taskRepository;

    private JobService jobService;

    @BeforeEach
    void setUp() {
        jobService = new JobService(jobRepository, jobRunRepository, taskRepository, new ParomProperties());
    }

    private static CreateJobRequest scheduled(String cron) {
        return new CreateJobRequest("/data/cron/src", "/backup/cron/dst", "dc1", "dc2",
            1024L, "hdfs@REALM.LOCAL", true, true, cron);
    }

    private JobEntity load(String id) {
        return jobRepository.findById(id).orElseThrow();
    }

    @Test
    @DisplayName("Создание задачи с невалидным cron отклоняется")
    void createScheduledJobRejectsInvalidCron() {
        assertThrows(InvalidCronExpressionException.class,
            () -> jobService.createJob(scheduled("every tuesday"), "de_user"));
    }

    @Test
    @DisplayName("Задача по расписанию без cron отклоняется")
    void createScheduledJobRejectsMissingCron() {
        assertThrows(InvalidCronExpressionException.class,
            () -> jobService.createJob(scheduled(null), "de_user"));
    }

    @Test
    @DisplayName("Первый запуск — ближайшее срабатывание выражения, а не момент создания")
    void createScheduledJobPlansFirstRunAtNextCronMatch() {
        Instant before = Instant.now();
        String id = jobService.createJob(scheduled("0 0 * * *"), "de_user").id();

        Instant nextRun = load(id).getNextRunAt();
        assertEquals(LocalTime.MIDNIGHT, nextRun.atZone(ZoneOffset.UTC).toLocalTime());
        assertTrue(nextRun.isAfter(before));
        assertTrue(Duration.between(before, nextRun).compareTo(Duration.ofDays(1)) <= 0);
    }

    @Test
    @DisplayName("Правка с невалидным cron отклоняется и не меняет сохраненное выражение")
    void updateRejectsInvalidCronAndKeepsOldExpression() {
        String id = jobService.createJob(scheduled("@hourly"), "de_user").id();

        assertThrows(InvalidCronExpressionException.class,
            () -> jobService.updateJob(id, scheduled("61 * * * *")));
        assertEquals("@hourly", load(id).getCronExpression());
    }

    @Test
    @DisplayName("Смена выражения пересчитывает следующий запуск")
    void updateChangingCronRecomputesNextRun() {
        String id = jobService.createJob(scheduled("@every_5m"), "de_user").id();

        jobService.updateJob(id, scheduled("0 0 * * *"));

        assertEquals(LocalTime.MIDNIGHT, load(id).getNextRunAt().atZone(ZoneOffset.UTC).toLocalTime());
    }

    @Test
    @DisplayName("Повторное включение расписания, снятого планировщиком, возвращает статус SCHEDULED")
    void reEnablingScheduleDisabledBySchedulerRestoresScheduledStatus() {
        String id = jobService.createJob(scheduled("@hourly"), "de_user").id();
        JobEntity job = load(id);
        job.setScheduled(false);
        job.setStatus("STOPPED");
        job.setMessage("Расписание отключено: неизвестный часовой пояс");
        jobRepository.save(job);

        jobService.updateJob(id, scheduled("@hourly"));

        JobEntity updated = load(id);
        assertTrue(updated.isScheduled());
        assertEquals("SCHEDULED", updated.getStatus());
        assertNull(updated.getMessage());
    }

    @Test
    @DisplayName("Включение расписания у задачи, остановленной оператором, статус не меняет")
    void reEnablingScheduleKeepsOperatorStop() {
        String id = jobService.createJob(scheduled("@hourly"), "de_user").id();
        JobEntity job = load(id);
        job.setScheduled(false);
        job.setStatus("STOPPED");
        job.setMessage("Остановлено оператором");
        jobRepository.save(job);

        jobService.updateJob(id, scheduled("@hourly"));

        JobEntity updated = load(id);
        assertEquals("STOPPED", updated.getStatus());
        assertEquals("Остановлено оператором", updated.getMessage());
    }
}
