package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class HmsReplicationControllerTest {

    private HmsReplicationJobRepository jobRepository;
    private HmsEventLogRepository eventLogRepository;
    private HmsCoordinatorService coordinatorService;
    private JobService jobService;
    private HmsReplicationController controller;

    private CommonAuthenticationToken adminAuth;
    private CommonAuthenticationToken ownerAuth;
    private CommonAuthenticationToken strangerAuth;

    @BeforeEach
    void setUp() {
        jobRepository = mock(HmsReplicationJobRepository.class);
        eventLogRepository = mock(HmsEventLogRepository.class);
        coordinatorService = mock(HmsCoordinatorService.class);
        jobService = mock(JobService.class);

        controller = new HmsReplicationController(jobRepository, eventLogRepository, coordinatorService, jobService);

        UserSession adminSession = new UserSession(
                "admin_user", "Admin", "admin@corp.org",
                List.of("admins"), "KERBEROS", true, Role.ADMIN
        );
        adminAuth = new CommonAuthenticationToken(adminSession);

        UserSession ownerSession = new UserSession(
                "alice", "Alice Engineer", "alice@corp.org",
                List.of("engineers"), "LDAP", false, Role.WRITER
        );
        ownerAuth = new CommonAuthenticationToken(ownerSession);

        UserSession strangerSession = new UserSession(
                "bob", "Bob Competitor", "bob@corp.org",
                List.of("engineers"), "LDAP", false, Role.WRITER
        );
        strangerAuth = new CommonAuthenticationToken(strangerSession);
    }

    private HmsReplicationJobEntity createJobEntity(String id, String owner) {
        HmsReplicationJobEntity job = new HmsReplicationJobEntity();
        job.setId(id);
        job.setSourceClusterId("dc1");
        job.setTargetClusterId("dc2");
        job.setSourceDbName("analytics");
        job.setTargetDbName("analytics_replica");
        job.setStatus("ACTIVE");
        job.setCreatedBy(owner);
        job.setCreatedAt(Instant.now());
        job.setUpdatedAt(Instant.now());
        return job;
    }

    @Test
    @DisplayName("IDOR защита: чужой пользователь не может поставить задачу на паузу (FORBIDDEN)")
    void strangerCannotPauseJob() {
        HmsReplicationJobEntity job = createJobEntity("job-100", "alice");
        when(jobRepository.findById("job-100")).thenReturn(Optional.of(job));

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                controller.pauseJob("job-100", strangerAuth)
        );

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        verify(jobRepository, never()).save(any());
    }

    @Test
    @DisplayName("IDOR защита: чужой пользователь не может удалить задачу (FORBIDDEN)")
    void strangerCannotDeleteJob() {
        HmsReplicationJobEntity job = createJobEntity("job-100", "alice");
        when(jobRepository.findById("job-100")).thenReturn(Optional.of(job));

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                controller.deleteJob("job-100", strangerAuth)
        );

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        verify(coordinatorService, never()).deleteReplicationJob(anyString());
    }

    @Test
    @DisplayName("Владелец задачи может успешно ставить её на паузу")
    void ownerCanPauseJob() {
        HmsReplicationJobEntity job = createJobEntity("job-100", "alice");
        when(jobRepository.findById("job-100")).thenReturn(Optional.of(job));

        ResponseEntity<HmsReplicationJobEntity> resp = controller.pauseJob("job-100", ownerAuth);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("PAUSED", resp.getBody().getStatus());
        verify(jobRepository, times(1)).save(job);
    }

    @Test
    @DisplayName("Администратор может управлять чужой задачей")
    void adminCanPauseAnyJob() {
        HmsReplicationJobEntity job = createJobEntity("job-100", "alice");
        when(jobRepository.findById("job-100")).thenReturn(Optional.of(job));

        ResponseEntity<HmsReplicationJobEntity> resp = controller.pauseJob("job-100", adminAuth);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("PAUSED", resp.getBody().getStatus());
        verify(jobRepository, times(1)).save(job);
    }

    @Test
    @DisplayName("Несуществующая задача возвращает 404 NOT_FOUND")
    void nonExistentJobReturnsNotFound() {
        when(jobRepository.findById("missing-job")).thenReturn(Optional.empty());

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                controller.pauseJob("missing-job", adminAuth)
        );

        assertEquals(HttpStatus.NOT_FOUND, ex.getStatusCode());
    }

    @Test
    @DisplayName("Владелец задачи может успешно удалить задачу")
    void ownerCanDeleteJob() {
        HmsReplicationJobEntity job = createJobEntity("job-100", "alice");
        when(jobRepository.findById("job-100")).thenReturn(Optional.of(job));
        when(coordinatorService.deleteReplicationJob("job-100")).thenReturn(true);

        ResponseEntity<Map<String, Object>> resp = controller.deleteJob("job-100", ownerAuth);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(coordinatorService, times(1)).deleteReplicationJob("job-100");
    }

    @Test
    @DisplayName("Аутентификация /progress: неаутентифицированный вызов отклоняется 401 UNAUTHORIZED")
    void unauthenticatedCannotUpdateProgress() {
        var report = new org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest(
                "ACTIVE", 1, 1, 10, 10, 100L, null, 0L, "ok", List.of(), "agent-1"
        );
        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                controller.updateProgress("job-100", report, null)
        );
        assertEquals(HttpStatus.UNAUTHORIZED, ex.getStatusCode());
        verify(coordinatorService, never()).updateJobProgress(anyString(), any());
    }

    @Test
    @DisplayName("Аутентификация /progress: авторизованный агент успешно передает прогресс")
    void authenticatedCanUpdateProgress() {
        var report = new org.apache.hadoop.explorer.replicator.model.HmsProgressReportRequest(
                "ACTIVE", 1, 1, 10, 10, 100L, null, 0L, "ok", List.of(), "agent-1"
        );
        ResponseEntity<Map<String, Object>> resp = controller.updateProgress("job-100", report, ownerAuth);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(coordinatorService, times(1)).updateJobProgress("job-100", report);
    }

    @Test
    @DisplayName("Аутентификация /pending: неаутентифицированный вызов отклоняется 401 UNAUTHORIZED")
    void unauthenticatedCannotGetPendingJobs() {
        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                controller.getPendingJobs("dc1", "agent-1", null)
        );
        assertEquals(HttpStatus.UNAUTHORIZED, ex.getStatusCode());
        verify(coordinatorService, never()).getPendingJobsForCluster(anyString(), anyString());
    }

    @Test
    @DisplayName("Аутентификация /pending: авторизованный запрос возвращает список задач")
    void authenticatedCanGetPendingJobs() {
        ResponseEntity<?> resp = controller.getPendingJobs("dc1", "agent-1", ownerAuth);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(coordinatorService, times(1)).getPendingJobsForCluster("dc1", "agent-1");
    }
}
