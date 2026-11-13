package io.github.balookrd.pristan.parom.orchestrator.controller;

import io.github.balookrd.pristan.zastava.model.Role;
import io.github.balookrd.pristan.zastava.model.UserSession;
import io.github.balookrd.pristan.zastava.security.CommonAuthenticationToken;
import io.github.balookrd.pristan.parom.orchestrator.dto.CreateJobRequest;
import io.github.balookrd.pristan.parom.orchestrator.entity.JobEntity;
import io.github.balookrd.pristan.parom.orchestrator.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The REST controller rebuilds the request to pin the execution principal and used a constructor
 * without {@code sync_mode} and {@code sync_deletes}: a streaming inotify job sent from the UI was
 * stored as MANUAL with deletions off.
 */
@DisplayName("REST заданий: режим синхронизации и репликация удалений доходят до сервиса")
class JobControllerSyncModeTest {

    private JobService jobService;
    private JobController controller;

    private final CommonAuthenticationToken admin = new CommonAuthenticationToken(
            new UserSession("admin", "Admin", "admin@example.com", List.of("admins"), "ldap", true, Role.ADMIN));
    private final CommonAuthenticationToken writer = new CommonAuthenticationToken(
            new UserSession("alice", "Alice", "alice@example.com", List.of("users"), "ldap", false, Role.WRITER));

    @BeforeEach
    void setUp() {
        jobService = mock(JobService.class);
        controller = new JobController(jobService);
    }

    private static CreateJobRequest streamingRequest(String principal) {
        return new CreateJobRequest("/warehouse/sales", "/backup/sales", "dc1", "dc2", 0L, principal,
                false, false, null, 20, "STANDARD", null, "STREAMING_INOTIFY", true);
    }

    private CreateJobRequest capturedCreate() {
        ArgumentCaptor<CreateJobRequest> captor = ArgumentCaptor.forClass(CreateJobRequest.class);
        verify(jobService).createJob(captor.capture(), any());
        return captor.getValue();
    }

    private CreateJobRequest capturedUpdate() {
        ArgumentCaptor<CreateJobRequest> captor = ArgumentCaptor.forClass(CreateJobRequest.class);
        verify(jobService).updateJob(eq("job-1"), captor.capture());
        return captor.getValue();
    }

    @Test
    @DisplayName("Создание администратором")
    void createByAdminKeepsSyncSettings() {
        controller.createJob(streamingRequest("etl@EXAMPLE.COM"), admin);

        CreateJobRequest passed = capturedCreate();
        assertEquals("STREAMING_INOTIFY", passed.syncMode());
        assertTrue(passed.syncDeletes());
    }

    @Test
    @DisplayName("Создание пользователем WRITER")
    void createByWriterKeepsSyncSettings() {
        controller.createJob(streamingRequest(null), writer);

        CreateJobRequest passed = capturedCreate();
        assertEquals("STREAMING_INOTIFY", passed.syncMode());
        assertTrue(passed.syncDeletes());
    }

    @Test
    @DisplayName("Правка администратором с явным принципалом")
    void updateByAdminKeepsSyncSettings() {
        when(jobService.updateJob(eq("job-1"), any())).thenReturn(Optional.empty());

        controller.updateJob("job-1", streamingRequest("etl@EXAMPLE.COM"), admin);

        CreateJobRequest passed = capturedUpdate();
        assertEquals("STREAMING_INOTIFY", passed.syncMode());
        assertTrue(passed.syncDeletes());
    }

    @Test
    @DisplayName("Правка владельцем WRITER")
    void updateByOwnerKeepsSyncSettings() {
        JobEntity existing = mock(JobEntity.class);
        when(existing.getCreatedBy()).thenReturn("alice");
        when(existing.getExecutionPrincipal()).thenReturn("alice@EXAMPLE.COM");
        when(jobService.getJobEntity("job-1")).thenReturn(Optional.of(existing));
        when(jobService.updateJob(eq("job-1"), any())).thenReturn(Optional.empty());

        controller.updateJob("job-1", streamingRequest(null), writer);

        CreateJobRequest passed = capturedUpdate();
        assertEquals("STREAMING_INOTIFY", passed.syncMode());
        assertTrue(passed.syncDeletes());
        assertEquals("alice@EXAMPLE.COM", passed.executionPrincipal());
    }
}
