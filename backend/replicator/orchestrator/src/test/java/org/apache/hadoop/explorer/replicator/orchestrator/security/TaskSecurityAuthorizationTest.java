package org.apache.hadoop.explorer.replicator.orchestrator.security;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.apache.hadoop.explorer.replicator.orchestrator.controller.TaskController;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DisplayName("TaskController Security & Pagination P0 Tests")
class TaskSecurityAuthorizationTest {

    private TaskService taskService;
    private JobService jobService;
    private TaskController controller;

    private CommonAuthenticationToken adminAuth;
    private CommonAuthenticationToken writerAuth;
    private CommonAuthenticationToken strangerAuth;

    @BeforeEach
    void setUp() {
        taskService = mock(TaskService.class);
        jobService = mock(JobService.class);
        controller = new TaskController(taskService, jobService);

        UserSession adminSession = new UserSession(
                "admin_user", "Admin", "admin@corp.local",
                List.of("admins"), "KERBEROS", true, Role.ADMIN
        );
        adminAuth = new CommonAuthenticationToken(adminSession);

        UserSession writerSession = new UserSession(
                "engineer_bob", "Bob", "bob@corp.local",
                List.of("engineers"), "LDAP", false, Role.WRITER
        );
        writerAuth = new CommonAuthenticationToken(writerSession);

        UserSession strangerSession = new UserSession(
                "stranger_dave", "Dave", "dave@corp.local",
                List.of("analytics"), "LDAP", false, Role.WRITER
        );
        strangerAuth = new CommonAuthenticationToken(strangerSession);
    }

    @Test
    @DisplayName("BOLA/IDOR: неаутентифицированный пользователь получает 401 UNAUTHORIZED при чтении подзадач")
    void testUnauthenticatedCannotReadTasks() {
        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> {
            controller.getTasks("job-123", null, null, null, null);
        });

        assertEquals(HttpStatus.UNAUTHORIZED, ex.getStatusCode());
        verify(taskService, never()).getTasksByJob(any(), any());
    }

    @Test
    @DisplayName("BOLA/IDOR: сторонний пользователь получает 403 FORBIDDEN при просмотре чужих подзадач")
    void testStrangerCannotReadOtherUsersTasks() {
        JobEntity job = new JobEntity();
        job.setId("job-secret-999");
        job.setCreatedBy("engineer_bob");

        when(jobService.getJobEntity("job-secret-999")).thenReturn(Optional.of(job));

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> {
            controller.getTasks("job-secret-999", null, null, null, strangerAuth);
        });

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        verify(taskService, never()).getTasksByJob(any(), any());
    }

    @Test
    @DisplayName("BOLA/IDOR: создатель задачи может просматривать свои подзадачи (200 OK)")
    void testCreatorCanReadOwnTasks() {
        JobEntity job = new JobEntity();
        job.setId("job-secret-999");
        job.setCreatedBy("engineer_bob");

        when(jobService.getJobEntity("job-secret-999")).thenReturn(Optional.of(job));
        Page<TaskItemDto> emptyPage = new PageImpl<>(List.of());
        when(taskService.getTasksByJob(eq("job-secret-999"), any(Pageable.class))).thenReturn(emptyPage);

        ResponseEntity<?> resp = controller.getTasks("job-secret-999", null, null, null, writerAuth);

        assertNotNull(resp);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(taskService).getTasksByJob(eq("job-secret-999"), any(Pageable.class));
    }

    @Test
    @DisplayName("BOLA/IDOR: администратор (ADMIN) может просматривать любые подзадачи (200 OK)")
    void testAdminCanReadAnyJobTasks() {
        JobEntity job = new JobEntity();
        job.setId("job-secret-999");
        job.setCreatedBy("engineer_bob");

        when(jobService.getJobEntity("job-secret-999")).thenReturn(Optional.of(job));
        Page<TaskItemDto> emptyPage = new PageImpl<>(List.of());
        when(taskService.getTasksByJob(eq("job-secret-999"), any(Pageable.class))).thenReturn(emptyPage);

        ResponseEntity<?> resp = controller.getTasks("job-secret-999", null, null, null, adminAuth);

        assertNotNull(resp);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(taskService).getTasksByJob(eq("job-secret-999"), any(Pageable.class));
    }

    @Test
    @DisplayName("BOLA/IDOR: системный агент с X-Agent-Secret может получать подзадачи без сессии")
    void testAgentWithSecretCanReadTasks() {
        Page<TaskItemDto> emptyPage = new PageImpl<>(List.of());
        when(taskService.getTasksByJob(eq("job-secret-999"), any(Pageable.class))).thenReturn(emptyPage);

        ResponseEntity<?> resp = controller.getTasks("job-secret-999", null, null, "valid-secret-12345", null);

        assertNotNull(resp);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(taskService).getTasksByJob(eq("job-secret-999"), any(Pageable.class));
    }

    @Test
    @DisplayName("PERF: при отсутствии параметров page и size принудительно выставляется пагинация page=0, size=100")
    void testTasksDefaultPaginationAppliedWhenParamsOmitted() {
        JobEntity job = new JobEntity();
        job.setId("job-secret-999");
        job.setCreatedBy("engineer_bob");

        when(jobService.getJobEntity("job-secret-999")).thenReturn(Optional.of(job));
        Page<TaskItemDto> emptyPage = new PageImpl<>(List.of());
        when(taskService.getTasksByJob(eq("job-secret-999"), any(Pageable.class))).thenReturn(emptyPage);

        controller.getTasks("job-secret-999", null, null, null, writerAuth);

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(taskService).getTasksByJob(eq("job-secret-999"), pageableCaptor.capture());

        Pageable pageable = pageableCaptor.getValue();
        assertEquals(0, pageable.getPageNumber());
        assertEquals(100, pageable.getPageSize());

        // Проверяем, что непагинированный метод getTasksByJob(String jobId) НЕ вызывался
        verify(taskService, never()).getTasksByJob(eq("job-secret-999"));
    }
}
