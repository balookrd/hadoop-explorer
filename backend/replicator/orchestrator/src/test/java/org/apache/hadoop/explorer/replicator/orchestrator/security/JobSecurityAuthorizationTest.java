package org.apache.hadoop.explorer.replicator.orchestrator.security;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.controller.JobController;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.CreateJobRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.JobResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("Orchestrator Security & Authorization P0 Tests")
class JobSecurityAuthorizationTest {

    private JobService jobService;
    private JobController controller;

    private CommonAuthenticationToken adminAuth;
    private CommonAuthenticationToken writerAuth;
    private CommonAuthenticationToken readerAuth;

    @BeforeEach
    void setUp() {
        jobService = mock(JobService.class);
        controller = new JobController(jobService);

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

        UserSession readerSession = new UserSession(
                "auditor_alice", "Alice", "alice@corp.local",
                List.of("auditors"), "LDAP", false, Role.READER
        );
        readerAuth = new CommonAuthenticationToken(readerSession);
    }

    @Test
    @DisplayName("Auth Bypass: неаутентифицированный вызов createJob отклоняется с кодом 401 UNAUTHORIZED")
    void testUnauthenticatedCreateJobRejected() {
        CreateJobRequest request = new CreateJobRequest(
                "/data/src", "/data/dst", "dc1", "dc2", 100L,
                "some_user@REALM.LOCAL", false, false, null, 20, "STANDARD", null, "MANUAL"
        );

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> {
            controller.createJob(request, null);
        });

        assertEquals(HttpStatus.UNAUTHORIZED, ex.getStatusCode());
        verify(jobService, never()).createJob(any(), any());
    }

    @Test
    @DisplayName("RBAC: роль READER не имеет права создавать задачи (код 403 FORBIDDEN)")
    void testReaderCannotCreateJob() {
        CreateJobRequest request = new CreateJobRequest(
                "/data/src", "/data/dst", "dc1", "dc2", 100L,
                "auditor_alice@REALM.LOCAL", false, false, null, 20, "STANDARD", null, "MANUAL"
        );

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> {
            controller.createJob(request, readerAuth);
        });

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        verify(jobService, never()).createJob(any(), any());
    }

    @Test
    @DisplayName("Privilege Escalation: обычный пользователь (WRITER) не может подменить executionPrincipal на чужой (hdfs)")
    void testWriterCannotImpersonateOtherPrincipal() {
        CreateJobRequest request = new CreateJobRequest(
                "/data/src", "/data/dst", "dc1", "dc2", 100L,
                "hdfs@REALM.LOCAL", true, false, null, 20, "STANDARD", null, "MANUAL"
        );

        when(jobService.createJob(any(), any())).thenReturn(mock(JobResponse.class));

        controller.createJob(request, writerAuth);

        ArgumentCaptor<CreateJobRequest> reqCaptor = ArgumentCaptor.forClass(CreateJobRequest.class);
        ArgumentCaptor<String> userCaptor = ArgumentCaptor.forClass(String.class);
        verify(jobService).createJob(reqCaptor.capture(), userCaptor.capture());

        CreateJobRequest capturedReq = reqCaptor.getValue();
        // executionPrincipal принудительно переписан на имя аутентифицированного пользователя
        assertEquals("engineer_bob@REALM.LOCAL", capturedReq.executionPrincipal());
        // runAsServiceAccount для non-admin сброшен в false
        assertFalse(capturedReq.runAsServiceAccount());
        assertEquals("engineer_bob", userCaptor.getValue());
    }

    @Test
    @DisplayName("Admin Impersonation: Администратор может явно указывать executionPrincipal")
    void testAdminCanSpecifyExecutionPrincipal() {
        CreateJobRequest request = new CreateJobRequest(
                "/data/src", "/data/dst", "dc1", "dc2", 100L,
                "etl_service@REALM.LOCAL", true, false, null, 20, "STANDARD", null, "MANUAL"
        );

        when(jobService.createJob(any(), any())).thenReturn(mock(JobResponse.class));

        controller.createJob(request, adminAuth);

        ArgumentCaptor<CreateJobRequest> reqCaptor = ArgumentCaptor.forClass(CreateJobRequest.class);
        verify(jobService).createJob(reqCaptor.capture(), eq("admin_user"));

        CreateJobRequest capturedReq = reqCaptor.getValue();
        assertEquals("etl_service@REALM.LOCAL", capturedReq.executionPrincipal());
        assertTrue(capturedReq.runAsServiceAccount());
    }

    @Test
    @DisplayName("Timing Attack: AgentRegistry.verifySecret выполняет constant-time валидацию")
    void testAgentRegistryVerifySecretConstantTime() {
        var props = new org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties();
        props.setAgentSecret("super-secret-cluster-token-12345");
        AgentRegistry registry = new AgentRegistry(props, new org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry(props));

        assertDoesNotThrow(() -> registry.verifySecret("super-secret-cluster-token-12345"));
        assertThrows(SecurityException.class, () -> registry.verifySecret("wrong-secret"));
        assertThrows(SecurityException.class, () -> registry.verifySecret("super-secret-cluster-token-1234X"));
        assertThrows(SecurityException.class, () -> registry.verifySecret(""));
        assertThrows(SecurityException.class, () -> registry.verifySecret(null));
    }

    @Test
    @DisplayName("IDOR Protection: READER не имеет права обновлять прогресс задачи (403 FORBIDDEN)")
    void testReaderCannotUpdateJobProgress() {
        org.apache.hadoop.explorer.replicator.model.UpdateJobRequest updateReq =
                new org.apache.hadoop.explorer.replicator.model.UpdateJobRequest("RUNNING", 100L, 500L, "progress");

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> {
            controller.updateJobProgress("job-123", updateReq, readerAuth);
        });

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        verify(jobService, never()).updateJobProgress(any(), any());
    }

    @Test
    @DisplayName("IDOR Protection: WRITER не может обновлять прогресс чужой задачи (403 FORBIDDEN)")
    void testWriterCannotUpdateOtherUsersJobProgress() {
        org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity otherJob =
                new org.apache.hadoop.explorer.replicator.orchestrator.entity.JobEntity();
        otherJob.setId("job-other");
        otherJob.setCreatedBy("stranger_external_user");

        when(jobService.getJobEntity("job-other")).thenReturn(java.util.Optional.of(otherJob));

        org.apache.hadoop.explorer.replicator.model.UpdateJobRequest updateReq =
                new org.apache.hadoop.explorer.replicator.model.UpdateJobRequest("FAILED", 0L, 500L, "hacked");

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> {
            controller.updateJobProgress("job-other", updateReq, writerAuth);
        });

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        verify(jobService, never()).updateJobProgress(any(), any());
    }

    @Test
    @DisplayName("Agent Auth: системный агент (system_agent) может обновлять прогресс задачи")
    void testAgentCanUpdateJobProgress() {
        UserSession agentSession = new UserSession(
                "system_agent", "Agent Daemon", "agent@cluster.local",
                List.of("replicator-agents"), "SECRET", true, Role.ADMIN
        );
        CommonAuthenticationToken agentAuth = new CommonAuthenticationToken(agentSession);

        org.apache.hadoop.explorer.replicator.model.UpdateJobRequest updateReq =
                new org.apache.hadoop.explorer.replicator.model.UpdateJobRequest("RUNNING", 500L, 500L, "all chunks copied");

        when(jobService.updateJobProgress(eq("job-123"), any())).thenReturn(java.util.Optional.of(mock(JobResponse.class)));

        var resp = controller.updateJobProgress("job-123", updateReq, agentAuth);
        assertNotNull(resp);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(jobService).updateJobProgress(eq("job-123"), eq(updateReq));
    }
}
