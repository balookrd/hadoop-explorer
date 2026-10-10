package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrActionResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrEmergencyStopRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrReverseRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.DrStatusResponse;
import org.apache.hadoop.explorer.replicator.orchestrator.service.DisasterRecoveryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DisasterRecoveryControllerTest {

    private DisasterRecoveryService drService;
    private DisasterRecoveryController controller;

    private CommonAuthenticationToken adminAuth;
    private CommonAuthenticationToken writerAuth;
    private CommonAuthenticationToken readerAuth;

    @BeforeEach
    void setUp() {
        drService = mock(DisasterRecoveryService.class);
        controller = new DisasterRecoveryController(drService);

        UserSession adminSession = new UserSession(
                "admin_user", "Admin User", "admin@example.com",
                List.of("admins"), "KERBEROS", true, Role.ADMIN
        );
        adminAuth = new CommonAuthenticationToken(adminSession);

        UserSession writerSession = new UserSession(
                "writer_user", "Data Engineer", "writer@example.com",
                List.of("engineers"), "LDAP", false, Role.WRITER
        );
        writerAuth = new CommonAuthenticationToken(writerSession);

        UserSession readerSession = new UserSession(
                "reader_user", "Auditor", "reader@example.com",
                List.of("auditors"), "LDAP", false, Role.READER
        );
        readerAuth = new CommonAuthenticationToken(readerSession);
    }

    @Test
    @DisplayName("Статус DR должен быть доступен для чтения всем ролям")
    void shouldAllowReadStatusForAll() {
        when(drService.getDrStatus()).thenReturn(new DrStatusResponse(List.of(), List.of(), null, List.of(), List.of()));

        ResponseEntity<DrStatusResponse> resp = controller.getDrStatus();
        assertNotNull(resp);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(drService).getDrStatus();
    }

    @Test
    @DisplayName("Kill-Switch: должен разрешать выполнение администратору")
    void shouldAllowEmergencyStopForAdmin() {
        when(drService.emergencyStop(any(), eq("admin_user")))
                .thenReturn(DrActionResponse.stopSuccess("Успешно остановлено", 1, 0));

        DrEmergencyStopRequest req = new DrEmergencyStopRequest("dc1", "Авария", true);
        ResponseEntity<DrActionResponse> resp = controller.emergencyStop(req, adminAuth);

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(drService).emergencyStop(req, "admin_user");
    }

    @Test
    @DisplayName("Kill-Switch: должен отклонять вызов для WRITER с кодом 403 FORBIDDEN")
    void shouldRejectEmergencyStopForWriter() {
        DrEmergencyStopRequest req = new DrEmergencyStopRequest("dc1", "Авария", true);

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                controller.emergencyStop(req, writerAuth)
        );

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        assertTrue(ex.getReason().contains("Администратору"));
        verifyNoInteractions(drService);
    }

    @Test
    @DisplayName("Kill-Switch: должен отклонять вызов для READER с кодом 403 FORBIDDEN")
    void shouldRejectEmergencyStopForReader() {
        DrEmergencyStopRequest req = new DrEmergencyStopRequest("dc1", "Авария", true);

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () ->
                controller.emergencyStop(req, readerAuth)
        );

        assertEquals(HttpStatus.FORBIDDEN, ex.getStatusCode());
        verifyNoInteractions(drService);
    }

    @Test
    @DisplayName("Reverse Replication: доступен только администратору")
    void shouldRejectReverseReplicationForNonAdmin() {
        DrReverseRequest req = new DrReverseRequest("dc2", "dc1", true, true, true);

        assertThrows(ResponseStatusException.class, () ->
                controller.reverseReplication(req, writerAuth)
        );

        when(drService.reverseReplication(any(), eq("admin_user")))
                .thenReturn(DrActionResponse.reverseSuccess("Инвертировано", 1, 0, List.of("rev-1")));

        ResponseEntity<DrActionResponse> resp = controller.reverseReplication(req, adminAuth);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
    }

    @Test
    @DisplayName("Точечный разворот и Undo: доступны только администратору")
    void shouldRejectSingleJobReverseAndUndoForNonAdmin() {
        // Точечный разворот
        assertThrows(ResponseStatusException.class, () ->
                controller.reverseSingleJob("job-1", writerAuth)
        );

        // Отзыв разворота
        assertThrows(ResponseStatusException.class, () ->
                controller.undoReverseJob("job-1", writerAuth)
        );

        // Удаление зеркала
        assertThrows(ResponseStatusException.class, () ->
                controller.deleteReverseJob("job-1", writerAuth)
        );

        when(drService.undoReverse("job-1", "admin_user"))
                .thenReturn(DrActionResponse.reverseSuccess("Отозвано", 0, 0, List.of("rev-1")));

        ResponseEntity<DrActionResponse> resp = controller.undoReverseJob("job-1", adminAuth);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        verify(drService).undoReverse("job-1", "admin_user");
    }
}
