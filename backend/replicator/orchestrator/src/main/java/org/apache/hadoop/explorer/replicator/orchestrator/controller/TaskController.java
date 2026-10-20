package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.replicator.orchestrator.service.JobService;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/**
 * REST API контроллер для распределенного пула пофайловых задач (Distributed Task Queue).
 */
@RestController
@RequestMapping
public class TaskController {

    private final TaskService taskService;
    private final JobService jobService;

    public TaskController(TaskService taskService, JobService jobService) {
        this.taskService = taskService;
        this.jobService = jobService;
    }

    private UserSession getSession(Authentication auth) {
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            return tokenAuth.getUserSession();
        }
        return null;
    }

    private void checkJobReadAccess(String jobId, Authentication auth, String agentSecret) {
        if (agentSecret != null && !agentSecret.isBlank()) {
            return;
        }
        UserSession session = getSession(auth);
        if (session == null) {
            throw new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "Требуется аутентификация для просмотра задач"
            );
        }
        if (session.isAdmin() || session.systemRole() == org.apache.hadoop.explorer.common.model.Role.READER
                || (session.groups() != null && session.groups().contains("replicator-agents"))) {
            return;
        }
        var opt = jobService.getJobEntity(jobId);
        if (opt.isPresent()) {
            String createdBy = opt.get().getCreatedBy();
            if (createdBy != null && (createdBy.equalsIgnoreCase(session.username())
                    || JobService.matchesJobOwner(opt.get(), session.username()))) {
                return;
            }
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN, "Недостаточно прав для просмотра задач другого пользователя"
            );
        }
    }

    /**
     * Проверка статуса наполненности очереди задач и насыщения (Backpressure).
     */
    @GetMapping({"/api/v1/tasks/queue-status", "/tasks/queue-status"})
    public ResponseEntity<Map<String, Object>> getQueueStatus(
            @RequestParam(required = false) String jobId,
            @RequestParam(required = false) String clusterId
    ) {
        long queuedCount = taskService.getQueuedTasksCount(jobId);
        boolean saturated = taskService.isQueueSaturated(jobId, clusterId);
        return ResponseEntity.ok(Map.of(
                "queued_count", queuedCount,
                "saturated", saturated
        ));
    }

    /**
     * Получение задач для инспекции в UI / мониторинге с поддержкой пагинации и защитой от BOLA/IDOR.
     */
    @GetMapping({"/api/v1/jobs/{jobId}/tasks", "/jobs/{jobId}/tasks"})
    public ResponseEntity<?> getTasks(
            @PathVariable String jobId,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            @RequestHeader(required = false, name = "X-Agent-Secret") String agentSecret,
            Authentication auth
    ) {
        checkJobReadAccess(jobId, auth, agentSecret);

        // Принудительная пагинация по умолчанию (PERF: защита от OOM при 100k+ файлов)
        int p = (page != null && page >= 0) ? page : 0;
        int s = (size != null && size > 0) ? Math.min(size, 1000) : 100;
        return ResponseEntity.ok(taskService.getTasksByJob(jobId, PageRequest.of(p, s)));
    }
}
