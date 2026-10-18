package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API контроллер для распределенного пула пофайловых задач (Distributed Task Queue).
 */
@RestController
@RequestMapping
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
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
     * Получение задач для инспекции в UI / мониторинге с поддержкой пагинации.
     */
    @GetMapping({"/api/v1/jobs/{jobId}/tasks", "/jobs/{jobId}/tasks"})
    public ResponseEntity<?> getTasks(
            @PathVariable String jobId,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size
    ) {
        if (page != null || size != null) {
            int p = page != null && page >= 0 ? page : 0;
            int s = size != null && size > 0 ? Math.min(size, 1000) : 100;
            return ResponseEntity.ok(taskService.getTasksByJob(jobId, org.springframework.data.domain.PageRequest.of(p, s)));
        }
        return ResponseEntity.ok(taskService.getTasksByJob(jobId));
    }
}
