package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.model.*;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
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
     * Пакетная регистрация пула задач анализатором.
     */
    @PostMapping({"/api/v1/jobs/{jobId}/tasks/batch", "/jobs/{jobId}/tasks/batch"})
    public ResponseEntity<Map<String, Object>> batchCreateTasks(
            @PathVariable String jobId,
            @RequestBody BatchCreateTasksRequest request
    ) {
        request.setJobId(jobId);
        boolean ok = taskService.batchCreateTasks(jobId, request);
        if (ok) {
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                    "success", true,
                    "job_id", jobId,
                    "tasks_count", request.getTasks() != null ? request.getTasks().size() : 0
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Задание не найдено"));
    }

    /**
     * Атомарный забор порции задач свободным воркером.
     */
    @PostMapping({"/api/v1/tasks/claim", "/tasks/claim"})
    public ResponseEntity<List<TaskItemDto>> claimTasks(@RequestBody ClaimTasksRequest request) {
        List<TaskItemDto> claimed = taskService.claimTasks(request);
        return ResponseEntity.ok(claimed);
    }

    /**
     * Фиксация успешной передачи файла воркером.
     */
    @PostMapping({"/api/v1/tasks/{taskId}/complete", "/tasks/{taskId}/complete"})
    public ResponseEntity<Map<String, Object>> completeTask(
            @PathVariable String taskId,
            @RequestBody CompleteTaskRequest request
    ) {
        request.setTaskId(taskId);
        boolean ok = taskService.completeTask(request);
        if (ok) {
            return ResponseEntity.ok(Map.of("success", true, "task_id", taskId));
        }
        return ResponseEntity.notFound().build();
    }

    /**
     * Фиксация ошибки передачи файла.
     */
    @PostMapping({"/api/v1/tasks/{taskId}/fail", "/tasks/{taskId}/fail"})
    public ResponseEntity<Map<String, Object>> failTask(
            @PathVariable String taskId,
            @RequestBody FailTaskRequest request
    ) {
        request.setTaskId(taskId);
        boolean ok = taskService.failTask(request);
        if (ok) {
            return ResponseEntity.ok(Map.of("success", true, "task_id", taskId));
        }
        return ResponseEntity.notFound().build();
    }

    /**
     * Получение всех задач для инспекции в UI / мониторинге.
     */
    @GetMapping({"/api/v1/jobs/{jobId}/tasks", "/jobs/{jobId}/tasks"})
    public ResponseEntity<List<TaskItemDto>> getTasks(@PathVariable String jobId) {
        return ResponseEntity.ok(taskService.getTasksByJob(jobId));
    }
}
