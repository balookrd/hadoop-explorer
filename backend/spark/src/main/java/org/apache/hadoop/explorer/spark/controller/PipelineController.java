package org.apache.hadoop.explorer.spark.controller;

import org.apache.hadoop.explorer.spark.dto.PipelineDefinition;
import org.apache.hadoop.explorer.spark.dto.PipelineRun;
import org.apache.hadoop.explorer.spark.service.PipelineService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/pipelines")
public class PipelineController {

    private final PipelineService pipelineService;

    public PipelineController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @GetMapping
    public List<PipelineDefinition> listPipelines() {
        return pipelineService.listPipelines();
    }

    @PostMapping
    public PipelineDefinition createOrUpdatePipeline(@RequestBody PipelineDefinition pipeline) {
        try {
            return pipelineService.createOrUpdate(pipeline);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
        }
    }

    @GetMapping("/{pipelineId}")
    public PipelineDefinition getPipeline(@PathVariable("pipelineId") String pipelineId) {
        return pipelineService.getPipeline(pipelineId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Пайплайн не найден: " + pipelineId));
    }

    @DeleteMapping("/{pipelineId}")
    public Map<String, Object> deletePipeline(@PathVariable("pipelineId") String pipelineId) {
        boolean removed = pipelineService.deletePipeline(pipelineId);
        return Map.of("success", true, "message", "Пайплайн успешно удален");
    }

    @PostMapping("/{pipelineId}/run")
    public PipelineRun runPipeline(@PathVariable("pipelineId") String pipelineId) {
        return pipelineService.runPipeline(pipelineId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Пайплайн не найден: " + pipelineId));
    }

    @GetMapping("/runs/{runId}")
    public PipelineRun getRun(@PathVariable("runId") String runId) {
        return pipelineService.getRun(runId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Запуск пайплайна не найден: " + runId));
    }
}
