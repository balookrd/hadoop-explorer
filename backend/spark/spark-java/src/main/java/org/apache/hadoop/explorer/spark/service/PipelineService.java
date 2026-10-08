package org.apache.hadoop.explorer.spark.service;

import org.apache.hadoop.explorer.spark.dto.*;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PipelineService {

    private final Map<String, PipelineDefinition> pipelines = new ConcurrentHashMap<>();
    private final Map<String, PipelineRun> runs = new ConcurrentHashMap<>();

    public List<PipelineDefinition> listPipelines() {
        return new ArrayList<>(pipelines.values());
    }

    public PipelineDefinition createOrUpdate(PipelineDefinition definition) {
        definition.validateDag();
        double now = System.currentTimeMillis() / 1000.0;
        PipelineDefinition updated = new PipelineDefinition(
                definition.id(),
                definition.name(),
                definition.description(),
                definition.clusterId(),
                definition.nodes(),
                definition.edges(),
                definition.createdAt() > 0 ? definition.createdAt() : now,
                now
        );
        pipelines.put(updated.id(), updated);
        return updated;
    }

    public Optional<PipelineDefinition> getPipeline(String id) {
        return Optional.ofNullable(pipelines.get(id));
    }

    public boolean deletePipeline(String id) {
        return pipelines.remove(id) != null;
    }

    public Optional<PipelineRun> runPipeline(String pipelineId) {
        PipelineDefinition p = pipelines.get(pipelineId);
        if (p == null) return Optional.empty();

        String runId = UUID.randomUUID().toString().substring(0, 8);
        double now = System.currentTimeMillis() / 1000.0;
        Map<String, NodeExecutionState> states = new LinkedHashMap<>();

        for (PipelineNode node : p.nodes()) {
            states.put(node.id(), new NodeExecutionState(
                    "success",
                    "Node [" + node.name() + "] completed successfully",
                    null,
                    0.4
            ));
        }

        PipelineRun run = new PipelineRun(
                runId,
                pipelineId,
                "success",
                states,
                now,
                now + 0.8
        );
        runs.put(runId, run);
        return Optional.of(run);
    }

    public Optional<PipelineRun> getRun(String runId) {
        return Optional.ofNullable(runs.get(runId));
    }
}
