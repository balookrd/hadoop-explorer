package org.apache.hadoop.explorer.spark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public record PipelineDefinition(
        String id,
        String name,
        String description,
        @JsonProperty("cluster_id") String clusterId,
        List<PipelineNode> nodes,
        List<PipelineEdge> edges,
        @JsonProperty("created_at") double createdAt,
        @JsonProperty("updated_at") double updatedAt
) {
    public PipelineDefinition {
        if (id == null || id.isBlank()) id = UUID.randomUUID().toString().substring(0, 8);
        if (description == null) description = "";
        if (nodes == null) nodes = List.of();
        if (edges == null) edges = List.of();
        if (createdAt <= 0) createdAt = System.currentTimeMillis() / 1000.0;
        if (updatedAt <= 0) updatedAt = createdAt;
    }

    public void validateDag() {
        Set<String> nodeIds = new HashSet<>();
        for (PipelineNode node : nodes) {
            nodeIds.add(node.id());
        }

        Map<String, List<String>> adj = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();
        for (String nid : nodeIds) {
            adj.put(nid, new ArrayList<>());
            inDegree.put(nid, 0);
        }

        for (PipelineEdge edge : edges) {
            if (!nodeIds.contains(edge.fromNodeId())) {
                throw new IllegalArgumentException("Ребро исходит из несуществующего узла: " + edge.fromNodeId());
            }
            if (!nodeIds.contains(edge.toNodeId())) {
                throw new IllegalArgumentException("Ребро входит в несуществующий узел: " + edge.toNodeId());
            }
            if (edge.fromNodeId().equals(edge.toNodeId())) {
                throw new IllegalArgumentException("Обнаружена петля на узле: " + edge.fromNodeId());
            }
            adj.get(edge.fromNodeId()).add(edge.toNodeId());
            inDegree.put(edge.toNodeId(), inDegree.get(edge.toNodeId()) + 1);
        }

        // Алгоритм Кана для поиска цикла
        Queue<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        int visitedCount = 0;
        while (!queue.isEmpty()) {
            String u = queue.poll();
            visitedCount++;
            for (String v : adj.get(u)) {
                int newDeg = inDegree.get(v) - 1;
                inDegree.put(v, newDeg);
                if (newDeg == 0) {
                    queue.add(v);
                }
            }
        }

        if (visitedCount < nodeIds.size()) {
            throw new IllegalArgumentException("Граф пайплайна содержит циклическую зависимость (не является DAG)");
        }
    }
}
