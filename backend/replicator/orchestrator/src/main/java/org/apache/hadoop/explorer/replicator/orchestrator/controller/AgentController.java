package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.model.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.model.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.service.TaskService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class AgentController {

    private final AgentRegistry agentRegistry;
    private final TaskService taskService;

    public AgentController(AgentRegistry agentRegistry, TaskService taskService) {
        this.agentRegistry = agentRegistry;
        this.taskService = taskService;
    }

    @PostMapping("/api/v1/agents/register")
    public ResponseEntity<Map<String, Object>> register(
        @RequestBody AgentRegisterRequest req,
        @RequestHeader(value = "X-Agent-Secret", required = false) String secret
    ) {
        agentRegistry.register(req, secret);
        return ResponseEntity.ok(Map.of("status", "registered", "agent_id", req.getAgentId()));
    }

    @PostMapping({"/api/v1/agents/heartbeat", "/api/v1/workers/heartbeat"})
    public ResponseEntity<Map<String, Object>> heartbeat(
        @RequestBody AgentHeartbeatRequest req,
        @RequestHeader(value = "X-Agent-Secret", required = false) String secret
    ) {
        boolean ok = agentRegistry.heartbeat(req, secret);
        return ResponseEntity.ok(Map.of("status", ok ? "acknowledged" : "unknown_agent"));
    }

    @PostMapping("/api/v1/agents/unregister")
    public ResponseEntity<Map<String, Object>> unregister(
        @RequestParam("agent_id") String agentId,
        @RequestHeader(value = "X-Agent-Secret", required = false) String secret
    ) {
        boolean ok = agentRegistry.unregister(agentId, secret);
        if (ok) {
            taskService.failoverTasksForAgent(agentId, "Агент дерегистрирован (Graceful stop)");
        }
        return ResponseEntity.ok(Map.of("status", ok ? "unregistered" : "not_found"));
    }

    @GetMapping("/api/v1/agents")
    public ResponseEntity<List<org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto>> listAgents() {
        return ResponseEntity.ok(agentRegistry.getAgentDtos());
    }
}
