package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API контроллер для отображения подключенных агентов репликации в веб-интерфейсе.
 */
@RestController
public class AgentController {

    private final AgentRegistry agentRegistry;

    public AgentController(AgentRegistry agentRegistry) {
        this.agentRegistry = agentRegistry;
    }

    @GetMapping("/api/v1/agents")
    public ResponseEntity<List<org.apache.hadoop.explorer.replicator.orchestrator.dto.AgentResponseDto>> listAgents() {
        return ResponseEntity.ok(agentRegistry.getAgentDtos());
    }
}
