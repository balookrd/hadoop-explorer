package org.apache.hadoop.explorer.spark.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.spark.dto.WorkspacePayload;
import org.apache.hadoop.explorer.spark.dto.WorkspaceResponse;
import org.apache.hadoop.explorer.spark.model.SparkUserWorkspace;
import org.apache.hadoop.explorer.spark.repository.SparkUserWorkspaceRepository;
import org.apache.hadoop.explorer.spark.util.SecurityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/workspace")
public class WorkspaceController {

    private final SparkUserWorkspaceRepository workspaceRepository;
    private final ObjectMapper objectMapper;

    public WorkspaceController(SparkUserWorkspaceRepository workspaceRepository, ObjectMapper objectMapper) {
        this.workspaceRepository = workspaceRepository;
        this.objectMapper = objectMapper;
    }

    @GetMapping
    public WorkspaceResponse getWorkspace(Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        return workspaceRepository.findById(username)
                .map(w -> new WorkspaceResponse(w.getUsername(), parseJson(w.getStateJson()), w.getUpdatedAt()))
                .orElse(null);
    }

    @PutMapping
    public WorkspaceResponse saveWorkspace(
            @RequestBody WorkspacePayload payload,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        String json;
        try {
            json = objectMapper.writeValueAsString(payload.state() != null ? payload.state() : Map.of());
        } catch (Exception e) {
            json = "{}";
        }

        SparkUserWorkspace entity = workspaceRepository.findById(username)
                .orElse(new SparkUserWorkspace(username, json, Instant.now()));

        entity.setStateJson(json);
        entity.setUpdatedAt(Instant.now());
        SparkUserWorkspace saved = workspaceRepository.save(entity);

        return new WorkspaceResponse(saved.getUsername(), parseJson(saved.getStateJson()), saved.getUpdatedAt());
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteWorkspace(Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        workspaceRepository.deleteById(username);
    }

    private Map<String, Object> parseJson(String json) {
        if (json == null || json.isBlank()) return Map.of();
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Map.of();
        }
    }
}
