package io.github.balookrd.pristan.iskra.controller;

import io.github.balookrd.pristan.iskra.dto.HistoryItemResponse;
import io.github.balookrd.pristan.iskra.service.SparkSessionManager;
import io.github.balookrd.pristan.iskra.util.SecurityUtils;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/history")
public class HistoryController {

    private final SparkSessionManager sessionManager;

    public HistoryController(SparkSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @GetMapping
    public List<HistoryItemResponse> listHistory(
            @RequestParam(name = "limit", defaultValue = "50") int limit,
            Authentication authentication
    ) {
        String username = SecurityUtils.getUsername(authentication);
        return sessionManager.getUserHistory(username, limit);
    }
}
