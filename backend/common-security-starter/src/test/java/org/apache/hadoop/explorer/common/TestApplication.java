package org.apache.hadoop.explorer.common;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@SpringBootApplication
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

    @RestController
    public static class TestProtectedController {
        @GetMapping("/api/v1/test/protected")
        public Map<String, Object> protectedEndpoint() {
            return Map.of("status", "ok", "message", "Protected data accessed");
        }
    }
}
