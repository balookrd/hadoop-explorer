package org.apache.hadoop.explorer.common.web;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.File;

/**
 * Контроллер для обслуживания главной страницы Svelte 5 SPA и маршрутизации HTML5 History API.
 */
@Controller
public class SpaController {

    @Value("${frontend.dist:${FRONTEND_DIST:/app/frontend/dist}}")
    private String frontendDist;

    @GetMapping(value = {
        "/",
        "/index.html",
        "/{p1:^(?!api|actuator|health|metrics|assets|error|v3|swagger).*$}",
        "/{p1:^(?!api|actuator|health|metrics|assets|error|v3|swagger).*$}/**"
    }, produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public ResponseEntity<Resource> index(HttpServletRequest request) {
        String uri = request.getRequestURI();
        if (uri.startsWith("/api") || uri.startsWith("/actuator") 
            || uri.startsWith("/health") || uri.startsWith("/metrics")
            || uri.startsWith("/assets") || uri.contains(".")) {
            return ResponseEntity.notFound().build();
        }

        File distDir = new File(frontendDist);
        Resource resource;
        if (distDir.exists() && distDir.isDirectory()) {
            resource = new FileSystemResource(new File(distDir, "index.html"));
        } else {
            resource = new ClassPathResource("static/index.html");
        }

        if (resource.exists() && resource.isReadable()) {
            return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .body(resource);
        }
        return ResponseEntity.notFound().build();
    }
}
