package org.apache.hadoop.explorer.common.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.io.File;

/**
 * Конфигурация обслуживания статических файлов (/assets/**, favicon и т.д.).
 */
public class SpaWebMvcConfigurer implements WebMvcConfigurer {

    @Value("${frontend.dist:${FRONTEND_DIST:/app/frontend/dist}}")
    private String frontendDist;

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        File distDir = new File(frontendDist);
        String baseLocation = (distDir.exists() && distDir.isDirectory())
            ? "file:" + distDir.getAbsolutePath() + "/"
            : "classpath:/static/";

        registry.addResourceHandler("/assets/**")
            .addResourceLocations(baseLocation + "assets/", "classpath:/static/assets/")
            .resourceChain(true);

        registry.addResourceHandler("/*.svg", "/*.ico", "/*.png", "/*.js", "/*.css", "/*.json")
            .addResourceLocations(baseLocation, "classpath:/static/")
            .resourceChain(true);
    }
}
