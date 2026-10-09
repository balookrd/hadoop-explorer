package org.apache.hadoop.explorer.replicator.orchestrator;

import org.apache.hadoop.explorer.common.security.CommonAuthFilter;
import org.apache.hadoop.explorer.common.security.CommonSecurityErrorHandlers;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfigurationSource;

@SpringBootApplication
@EnableScheduling
@EnableWebSecurity
@EnableConfigurationProperties(ReplicatorProperties.class)
public class ReplicatorOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReplicatorOrchestratorApplication.class, args);
    }

    @Bean
    public SecurityFilterChain replicatorSecurityFilterChain(
        HttpSecurity http,
        CommonAuthFilter authFilter,
        CommonSecurityErrorHandlers errorHandlers,
        CorsConfigurationSource corsConfigurationSource
    ) throws Exception {

        http
            .csrf(AbstractHttpConfigurer::disable)
            .cors(cors -> cors.configurationSource(corsConfigurationSource))
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .exceptionHandling(eh -> eh
                .authenticationEntryPoint(errorHandlers)
                .accessDeniedHandler(errorHandlers)
            )
            .authorizeHttpRequests(auth -> auth
                .dispatcherTypeMatchers(jakarta.servlet.DispatcherType.FORWARD, jakarta.servlet.DispatcherType.ERROR).permitAll()
                .requestMatchers(
                    "/",
                    "/index.html",
                    "/favicon.*",
                    "/assets/**",
                    "/*.js",
                    "/*.css",
                    "/*.ico",
                    "/*.svg",
                    "/*.png",
                    "/*.json",
                    "/*.woff",
                    "/*.woff2",
                    "/*.ttf",
                    "/health",
                    "/healthz",
                    "/metrics",
                    "/actuator/**",
                    "/error",
                    "/api/v1/auth/**",
                    "/api/v1/agents/**",
                    "/api/v1/workers/**",
                    "/api/v1/tokens/request",
                    "/api/v1/jobs/**",
                    "/jobs/**",
                    "/api/v1/tasks/**",
                    "/tasks/**",
                    "/api/v1/topology/**",
                    "/topology/**",
                    "/v3/api-docs/**",
                    "/swagger-ui/**"
                ).permitAll()
                .anyRequest().authenticated()
            )
            .addFilterBefore(authFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
