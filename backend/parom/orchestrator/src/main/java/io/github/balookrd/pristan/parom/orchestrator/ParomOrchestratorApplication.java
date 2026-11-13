package io.github.balookrd.pristan.parom.orchestrator;

import io.github.balookrd.pristan.zastava.security.CommonAuthFilter;
import io.github.balookrd.pristan.zastava.security.ZastavaErrorHandlers;
import io.github.balookrd.pristan.parom.orchestrator.config.ParomProperties;
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
@EnableConfigurationProperties(ParomProperties.class)
public class ParomOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ParomOrchestratorApplication.class, args);
    }

    @Bean
    public SecurityFilterChain paromSecurityFilterChain(
        HttpSecurity http,
        CommonAuthFilter authFilter,
        io.github.balookrd.pristan.parom.orchestrator.security.AgentSecretAuthenticationFilter agentSecretFilter,
        io.github.balookrd.pristan.parom.orchestrator.security.MetricsScrapeAuthenticationFilter metricsScrapeFilter,
        ZastavaErrorHandlers errorHandlers,
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
                    "/actuator/health",
                    "/actuator/health/**",
                    "/actuator/info",
                    "/actuator/prometheus",
                    "/error",
                    "/api/v1/auth/**",
                    "/v3/api-docs/**",
                    "/swagger-ui/**"
                ).permitAll()
                .requestMatchers("/actuator/**").hasRole("ADMIN")
                .anyRequest().authenticated()
            )
            .addFilterBefore(agentSecretFilter, UsernamePasswordAuthenticationFilter.class)
            .addFilterBefore(metricsScrapeFilter, UsernamePasswordAuthenticationFilter.class)
            .addFilterBefore(authFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
