package org.apache.hadoop.explorer.common.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.api.AuthController;
import org.apache.hadoop.explorer.common.audit.AuditAspect;
import org.apache.hadoop.explorer.common.audit.AuditLogger;
import org.apache.hadoop.explorer.common.auth.*;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.config.SecurityConfigurationValidator;
import org.apache.hadoop.explorer.common.resilience.RateLimiterService;
import org.apache.hadoop.explorer.common.security.*;
import org.apache.hadoop.explorer.common.session.DefaultSessionStore;
import org.apache.hadoop.explorer.common.session.SessionStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Locale;

/**
 * Автоконфигурация Spring Boot 3 для ядра безопасности платформы Hadoop Explorer.
 */
@AutoConfiguration
@EnableWebSecurity
@EnableMethodSecurity
@EnableConfigurationProperties(CommonSecurityProperties.class)
public class CommonSecurityAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SecurityConfigurationValidator securityConfigurationValidator(CommonSecurityProperties properties) {
        return new SecurityConfigurationValidator(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public RoleResolver roleResolver(CommonSecurityProperties properties) {
        return new RoleResolver(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public JwtTokenService jwtTokenService(CommonSecurityProperties properties) {
        return new JwtTokenService(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SecurityTokenExtractor securityTokenExtractor(CommonSecurityProperties properties) {
        return new SecurityTokenExtractor(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public CsrfProtectionValidator csrfProtectionValidator(CommonSecurityProperties properties) {
        return new CsrfProtectionValidator(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SessionStore sessionStore(
        CommonSecurityProperties properties,
        @Autowired(required = false) ObjectMapper objectMapper,
        @Autowired(required = false) DataSource dataSource
    ) {
        return new DefaultSessionStore(properties, objectMapper, dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public LdapAuthService ldapAuthService(CommonSecurityProperties properties, RoleResolver roleResolver) {
        return new LdapAuthService(properties, roleResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public KerberosSpnegoService kerberosSpnegoService(CommonSecurityProperties properties, RoleResolver roleResolver) {
        return new KerberosSpnegoService(properties, roleResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public MockAuthService mockAuthService(CommonSecurityProperties properties, RoleResolver roleResolver) {
        return new MockAuthService(properties, roleResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public AuthService authService(
        CommonSecurityProperties properties,
        JwtTokenService jwtTokenService,
        SessionStore sessionStore,
        SecurityTokenExtractor tokenExtractor,
        LdapAuthService ldapAuthService,
        KerberosSpnegoService kerberosSpnegoService,
        MockAuthService mockAuthService
    ) {
        return new CompositeAuthService(
            properties,
            jwtTokenService,
            sessionStore,
            tokenExtractor,
            ldapAuthService,
            kerberosSpnegoService,
            mockAuthService
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public AuthController authController(AuthService authService) {
        return new AuthController(authService);
    }

    @Bean
    @ConditionalOnMissingBean
    public RateLimiterService rateLimiterService(CommonSecurityProperties properties) {
        return new RateLimiterService(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ClientIpResolver clientIpResolver(CommonSecurityProperties properties) {
        return new ClientIpResolver(properties.getRateLimiter().getTrustedProxies());
    }

    @Bean
    @ConditionalOnMissingBean(name = "rateLimitFilterRegistration")
    public FilterRegistrationBean<RateLimitFilter> rateLimitFilterRegistration(
        RateLimiterService rateLimiterService,
        ClientIpResolver clientIpResolver,
        ObjectProvider<RateLimitBlockListener> blockListener
    ) {
        FilterRegistrationBean<RateLimitFilter> registration = new FilterRegistrationBean<>(
            new RateLimitFilter(rateLimiterService, clientIpResolver, blockListener.getIfAvailable()));
        registration.setName("rateLimitFilter");
        registration.addUrlPatterns("/api/*");
        // Right after springSecurityFilterChain (order -100): the authenticated user is known
        // and the filter applies even when a service declares its own SecurityFilterChain.
        registration.setOrder(SecurityProperties.DEFAULT_FILTER_ORDER + 10);
        return registration;
    }

    @Bean
    @ConditionalOnMissingBean
    public AuditLogger auditLogger() {
        return new AuditLogger();
    }

    @Bean
    @ConditionalOnMissingBean
    public AuditAspect auditAspect(
        AuditLogger auditLogger,
        @Value("${spring.application.name:hadoop-explorer}") String serviceName
    ) {
        return new AuditAspect(auditLogger, serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    public CommonSecurityErrorHandlers commonSecurityErrorHandlers() {
        return new CommonSecurityErrorHandlers();
    }

    @Bean
    @ConditionalOnMissingBean
    public CommonAuthFilter commonAuthFilter(
        SecurityTokenExtractor tokenExtractor,
        CsrfProtectionValidator csrfValidator,
        JwtTokenService jwtTokenService,
        SessionStore sessionStore,
        RoleResolver roleResolver
    ) {
        return new CommonAuthFilter(tokenExtractor, csrfValidator, jwtTokenService, sessionStore, roleResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public CorsConfigurationSource corsConfigurationSource(CommonSecurityProperties properties) {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedOriginPatterns(properties.getCors().getAllowedOrigins());
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"));
        config.setAllowedHeaders(List.of("*"));
        config.setAllowCredentials(true);
        config.setExposedHeaders(List.of("Set-Cookie", "Authorization", "X-Total-Count"));

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return source;
    }

    @Bean
    @ConditionalOnMissingBean
    public org.apache.hadoop.explorer.common.web.SpaWebMvcConfigurer spaWebMvcConfigurer() {
        return new org.apache.hadoop.explorer.common.web.SpaWebMvcConfigurer();
    }

    @Bean
    @ConditionalOnMissingBean
    public org.apache.hadoop.explorer.common.web.SpaController spaController() {
        return new org.apache.hadoop.explorer.common.web.SpaController();
    }

    @Bean
    @ConditionalOnMissingBean
    public TlsWebServerCustomizer tlsWebServerCustomizer(CommonSecurityProperties properties) {
        return new TlsWebServerCustomizer(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SecurityFilterChain securityFilterChain(
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
                    "/api/v1/auth/login",
                    "/api/v1/auth/sso",
                    "/api/v1/auth/logout",
                    "/actuator/health",
                    "/actuator/health/**",
                    "/actuator/info",
                    "/actuator/prometheus",
                    "/health",
                    "/healthz",
                    "/metrics",
                    "/error",
                    "/v3/api-docs/**",
                    "/swagger-ui/**",
                    "/swagger-ui.html"
                ).permitAll()
                // Same split as the replicator orchestrator: probes and the Prometheus scrape
                // endpoint are public, every other Actuator endpoint is for administrators only
                .requestMatchers("/actuator/**").hasRole("ADMIN")
                .requestMatchers("/api/**").authenticated()
                .anyRequest().permitAll()
            )
            .addFilterBefore(authFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }

    /**
     * Counts blocked requests in Micrometer. Nested so that the outer class never links
     * Micrometer types: without micrometer-core on the classpath this class is skipped.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    static class RateLimitMetricsConfiguration {

        static final String BLOCKS_METRIC = "hadoop.rate.limit.blocks";

        @Bean
        @ConditionalOnMissingBean(RateLimitBlockListener.class)
        RateLimitBlockListener rateLimitBlockListener(ObjectProvider<MeterRegistry> meterRegistry) {
            MeterRegistry registry = meterRegistry.getIfUnique();
            if (registry == null) {
                return null;
            }
            // Only the tier is a tag: user, IP and path would give unbounded cardinality
            return tier -> registry.counter(BLOCKS_METRIC, "tier", tier.name().toLowerCase(Locale.ROOT)).increment();
        }
    }
}
