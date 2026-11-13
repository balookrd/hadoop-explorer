package io.github.balookrd.pristan.zastava.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.zastava.api.AuthController;
import io.github.balookrd.pristan.zastava.audit.AuditAspect;
import io.github.balookrd.pristan.zastava.audit.AuditLogger;
import io.github.balookrd.pristan.zastava.auth.*;
import io.github.balookrd.pristan.zastava.config.ZastavaProperties;
import io.github.balookrd.pristan.zastava.config.SecurityConfigurationValidator;
import io.github.balookrd.pristan.zastava.resilience.RateLimiterService;
import io.github.balookrd.pristan.zastava.security.*;
import io.github.balookrd.pristan.zastava.session.DefaultSessionStore;
import io.github.balookrd.pristan.zastava.session.SessionStore;
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
import org.springframework.context.ApplicationEventPublisher;
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
 * Автоконфигурация Spring Boot 3 для ядра безопасности платформы Pristan.
 */
@AutoConfiguration
@EnableWebSecurity
@EnableMethodSecurity
@EnableConfigurationProperties(ZastavaProperties.class)
public class ZastavaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SecurityConfigurationValidator securityConfigurationValidator(ZastavaProperties properties) {
        return new SecurityConfigurationValidator(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public RoleResolver roleResolver(ZastavaProperties properties) {
        return new RoleResolver(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public JwtTokenService jwtTokenService(ZastavaProperties properties) {
        return new JwtTokenService(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SecurityTokenExtractor securityTokenExtractor(ZastavaProperties properties) {
        return new SecurityTokenExtractor(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public CsrfProtectionValidator csrfProtectionValidator(ZastavaProperties properties) {
        return new CsrfProtectionValidator(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SessionStore sessionStore(
        ZastavaProperties properties,
        @Autowired(required = false) ObjectMapper objectMapper,
        @Autowired(required = false) DataSource dataSource
    ) {
        return new DefaultSessionStore(properties, objectMapper, dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public LdapAuthService ldapAuthService(ZastavaProperties properties, RoleResolver roleResolver) {
        return new LdapAuthService(properties, roleResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public KerberosSpnegoService kerberosSpnegoService(ZastavaProperties properties, RoleResolver roleResolver) {
        return new KerberosSpnegoService(properties, roleResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public MockAuthService mockAuthService(ZastavaProperties properties, RoleResolver roleResolver) {
        return new MockAuthService(properties, roleResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public AuthService authService(
        ZastavaProperties properties,
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
    public AuthController authController(AuthService authService,
                                         ApplicationEventPublisher eventPublisher) {
        return new AuthController(authService, eventPublisher);
    }

    @Bean
    @ConditionalOnMissingBean
    public RateLimiterService rateLimiterService(ZastavaProperties properties) {
        return new RateLimiterService(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ClientIpResolver clientIpResolver(ZastavaProperties properties) {
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
        @Value("${spring.application.name:pristan}") String serviceName
    ) {
        return new AuditAspect(auditLogger, serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    public ZastavaErrorHandlers commonSecurityErrorHandlers() {
        return new ZastavaErrorHandlers();
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
    public CorsConfigurationSource corsConfigurationSource(ZastavaProperties properties) {
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
    public io.github.balookrd.pristan.zastava.web.SpaWebMvcConfigurer spaWebMvcConfigurer() {
        return new io.github.balookrd.pristan.zastava.web.SpaWebMvcConfigurer();
    }

    @Bean
    @ConditionalOnMissingBean
    public io.github.balookrd.pristan.zastava.web.SpaController spaController() {
        return new io.github.balookrd.pristan.zastava.web.SpaController();
    }

    @Bean
    @ConditionalOnMissingBean
    public TlsWebServerCustomizer tlsWebServerCustomizer(ZastavaProperties properties) {
        return new TlsWebServerCustomizer(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SecurityFilterChain securityFilterChain(
        HttpSecurity http,
        CommonAuthFilter authFilter,
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
