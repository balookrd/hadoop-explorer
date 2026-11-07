package org.apache.hadoop.explorer.common.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.explorer.common.resilience.RateLimiterService;
import org.apache.hadoop.explorer.common.security.RateLimitBlockListener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimitMetricsAutoConfigurationTest {

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(
            JacksonAutoConfiguration.class, DataSourceAutoConfiguration.class,
            SecurityAutoConfiguration.class, WebMvcAutoConfiguration.class,
            CommonSecurityAutoConfiguration.class))
        .withPropertyValues(
            "hadoop.security.auth.mode=mock",
            "hadoop.security.debug=true",
            "hadoop.security.jwt.secret-key=test-secret-key-that-is-at-least-32-characters-long!",
            "spring.datasource.url=jdbc:h2:mem:rlmetrics;DB_CLOSE_DELAY=-1");

    @Configuration(proxyBeanMethods = false)
    static class RegistryConfig {
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Test
    @DisplayName("Без micrometer-core на classpath контекст поднимается, фильтр зарегистрирован")
    void contextStartsWithoutMicrometer() {
        runner.withClassLoader(new FilteredClassLoader(MeterRegistry.class)).run(context -> {
            assertNull(context.getStartupFailure());
            assertTrue(context.containsBean("rateLimitFilterRegistration"));
            assertTrue(context.getBeansOfType(RateLimitBlockListener.class).isEmpty());
            // The nested @ConditionalOnClass configuration must be skipped entirely, not just return null.
            // Limitation: the outer auto-configuration class is loaded by the regular class loader here,
            // so this test does not prove that it links without Micrometer.
            assertFalse(context.containsBean("rateLimitBlockListener"));
        });
    }

    @Test
    @DisplayName("С реестром метрик слушатель считает отказы по tier")
    void listenerCountsBlocksWithRegistry() {
        runner.withUserConfiguration(RegistryConfig.class).run(context -> {
            assertNotNull(context.getBean("rateLimitFilterRegistration", FilterRegistrationBean.class));
            RateLimitBlockListener listener = context.getBean(RateLimitBlockListener.class);
            listener.onBlock(RateLimiterService.Tier.LOGIN);
            assertEquals(1.0, context.getBean(MeterRegistry.class)
                .get("hadoop.rate.limit.blocks").tag("tier", "login").counter().count());
        });
    }
}
