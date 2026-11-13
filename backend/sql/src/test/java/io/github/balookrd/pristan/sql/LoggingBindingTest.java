package io.github.balookrd.pristan.sql;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * kyuubi-hive-jdbc-shaded bundles unrelocated SLF4J 1.7 API classes. If they win on the classpath over
 * slf4j-api 2.x from Spring Boot, logback is not bound and the whole service logs into NOP.
 */
class LoggingBindingTest {

    @Test
    void slf4jApiComesFromSlf4jJarAndIsBoundToLogback() {
        String location = LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation().toString();
        assertTrue(location.contains("slf4j-api"), "org.slf4j.LoggerFactory loaded from " + location);
        assertEquals("ch.qos.logback.classic.LoggerContext", LoggerFactory.getILoggerFactory().getClass().getName());
    }
}
