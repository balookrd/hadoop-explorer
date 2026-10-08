package org.apache.hadoop.explorer.spark.service.engine;

import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Component
public class LivyClient {

    private static final Logger log = LoggerFactory.getLogger(LivyClient.class);
    private final MockSparkEngine mockEngine = new MockSparkEngine();
    private final Random random = new Random();

    public record LivySessionInfo(
            int livyId,
            String appId,
            String state
    ) {}

    public LivySessionInfo createSession(SparkProperties.SparkClusterConfig cluster, String kind) {
        log.info("Создание сессии Spark через Livy ({}) kind={}", cluster.getLivyUrl(), kind);
        int livyId = random.nextInt(9000) + 1000;
        String appId = "application_1694000000_" + (random.nextInt(8999) + 1000);
        return new LivySessionInfo(livyId, appId, "idle");
    }

    public MockSparkEngine getMockEngine() {
        return mockEngine;
    }
}
