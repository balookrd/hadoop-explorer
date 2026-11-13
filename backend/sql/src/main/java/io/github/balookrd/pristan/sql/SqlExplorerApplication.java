package io.github.balookrd.pristan.sql;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"io.github.balookrd.pristan"})
@ConfigurationPropertiesScan(basePackages = {"io.github.balookrd.pristan"})
@EnableAsync
@EnableScheduling
public class SqlExplorerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SqlExplorerApplication.class, args);
    }
}
