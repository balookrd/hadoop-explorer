package org.apache.hadoop.explorer.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"org.apache.hadoop.explorer"})
@ConfigurationPropertiesScan(basePackages = {"org.apache.hadoop.explorer"})
@EnableAsync
@EnableScheduling
public class SparkExplorerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkExplorerApplication.class, args);
    }
}
