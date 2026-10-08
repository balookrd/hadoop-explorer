package org.apache.hadoop.explorer.yarn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = {"org.apache.hadoop.explorer"})
@ConfigurationPropertiesScan(basePackages = {"org.apache.hadoop.explorer"})
public class YarnExplorerApplication {

    public static void main(String[] args) {
        SpringApplication.run(YarnExplorerApplication.class, args);
    }
}
