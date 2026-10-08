package org.apache.hadoop.explorer.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = {"org.apache.hadoop.explorer"})
@ConfigurationPropertiesScan(basePackages = {"org.apache.hadoop.explorer"})
public class HdfsExplorerApplication {

    public static void main(String[] args) {
        SpringApplication.run(HdfsExplorerApplication.class, args);
    }
}
