package io.github.balookrd.pristan.sklad;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = {"io.github.balookrd.pristan"})
@ConfigurationPropertiesScan(basePackages = {"io.github.balookrd.pristan"})
public class SkladApplication {

    public static void main(String[] args) {
        SpringApplication.run(SkladApplication.class, args);
    }
}
