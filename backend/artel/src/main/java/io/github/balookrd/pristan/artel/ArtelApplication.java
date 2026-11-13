package io.github.balookrd.pristan.artel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = {"io.github.balookrd.pristan"})
@ConfigurationPropertiesScan(basePackages = {"io.github.balookrd.pristan"})
public class ArtelApplication {

    public static void main(String[] args) {
        SpringApplication.run(ArtelApplication.class, args);
    }
}
