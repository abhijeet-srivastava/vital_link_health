package com.vitallink.readings.alerting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.vitallink.readings")
public class AlertWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlertWorkerApplication.class, args);
    }
}
