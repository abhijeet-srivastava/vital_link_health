package com.vitallink.readings.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.vitallink.readings")
public class IngestApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestApiApplication.class, args);
    }
}
