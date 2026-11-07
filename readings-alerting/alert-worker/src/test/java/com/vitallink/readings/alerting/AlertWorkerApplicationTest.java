package com.vitallink.readings.alerting;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AlertWorkerApplicationTest extends SpringIntegrationTestBase {

    @Autowired
    TestRestTemplate http;

    @Test
    void context_loads() {
        ResponseEntity<String> health = http.getForEntity("/actuator/health", String.class);

        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).contains("\"status\":\"UP\"");
    }
}
