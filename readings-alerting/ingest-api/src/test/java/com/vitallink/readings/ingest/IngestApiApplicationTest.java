package com.vitallink.readings.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.vitallink.readings.ingest.seed.DevCredentialSeeder;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IngestApiApplicationTest extends SpringIntegrationTestBase {

    @Autowired
    TestRestTemplate http;

    @Autowired
    ApplicationContext context;

    @Test
    void context_loads() {
        ResponseEntity<String> health = http.getForEntity("/actuator/health", String.class);

        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).contains("\"status\":\"UP\"");
    }

    @Test
    void dev_credential_seeder_is_not_loaded_without_seed_profile() {
        assertThat(context.getBeanNamesForType(DevCredentialSeeder.class)).isEmpty();
    }
}
