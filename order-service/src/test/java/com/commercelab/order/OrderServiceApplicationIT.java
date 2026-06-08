package com.commercelab.order;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Application smoke integration test. {@code contextLoads} proves the full Spring
 * context wires up against a real Postgres (Testcontainers) — which also exercises
 * Flyway migration and Hibernate {@code ddl-auto=validate}; {@code healthIsUp} proves
 * the Actuator health endpoint (now including the DB indicator) reports UP. Runs under
 * Failsafe as an {@code *IT} because it needs Docker.
 */
@SpringBootTest
class OrderServiceApplicationIT extends AbstractPostgresIntegrationTest {

    @Autowired
    private HealthEndpoint healthEndpoint;

    @Test
    void contextLoads() {
        assertThat(healthEndpoint).isNotNull();
    }

    @Test
    void healthIsUp() {
        assertThat(healthEndpoint.health().getStatus()).isEqualTo(Status.UP);
    }
}
