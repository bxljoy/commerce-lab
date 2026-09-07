package com.commercelab.order;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

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

    @Autowired
    private Environment environment;

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void contextLoads() {
        assertThat(healthEndpoint).isNotNull();
    }

    @Test
    void healthIsUp() {
        assertThat(healthEndpoint.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void openEntityManagerInViewRemainsDisabled() {
        assertThat(environment.getProperty("spring.jpa.open-in-view", Boolean.class)).isFalse();
        assertThat(applicationContext.containsBean("openEntityManagerInViewInterceptor")).isFalse();
    }
}
