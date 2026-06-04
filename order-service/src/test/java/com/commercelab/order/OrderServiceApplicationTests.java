package com.commercelab.order;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Phase 0 smoke test.
 *
 * <p>{@code contextLoads} proves the Spring context wires up; {@code healthIsUp}
 * proves the Actuator health endpoint reports UP without any external dependency
 * (there is no database yet in Phase 0). Richer unit tests arrive in Phase 1 once
 * there is domain logic worth testing in isolation.
 */
@SpringBootTest
class OrderServiceApplicationTests {

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
