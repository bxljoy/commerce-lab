package com.commercelab.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

@SpringBootTest(properties = "inventory.outbox.enabled=false")
class InventoryServiceApplicationIT extends AbstractPostgresIntegrationTest {

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
