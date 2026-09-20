package com.commercelab.order;

import com.commercelab.order.events.EventJson;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Entry point for the order-service.
 *
 * <p>Phase 0 is intentionally a skeleton: the application boots, exposes Actuator
 * health, and nothing more. Domain logic (the Order aggregate and lifecycle) arrives
 * in Phase 1. See {@code docs/notes-verification.md} for the phase plan.
 */
@SpringBootApplication
public class OrderServiceApplication {

    @Bean
    EventJson eventJson() {
        return new EventJson();
    }

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
