package com.commercelab.order.service;

import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "order.recovery", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OrderRecoveryConfiguration {
    @Bean
    OrderRecoveryWorker orderRecoveryWorker(OrderProgressService progress, OrderReservationCoordinator coordinator,
            Clock clock, @Value("${order.recovery.batch-size:20}") int batchSize) {
        return new OrderRecoveryWorker(progress, coordinator, clock, batchSize);
    }
}
