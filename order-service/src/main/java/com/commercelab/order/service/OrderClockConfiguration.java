package com.commercelab.order.service;

import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class OrderClockConfiguration {
    @Bean
    @ConditionalOnMissingBean(Clock.class)
    Clock orderClock() { return Clock.systemUTC(); }
}
