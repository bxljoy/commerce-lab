package com.commercelab.order.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.Timeout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
public class InventoryClientConfiguration {
    @Bean(destroyMethod = "close")
    public CloseableHttpClient inventoryHttpClient(
            @Value("${inventory.pool-acquisition-timeout-ms:500}") int acquisition,
            @Value("${inventory.connect-timeout-ms:500}") int connect,
            @Value("${inventory.response-timeout-ms:2000}") int response,
            @Value("${inventory.pool-max-total:20}") int total,
            @Value("${inventory.pool-max-per-route:10}") int perRoute) {
        if (acquisition < 1 || connect < 1 || response < 1 || total < 1 || perRoute < 1) {
            throw new IllegalArgumentException("inventory timeouts and pool limits must be positive");
        }
        var pool = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(total).setMaxConnPerRoute(perRoute)
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(connect))
                        .setSocketTimeout(Timeout.ofMilliseconds(response)).build())
                .setDefaultSocketConfig(SocketConfig.custom()
                        .setSoTimeout(Timeout.ofMilliseconds(response)).build())
                .build();
        return HttpClients.custom().setConnectionManager(pool)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofMilliseconds(acquisition))
                        .setResponseTimeout(Timeout.ofMilliseconds(response))
                        .setAuthenticationEnabled(false).build())
                .disableAutomaticRetries().disableRedirectHandling().disableCookieManagement().build();
    }

    @Bean
    public CircuitBreaker inventoryCircuitBreaker() {
        return CircuitBreaker.of("inventory", CircuitBreakerConfig.custom()
                .slidingWindowSize(10).minimumNumberOfCalls(5).failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(2)
                .recordExceptions(TransientInventoryException.class)
                .ignoreExceptions(InventoryProtocolException.class).build());
    }

    @Bean
    public InventoryGateway inventoryGateway(CloseableHttpClient inventoryHttpClient,
            CircuitBreaker inventoryCircuitBreaker, ObjectMapper mapper,
            @Value("${inventory.base-url:http://localhost:8081}") String baseUrl) {
        var client = RestClient.builder().baseUrl(baseUrl)
                .requestFactory(new HttpComponentsClientHttpRequestFactory(inventoryHttpClient)).build();
        return new RestInventoryGateway(client, mapper, inventoryCircuitBreaker);
    }
}
