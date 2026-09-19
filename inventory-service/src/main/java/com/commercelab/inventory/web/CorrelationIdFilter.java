package com.commercelab.inventory.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Correlation-ID";
    private static final Logger log = LoggerFactory.getLogger(CorrelationIdFilter.class);

    public static String validOrNew(String value) {
        return value != null && value.matches("[A-Za-z0-9._:-]{1,128}")
                ? value : UUID.randomUUID().toString();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain chain) throws ServletException, IOException {
        String correlationId = validOrNew(request.getHeader(HEADER));
        long start = System.nanoTime();
        MDC.put("correlationId", correlationId);
        request.setAttribute("correlationId", correlationId);
        response.setHeader(HEADER, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            log.info("correlationId={} operation={} status={} latencyMs={}",
                    correlationId, request.getMethod(), response.getStatus(),
                    (System.nanoTime() - start) / 1_000_000);
            MDC.remove("correlationId");
        }
    }
}
