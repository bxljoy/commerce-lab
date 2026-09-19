package com.commercelab.inventory.web;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.slf4j.MDC;

@ExtendWith(OutputCaptureExtension.class)
class CorrelationIdFilterTest {
    @Test void logsOnlyTypedIdentityAndClearsItEvenOnException(CapturedOutput output) throws Exception {
        var id = java.util.UUID.randomUUID();
        var request = new MockHttpServletRequest("POST", "/api/v1/reservations");
        request.addHeader("X-Correlation-ID", "identity:failure");
        assertThatThrownBy(() -> new CorrelationIdFilter().doFilter(request, new MockHttpServletResponse(), (req, res) -> {
            req.setAttribute(CorrelationIdFilter.ORDER_ID_ATTRIBUTE, id);
            throw new jakarta.servlet.ServletException("failure");
        })).isInstanceOf(jakarta.servlet.ServletException.class);
        assertThat(output.getAll()).contains("orderId=" + id + " correlationId=identity:failure");
        assertThat(request.getAttribute(CorrelationIdFilter.ORDER_ID_ATTRIBUTE)).isNull();
        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("orderId")).isNull();

        var next = new MockHttpServletRequest();
        new CorrelationIdFilter().doFilter(next, new MockHttpServletResponse(), (req, res) ->
                req.setAttribute(CorrelationIdFilter.ORDER_ID_ATTRIBUTE, "unsafe-identity\nraw-body"));
        assertThat(output.getAll()).doesNotContain("unsafe-identity", "raw-body");
        assertThat(next.getAttribute(CorrelationIdFilter.ORDER_ID_ATTRIBUTE)).isNull();
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test void acceptsEchoesAndLogsCorrelationWithCleanup(CapturedOutput output) throws Exception {
        var request = new MockHttpServletRequest("POST", "/api/v1/reservations");
        request.addHeader("X-Correlation-ID", "test:origin-1");
        var response = new MockHttpServletResponse();
        new CorrelationIdFilter().doFilter(request, response, (req, res) ->
                assertThat(MDC.get("correlationId")).isEqualTo("test:origin-1"));
        assertThat(response.getHeader("X-Correlation-ID")).isEqualTo("test:origin-1");
        assertThat(MDC.get("correlationId")).isNull();
        assertThat(output.getAll()).contains("correlationId=test:origin-1", "operation=POST", "latencyMs=");
    }

    @Test void generatesForMissingOrInvalidAndCleansOnException() {
        for (String value : new String[] {null, "", "bad value", "x".repeat(129), "bad\nheader"}) {
            var request = new MockHttpServletRequest();
            if (value != null) request.addHeader("X-Correlation-ID", value);
            var response = new MockHttpServletResponse();
            assertThatThrownBy(() -> new CorrelationIdFilter().doFilter(request, response, (req, res) -> {
                assertThat(MDC.get("correlationId")).matches("[0-9a-f-]{36}");
                throw new jakarta.servlet.ServletException("failure");
            })).isInstanceOf(jakarta.servlet.ServletException.class);
            assertThat(response.getHeader("X-Correlation-ID")).matches("[0-9a-f-]{36}");
            assertThat(MDC.get("correlationId")).isNull();
        }
    }
}
