package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class OutboxRetryPolicyTest {
    @ParameterizedTest
    @CsvSource({"1,1000", "2,2000", "6,32000", "7,60000", "9223372036854775807,60000"})
    void capsBeforeOverflowAndAddsBoundedJitter(long attempt, long base) {
        assertThat(new OutboxRetryPolicy(() -> 0).delay(attempt)).isEqualTo(Duration.ofMillis(base));
        assertThat(new OutboxRetryPolicy(() -> 250).delay(attempt)).isEqualTo(Duration.ofMillis(base + 250));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void rejectsNonpositiveAttempts(long attempt) {
        assertThatIllegalArgumentException().isThrownBy(() -> new OutboxRetryPolicy(() -> 0).delay(attempt));
    }
}
