package com.commercelab.order.service;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.commercelab.order.inventory.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;

class OrderRecoveryScheduleTest {
    final OrderProgressService progress = mock(OrderProgressService.class);
    final OrderReservationCoordinator coordinator = mock(OrderReservationCoordinator.class);
    final Clock clock = Clock.fixed(Instant.parse("2026-09-19T12:00:00Z"), ZoneOffset.UTC);

    ApplicationContextRunner context() {
        return new ApplicationContextRunner().withUserConfiguration(OrderRecoveryConfiguration.class)
                .withBean(OrderProgressService.class, () -> progress)
                .withBean(OrderReservationCoordinator.class, () -> coordinator)
                .withBean(Clock.class, () -> clock);
    }

    @Test void enabledByDefaultWithFixedDelayAndDefaultBatch() {
        when(progress.findDueIds(any(), anyInt())).thenReturn(List.of());
        context().run(ctx -> {
            assertThat(ctx).hasSingleBean(OrderRecoveryWorker.class);
            var tasks = ctx.getBean(ScheduledAnnotationBeanPostProcessor.class).getScheduledTasks();
            assertThat(tasks).hasSize(1);
            var task = (org.springframework.scheduling.config.FixedDelayTask) tasks.iterator().next().getTask();
            assertThat(task.getIntervalDuration()).isEqualTo(Duration.ofSeconds(5));
            ctx.getBean(OrderRecoveryWorker.class).runOnce();
            verify(progress, atLeastOnce()).findDueIds(clock.instant(), 20);
        });
    }

    @Test void disabledCreatesNoWorker() {
        context().withPropertyValues("order.recovery.enabled=false").run(ctx ->
                assertThat(ctx).doesNotHaveBean(OrderRecoveryWorker.class));
    }

    @Test void configurableScheduleAndBatchIsolateFailuresAndClearMdc() {
        UUID first = UUID.randomUUID(), second = UUID.randomUUID();
        when(progress.findDueIds(clock.instant(), 2)).thenReturn(List.of(first, second));
        doAnswer(call -> { MDC.put("correlationId", "leaked"); throw new IllegalStateException("failure"); })
                .when(coordinator).reconcile(first);
        doAnswer(call -> { assertThat(MDC.get("correlationId")).isNull(); return null; })
                .when(coordinator).reconcile(second);
        context().withPropertyValues("order.recovery.batch-size=2", "order.recovery.fixed-delay-ms=12345")
                .run(ctx -> {
                    var task = (org.springframework.scheduling.config.FixedDelayTask) ctx
                            .getBean(ScheduledAnnotationBeanPostProcessor.class).getScheduledTasks()
                            .iterator().next().getTask();
                    assertThat(task.getIntervalDuration()).isEqualTo(Duration.ofMillis(12345));
                    ctx.getBean(OrderRecoveryWorker.class).runOnce();
                    verify(coordinator, atLeastOnce()).reconcile(second);
                    assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
                });
    }
}
