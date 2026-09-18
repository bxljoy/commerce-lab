package com.commercelab.inventory.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.inventory.domain.InvalidReservationException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ReserveInventoryCommandTest {

    @Test
    void rejectsNullLineBeforeDomainConversion() {
        List<ReserveInventoryCommand.Line> lines = new ArrayList<>();
        lines.add(null);

        assertThatThrownBy(() -> new ReserveInventoryCommand(UUID.randomUUID(), lines))
                .isInstanceOf(InvalidReservationException.class);
    }
}
