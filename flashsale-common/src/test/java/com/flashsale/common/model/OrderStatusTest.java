package com.flashsale.common.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderStatusTest {

    @Test
    void shouldIdentifyTerminalStatesCorrectly() {
        assertThat(OrderStatus.CONFIRMED.isTerminal()).isTrue();
        assertThat(OrderStatus.PAYMENT_FAILED.isTerminal()).isTrue();
        assertThat(OrderStatus.EXPIRED.isTerminal()).isTrue();
        assertThat(OrderStatus.CANCELLED.isTerminal()).isTrue();
        assertThat(OrderStatus.PENDING.isTerminal()).isFalse();
        assertThat(OrderStatus.PROCESSING.isTerminal()).isFalse();
    }
}
