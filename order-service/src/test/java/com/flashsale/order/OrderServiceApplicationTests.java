package com.flashsale.order;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderServiceApplicationTests {

    @Test
    void applicationClassLoads() {
        assertThat(OrderServiceApplication.class).isNotNull();
    }
}
