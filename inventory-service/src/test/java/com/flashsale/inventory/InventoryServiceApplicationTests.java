package com.flashsale.inventory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryServiceApplicationTests {

    @Test
    void applicationClassLoads() {
        assertThat(InventoryServiceApplication.class).isNotNull();
    }
}
