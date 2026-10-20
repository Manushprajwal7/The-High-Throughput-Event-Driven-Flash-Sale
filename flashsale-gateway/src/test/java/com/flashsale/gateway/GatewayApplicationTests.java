package com.flashsale.gateway;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayApplicationTests {

    @Test
    void applicationClassLoads() {
        assertThat(GatewayApplication.class).isNotNull();
    }
}
