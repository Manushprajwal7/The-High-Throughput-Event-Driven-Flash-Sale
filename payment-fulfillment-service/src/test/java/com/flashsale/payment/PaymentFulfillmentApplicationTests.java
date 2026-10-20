package com.flashsale.payment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentFulfillmentApplicationTests {

    @Test
    void applicationClassLoads() {
        assertThat(PaymentFulfillmentApplication.class).isNotNull();
    }
}
