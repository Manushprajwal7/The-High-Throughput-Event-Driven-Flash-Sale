package com.flashsale.payment.simulation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentSimulationServiceTest {

    private PaymentSimulationService simulationService;

    @BeforeEach
    void setUp() {
        simulationService = new PaymentSimulationService("ALWAYS_SUCCESS", 0);
    }

    @Test
    void shouldReturnSuccessInAlwaysSuccessMode() {
        simulationService.setMode(PaymentSimulationMode.ALWAYS_SUCCESS);
        PaymentResult result = simulationService.processPayment(UUID.randomUUID(), "shopper_1", 1);

        assertThat(result.successful()).isTrue();
        assertThat(result.paymentReference()).startsWith("PAY-SIM-");
        assertThat(result.failureCode()).isNull();
    }

    @Test
    void shouldReturnDeclinedInAlwaysDeclineMode() {
        simulationService.setMode(PaymentSimulationMode.ALWAYS_DECLINE);
        PaymentResult result = simulationService.processPayment(UUID.randomUUID(), "shopper_1", 1);

        assertThat(result.successful()).isFalse();
        assertThat(result.paymentReference()).isNull();
        assertThat(result.failureCode()).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    void shouldThrowExceptionInAlwaysTimeoutMode() {
        simulationService.setMode(PaymentSimulationMode.ALWAYS_TIMEOUT);

        assertThatThrownBy(() -> simulationService.processPayment(UUID.randomUUID(), "shopper_1", 1))
            .isInstanceOf(PaymentTimeoutException.class)
            .hasMessageContaining("Simulated payment gateway timeout");
    }

    @Test
    void shouldHandleRuleBasedModeDeterministically() {
        simulationService.setMode(PaymentSimulationMode.RULE_BASED);

        // Shopper with decline
        PaymentResult declined = simulationService.processPayment(UUID.randomUUID(), "user_decline_99", 1);
        assertThat(declined.successful()).isFalse();
        assertThat(declined.failureCode()).isEqualTo("CARD_DECLINED");

        // Shopper with timeout
        assertThatThrownBy(() -> simulationService.processPayment(UUID.randomUUID(), "user_timeout_88", 1))
            .isInstanceOf(PaymentTimeoutException.class);

        // Normal shopper
        PaymentResult success = simulationService.processPayment(UUID.randomUUID(), "user_happy_path", 1);
        assertThat(success.successful()).isTrue();
        assertThat(success.paymentReference()).startsWith("PAY-SIM-");
    }
}
