package com.flashsale.order.statemachine;

import com.flashsale.common.exception.IllegalStateTransitionException;
import com.flashsale.common.model.OrderStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStateMachineTest {

    @Test
    void shouldAllowValidTransitionsFromPending() {
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PENDING, OrderStatus.PROCESSING)).doesNotThrowAnyException();
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PENDING, OrderStatus.PAYMENT_FAILED)).doesNotThrowAnyException();
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PENDING, OrderStatus.EXPIRED)).doesNotThrowAnyException();
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PENDING, OrderStatus.CANCELLED)).doesNotThrowAnyException();
    }

    @Test
    void shouldAllowValidTransitionsFromProcessing() {
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PROCESSING, OrderStatus.CONFIRMED)).doesNotThrowAnyException();
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PROCESSING, OrderStatus.PAYMENT_FAILED)).doesNotThrowAnyException();
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PROCESSING, OrderStatus.EXPIRED)).doesNotThrowAnyException();
        assertThatCode(() -> OrderStateMachine.validateTransition(OrderStatus.PROCESSING, OrderStatus.CANCELLED)).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectInvalidTransitionsFromTerminalStates() {
        assertThatThrownBy(() -> OrderStateMachine.validateTransition(OrderStatus.CONFIRMED, OrderStatus.CANCELLED))
            .isInstanceOf(IllegalStateTransitionException.class);

        assertThatThrownBy(() -> OrderStateMachine.validateTransition(OrderStatus.CONFIRMED, OrderStatus.PROCESSING))
            .isInstanceOf(IllegalStateTransitionException.class);

        assertThatThrownBy(() -> OrderStateMachine.validateTransition(OrderStatus.PAYMENT_FAILED, OrderStatus.CONFIRMED))
            .isInstanceOf(IllegalStateTransitionException.class);

        assertThatThrownBy(() -> OrderStateMachine.validateTransition(OrderStatus.EXPIRED, OrderStatus.CONFIRMED))
            .isInstanceOf(IllegalStateTransitionException.class);

        assertThatThrownBy(() -> OrderStateMachine.validateTransition(OrderStatus.CANCELLED, OrderStatus.CONFIRMED))
            .isInstanceOf(IllegalStateTransitionException.class);
    }
}
