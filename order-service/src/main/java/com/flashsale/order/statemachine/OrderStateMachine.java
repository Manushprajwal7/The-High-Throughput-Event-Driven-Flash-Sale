package com.flashsale.order.statemachine;

import com.flashsale.common.model.OrderStatus;

public final class OrderStateMachine {
    private OrderStateMachine() {}

    public static boolean isValidTransition(OrderStatus from, OrderStatus to) {
        return com.flashsale.common.model.OrderStateMachine.isValidTransition(from, to);
    }

    public static void validateTransition(OrderStatus from, OrderStatus to) {
        com.flashsale.common.model.OrderStateMachine.validateTransition(from, to);
    }
}
