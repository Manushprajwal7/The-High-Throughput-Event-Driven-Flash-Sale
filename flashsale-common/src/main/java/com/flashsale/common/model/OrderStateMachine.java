package com.flashsale.common.model;

import com.flashsale.common.exception.IllegalStateTransitionException;

import java.util.Map;
import java.util.Set;

public final class OrderStateMachine {

    private static final Map<OrderStatus, Set<OrderStatus>> VALID_TRANSITIONS = Map.of(
        OrderStatus.PENDING, Set.of(
            OrderStatus.PROCESSING,
            OrderStatus.PAYMENT_FAILED,
            OrderStatus.EXPIRED,
            OrderStatus.CANCELLED
        ),
        OrderStatus.PROCESSING, Set.of(
            OrderStatus.CONFIRMED,
            OrderStatus.PAYMENT_FAILED,
            OrderStatus.EXPIRED,
            OrderStatus.CANCELLED
        ),
        OrderStatus.CONFIRMED, Set.of(),
        OrderStatus.PAYMENT_FAILED, Set.of(),
        OrderStatus.EXPIRED, Set.of(),
        OrderStatus.CANCELLED, Set.of()
    );

    private OrderStateMachine() {}

    public static boolean isValidTransition(OrderStatus from, OrderStatus to) {
        if (from == null || to == null) {
            return false;
        }
        Set<OrderStatus> allowed = VALID_TRANSITIONS.get(from);
        return allowed != null && allowed.contains(to);
    }

    public static void validateTransition(OrderStatus from, OrderStatus to) {
        if (!isValidTransition(from, to)) {
            throw new IllegalStateTransitionException(
                String.format("Illegal order state transition from %s to %s", from, to)
            );
        }
    }
}
