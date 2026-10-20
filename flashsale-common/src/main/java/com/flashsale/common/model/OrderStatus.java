package com.flashsale.common.model;

/**
 * Valid states for the order lifecycle finite state machine.
 */
public enum OrderStatus {
    PENDING,
    PROCESSING,
    CONFIRMED,
    PAYMENT_FAILED,
    EXPIRED,
    CANCELLED;

    public boolean isTerminal() {
        return this == CONFIRMED || this == PAYMENT_FAILED || this == EXPIRED || this == CANCELLED;
    }
}
