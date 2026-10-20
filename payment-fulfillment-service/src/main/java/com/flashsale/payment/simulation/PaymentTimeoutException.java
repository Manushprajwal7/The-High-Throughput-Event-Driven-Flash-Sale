package com.flashsale.payment.simulation;

public class PaymentTimeoutException extends RuntimeException {
    public PaymentTimeoutException(String message) {
        super(message);
    }
}
