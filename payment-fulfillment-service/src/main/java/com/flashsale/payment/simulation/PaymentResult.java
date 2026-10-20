package com.flashsale.payment.simulation;

public record PaymentResult(
    boolean successful,
    String paymentReference,
    String failureCode,
    String reason
) {
    public static PaymentResult success(String paymentReference) {
        return new PaymentResult(true, paymentReference, null, null);
    }

    public static PaymentResult declined(String failureCode, String reason) {
        return new PaymentResult(false, null, failureCode, reason);
    }
}
