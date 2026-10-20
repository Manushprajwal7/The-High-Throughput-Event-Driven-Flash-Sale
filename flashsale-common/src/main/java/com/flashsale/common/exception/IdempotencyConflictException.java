package com.flashsale.common.exception;

public class IdempotencyConflictException extends FlashSaleException {
    public IdempotencyConflictException(String message) {
        super("IDEMPOTENCY_CONFLICT", message, 409);
    }
}
