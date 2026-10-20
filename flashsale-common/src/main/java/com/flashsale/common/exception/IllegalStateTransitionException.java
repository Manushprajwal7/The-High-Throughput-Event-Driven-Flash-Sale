package com.flashsale.common.exception;

public class IllegalStateTransitionException extends FlashSaleException {
    public IllegalStateTransitionException(String message) {
        super("ILLEGAL_STATE_TRANSITION", message, 409);
    }
}
