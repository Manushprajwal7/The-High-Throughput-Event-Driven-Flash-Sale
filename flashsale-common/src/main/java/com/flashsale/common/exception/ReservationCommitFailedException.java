package com.flashsale.common.exception;

public class ReservationCommitFailedException extends FlashSaleException {
    public ReservationCommitFailedException(String message) {
        super("RESERVATION_COMMIT_FAILED", message, 409);
    }
}
