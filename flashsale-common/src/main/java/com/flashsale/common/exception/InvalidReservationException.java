package com.flashsale.common.exception;

public class InvalidReservationException extends FlashSaleException {
    public InvalidReservationException(String message) {
        super("INVALID_RESERVATION", message, 400);
    }
}
