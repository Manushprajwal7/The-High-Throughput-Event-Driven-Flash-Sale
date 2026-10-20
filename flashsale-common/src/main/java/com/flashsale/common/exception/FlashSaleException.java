package com.flashsale.common.exception;

public abstract class FlashSaleException extends RuntimeException {
    private final String errorCode;
    private final int httpStatus;

    protected FlashSaleException(String errorCode, String message, int httpStatus) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public int getHttpStatus() {
        return httpStatus;
    }
}
