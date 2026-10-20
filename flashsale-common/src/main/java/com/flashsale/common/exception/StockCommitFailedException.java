package com.flashsale.common.exception;

public class StockCommitFailedException extends RuntimeException {
    public StockCommitFailedException(String message) {
        super(message);
    }

    public StockCommitFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
