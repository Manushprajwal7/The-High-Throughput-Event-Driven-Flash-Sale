package com.flashsale.common.exception;

import java.util.UUID;

public class SoldOutException extends FlashSaleException {
    public SoldOutException(UUID saleId) {
        super("SOLD_OUT", "Sale " + saleId + " is completely sold out or has insufficient stock", 409);
    }

    public SoldOutException(String message) {
        super("SOLD_OUT", message, 409);
    }
}
