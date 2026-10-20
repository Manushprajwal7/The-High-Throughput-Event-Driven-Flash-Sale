package com.flashsale.common.exception;

import java.util.UUID;

public class SaleNotOpenException extends FlashSaleException {
    public SaleNotOpenException(UUID saleId, String currentStatus) {
        super("SALE_NOT_OPEN", "Sale " + saleId + " is not open for purchase (current status: " + currentStatus + ")", 409);
    }

    public SaleNotOpenException(String message) {
        super("SALE_NOT_OPEN", message, 409);
    }
}
