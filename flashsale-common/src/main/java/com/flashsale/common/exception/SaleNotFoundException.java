package com.flashsale.common.exception;

import java.util.UUID;

public class SaleNotFoundException extends FlashSaleException {
    public SaleNotFoundException(UUID saleId) {
        super("SALE_NOT_FOUND", "Sale with ID " + saleId + " not found", 404);
    }

    public SaleNotFoundException(String message) {
        super("SALE_NOT_FOUND", message, 404);
    }
}
