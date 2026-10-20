package com.flashsale.common.exception;

import java.util.UUID;

public class OrderNotFoundException extends FlashSaleException {
    public OrderNotFoundException(UUID orderId) {
        super("ORDER_NOT_FOUND", "Order with ID " + orderId + " not found", 404);
    }
}
