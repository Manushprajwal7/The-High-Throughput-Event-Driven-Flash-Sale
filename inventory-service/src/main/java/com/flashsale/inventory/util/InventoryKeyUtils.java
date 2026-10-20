package com.flashsale.inventory.util;

import java.util.UUID;

public final class InventoryKeyUtils {
    private InventoryKeyUtils() {}

    public static String stockKey(UUID saleId) {
        return "flashsale:stock:" + saleId;
    }

    public static String reservationKey(UUID saleId, UUID reservationId) {
        return "flashsale:res:" + saleId + ":" + reservationId;
    }

    public static String saleKey(UUID saleId) {
        return "flashsale:sale:" + saleId;
    }

    public static String expirationsKey(UUID saleId) {
        return "flashsale:res_expirations:" + saleId;
    }

    public static String idempotencyKey(UUID saleId, String idempotencyKey) {
        return "flashsale:res_idempotency:" + saleId + ":" + idempotencyKey;
    }

    public static String salesIndexKey() {
        return "flashsale:sales:all";
    }
}
