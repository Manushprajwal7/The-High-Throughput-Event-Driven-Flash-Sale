package com.flashsale.common.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ReserveStockRequest(
    @NotBlank(message = "Shopper ID is required")
    String shopperId,

    @Min(value = 1, message = "Quantity must be at least 1")
    @Max(value = 10, message = "Maximum 10 units per reservation")
    Integer quantity,

    @Min(value = 10, message = "Reservation TTL must be at least 10 seconds")
    @Max(value = 600, message = "Reservation TTL cannot exceed 600 seconds")
    Integer reservationTtlSeconds
) {
    public ReserveStockRequest {
        if (quantity == null) {
            quantity = 1;
        }
        if (reservationTtlSeconds == null) {
            reservationTtlSeconds = 120;
        }
    }
}
