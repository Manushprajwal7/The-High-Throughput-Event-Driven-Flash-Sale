package com.flashsale.common.dto;

public record ReleaseReservationRequest(
    String reason
) {
    public ReleaseReservationRequest {
        if (reason == null || reason.isBlank()) {
            reason = "ORDER_CANCELLED_OR_EXPIRED";
        }
    }
}
