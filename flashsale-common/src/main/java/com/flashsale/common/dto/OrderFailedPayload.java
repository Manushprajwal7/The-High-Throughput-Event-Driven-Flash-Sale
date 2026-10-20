package com.flashsale.common.dto;

import java.time.Instant;
import java.util.UUID;

public record OrderFailedPayload(
    UUID orderId,
    UUID saleId,
    UUID reservationId,
    String failureCode,
    String reason,
    Instant failedAt
) {}
