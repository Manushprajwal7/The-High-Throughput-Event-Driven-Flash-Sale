package com.flashsale.common.dto;

import java.time.Instant;
import java.util.UUID;

public record OrderReservedPayload(
    UUID orderId,
    UUID saleId,
    String shopperId,
    int quantity,
    UUID reservationId,
    Instant expiresAt
) {}
