package com.flashsale.common.dto;

import java.time.Instant;
import java.util.UUID;

public record ReservationResponse(
    UUID reservationId,
    UUID saleId,
    int quantity,
    Instant expiresAt
) {}
