package com.flashsale.common.dto;

import java.time.Instant;
import java.util.UUID;

public record OrderConfirmedPayload(
    UUID orderId,
    UUID saleId,
    UUID reservationId,
    String paymentReference,
    Instant confirmedAt
) {}
