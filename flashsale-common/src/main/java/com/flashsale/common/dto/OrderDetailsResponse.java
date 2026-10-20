package com.flashsale.common.dto;

import com.flashsale.common.model.OrderStatus;

import java.time.Instant;
import java.util.UUID;

public record OrderDetailsResponse(
    UUID orderId,
    UUID saleId,
    String shopperId,
    int quantity,
    OrderStatus status,
    UUID reservationId,
    String paymentReference,
    String failureCode,
    Instant createdAt,
    Instant updatedAt
) {}
