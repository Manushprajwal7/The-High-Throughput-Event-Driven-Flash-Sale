package com.flashsale.common.dto;

import com.flashsale.common.model.OrderStatus;

import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
    UUID orderId,
    UUID saleId,
    String shopperId,
    int quantity,
    OrderStatus status,
    UUID reservationId,
    Instant createdAt,
    String message
) {}
