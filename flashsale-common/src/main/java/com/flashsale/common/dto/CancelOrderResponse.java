package com.flashsale.common.dto;

import com.flashsale.common.model.OrderStatus;

import java.util.UUID;

public record CancelOrderResponse(
    UUID orderId,
    OrderStatus status,
    String message
) {}
