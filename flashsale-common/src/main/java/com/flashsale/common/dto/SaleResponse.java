package com.flashsale.common.dto;

import com.flashsale.common.model.SaleStatus;

import java.time.Instant;
import java.util.UUID;

public record SaleResponse(
    UUID saleId,
    String name,
    int totalCapacity,
    int availableStock,
    SaleStatus status,
    Instant startsAt,
    Instant endsAt,
    Instant createdAt
) {}
