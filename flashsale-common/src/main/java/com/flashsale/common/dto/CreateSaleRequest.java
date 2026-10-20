package com.flashsale.common.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record CreateSaleRequest(
    @NotBlank(message = "Sale name is required")
    String name,

    @NotNull(message = "Total capacity is required")
    @Min(value = 0, message = "Total capacity must be non-negative")
    Integer totalCapacity,

    @NotNull(message = "Start time is required")
    Instant startsAt,

    @NotNull(message = "End time is required")
    Instant endsAt
) {
    public CreateSaleRequest {
        if (startsAt != null && endsAt != null && !endsAt.isAfter(startsAt)) {
            throw new IllegalArgumentException("Sale end time must be strictly after start time");
        }
    }
}
