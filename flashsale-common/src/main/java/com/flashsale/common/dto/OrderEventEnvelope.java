package com.flashsale.common.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Standard envelope for all events published to Apache Kafka.
 */
public record OrderEventEnvelope<T>(
    UUID eventId,
    String aggregateType,
    String aggregateId,
    String eventType,
    int schemaVersion,
    Instant timestamp,
    String correlationId,
    T payload
) {
    public static <T> OrderEventEnvelope<T> create(
        String aggregateType,
        String aggregateId,
        String eventType,
        String correlationId,
        T payload
    ) {
        return new OrderEventEnvelope<>(
            UUID.randomUUID(),
            aggregateType,
            aggregateId,
            eventType,
            1,
            Instant.now(),
            correlationId,
            payload
        );
    }
}
