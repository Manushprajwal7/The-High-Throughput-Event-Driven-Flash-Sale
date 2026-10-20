package com.flashsale.payment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
public class ProcessedEventEntity {

    @EmbeddedId
    private ProcessedEventId id;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    public ProcessedEventEntity() {}

    public ProcessedEventEntity(String consumerName, UUID eventId) {
        this.id = new ProcessedEventId(consumerName, eventId);
        this.processedAt = Instant.now();
    }

    public ProcessedEventId getId() {
        return id;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
