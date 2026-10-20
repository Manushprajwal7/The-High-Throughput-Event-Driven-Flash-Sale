package com.flashsale.payment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class ProcessedEventId implements Serializable {

    @Column(name = "consumer_name", nullable = false, length = 64)
    private String consumerName;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    public ProcessedEventId() {}

    public ProcessedEventId(String consumerName, UUID eventId) {
        this.consumerName = consumerName;
        this.eventId = eventId;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public UUID getEventId() {
        return eventId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProcessedEventId that)) return false;
        return Objects.equals(consumerName, that.consumerName) && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerName, eventId);
    }
}
