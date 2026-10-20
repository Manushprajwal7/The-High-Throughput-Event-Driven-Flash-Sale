package com.flashsale.payment.repository;

import com.flashsale.payment.entity.ProcessedEventEntity;
import com.flashsale.payment.entity.ProcessedEventId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, ProcessedEventId> {

    default boolean isEventProcessed(String consumerName, UUID eventId) {
        return existsById(new ProcessedEventId(consumerName, eventId));
    }
}
