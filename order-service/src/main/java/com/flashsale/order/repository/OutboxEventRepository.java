package com.flashsale.order.repository;

import com.flashsale.order.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {
    List<OutboxEventEntity> findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
    long countByPublishedAtIsNull();
}
