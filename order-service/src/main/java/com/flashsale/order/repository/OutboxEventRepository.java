package com.flashsale.order.repository;

import com.flashsale.order.entity.OutboxEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {
    @Query("SELECT e FROM OutboxEventEntity e WHERE e.publishedAt IS NULL AND (e.nextRetryAt IS NULL OR e.nextRetryAt <= :now) AND (e.status IS NULL OR e.status = 'PENDING') ORDER BY e.createdAt ASC")
    List<OutboxEventEntity> findEligibleEventsForPublishing(@Param("now") Instant now, Pageable pageable);

    List<OutboxEventEntity> findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
    long countByPublishedAtIsNull();
}
