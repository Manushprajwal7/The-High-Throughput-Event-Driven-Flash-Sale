package com.flashsale.payment.repository;

import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.entity.ReservationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ReservationRepository extends JpaRepository<ReservationEntity, UUID> {

    List<ReservationEntity> findByStatusAndExpiresAtBefore(ReservationStatus status, Instant cutoff);

    Optional<ReservationEntity> findByOrderId(UUID orderId);
}
