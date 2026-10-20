package com.flashsale.order.repository;

import com.flashsale.order.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
    Optional<OrderEntity> findBySaleIdAndShopperIdAndIdempotencyKey(UUID saleId, String shopperId, String idempotencyKey);
}
