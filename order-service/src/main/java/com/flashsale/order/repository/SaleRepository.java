package com.flashsale.order.repository;

import com.flashsale.order.entity.SaleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface SaleRepository extends JpaRepository<SaleEntity, UUID> {
}
