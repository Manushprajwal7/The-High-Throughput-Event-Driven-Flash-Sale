package com.flashsale.order.service;

import com.flashsale.common.model.SaleStatus;
import com.flashsale.order.entity.OrderEntity;
import com.flashsale.order.entity.OutboxEventEntity;
import com.flashsale.order.entity.ReservationEntity;
import com.flashsale.order.entity.SaleEntity;
import com.flashsale.order.repository.OrderRepository;
import com.flashsale.order.repository.OutboxEventRepository;
import com.flashsale.order.repository.ReservationRepository;
import com.flashsale.order.repository.SaleRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class OrderPersistenceHelper {

    private final OrderRepository orderRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final SaleRepository saleRepository;

    public OrderPersistenceHelper(
        OrderRepository orderRepository,
        ReservationRepository reservationRepository,
        OutboxEventRepository outboxEventRepository,
        SaleRepository saleRepository
    ) {
        this.orderRepository = orderRepository;
        this.reservationRepository = reservationRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.saleRepository = saleRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void persistOrderAndReservation(OrderEntity order, ReservationEntity reservation, OutboxEventEntity outboxEvent) {
        if (!saleRepository.existsById(order.getSaleId())) {
            SaleEntity sale = new SaleEntity(
                order.getSaleId(),
                "Flash Sale",
                1000,
                SaleStatus.OPEN,
                Instant.now().minusSeconds(3600),
                Instant.now().plusSeconds(86400)
            );
            saleRepository.saveAndFlush(sale);
        }
        orderRepository.saveAndFlush(order);
        reservationRepository.saveAndFlush(reservation);
        if (outboxEvent != null) {
            outboxEventRepository.saveAndFlush(outboxEvent);
        }
    }
}
