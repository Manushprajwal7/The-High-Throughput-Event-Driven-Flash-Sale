package com.flashsale.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashsale.common.dto.*;
import com.flashsale.common.exception.IdempotencyConflictException;
import com.flashsale.common.exception.OrderNotFoundException;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.order.client.InventoryClient;
import com.flashsale.order.entity.OrderEntity;
import com.flashsale.order.entity.OutboxEventEntity;
import com.flashsale.order.entity.ReservationEntity;
import com.flashsale.order.repository.OrderRepository;
import com.flashsale.order.repository.ReservationRepository;
import com.flashsale.order.statemachine.OrderStateMachine;
import com.flashsale.order.util.RequestFingerprintUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final ReservationRepository reservationRepository;
    private final InventoryClient inventoryClient;
    private final OrderPersistenceHelper orderPersistenceHelper;
    private final ObjectMapper objectMapper;

    private final Counter ordersCreatedCounter;
    private final Counter idempotencyHitCounter;
    private final Counter idempotencyConflictCounter;

    public OrderService(
        OrderRepository orderRepository,
        ReservationRepository reservationRepository,
        InventoryClient inventoryClient,
        OrderPersistenceHelper orderPersistenceHelper,
        ObjectMapper objectMapper
    ) {
        this(orderRepository, reservationRepository, inventoryClient, orderPersistenceHelper, objectMapper, new SimpleMeterRegistry());
    }

    @Autowired
    public OrderService(
        OrderRepository orderRepository,
        ReservationRepository reservationRepository,
        InventoryClient inventoryClient,
        OrderPersistenceHelper orderPersistenceHelper,
        ObjectMapper objectMapper,
        MeterRegistry meterRegistry
    ) {
        this.orderRepository = orderRepository;
        this.reservationRepository = reservationRepository;
        this.inventoryClient = inventoryClient;
        this.orderPersistenceHelper = orderPersistenceHelper;
        this.objectMapper = objectMapper;
        this.ordersCreatedCounter = meterRegistry.counter("flashsale.orders.created");
        this.idempotencyHitCounter = meterRegistry.counter("flashsale.orders.idempotency", "result", "hit");
        this.idempotencyConflictCounter = meterRegistry.counter("flashsale.orders.idempotency", "result", "conflict");
    }

    public OrderResponse createOrder(String idempotencyKey, CreateOrderRequest request, String correlationId) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("Idempotency-Key header is required");
        }

        String fingerprint = RequestFingerprintUtils.computeFingerprint(request);

        // 1. Check existing order by idempotency key
        Optional<OrderEntity> existingOpt = orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(
            request.saleId(),
            request.shopperId(),
            idempotencyKey
        );

        if (existingOpt.isPresent()) {
            OrderEntity existing = existingOpt.get();
            if (!existing.getRequestFingerprint().equals(fingerprint)) {
                idempotencyConflictCounter.increment();
                log.warn("Idempotency conflict for key {}: fingerprint mismatch", idempotencyKey);
                throw new IdempotencyConflictException("Idempotency key has already been used with a different request payload");
            }
            idempotencyHitCounter.increment();
            log.info("Idempotent replay for key {}, orderId: {}", idempotencyKey, existing.getId());
            return mapToOrderResponse(existing, "Order retrieved from existing idempotent submission");
        }

        // 2. Reserve stock in Inventory Service (Redis)
        ReservationResponse reservation = inventoryClient.reserveStock(
            request.saleId(),
            request.shopperId(),
            request.quantity(),
            correlationId
        );

        UUID orderId = UUID.randomUUID();
        OrderEntity orderEntity = new OrderEntity(
            orderId,
            request.saleId(),
            request.shopperId(),
            idempotencyKey,
            fingerprint,
            reservation.reservationId(),
            request.quantity(),
            OrderStatus.PENDING
        );

        ReservationEntity reservationEntity = new ReservationEntity(
            reservation.reservationId(),
            request.saleId(),
            orderId,
            request.quantity(),
            ReservationStatus.ACTIVE,
            reservation.expiresAt()
        );

        // 3. Build Transactional Outbox Event
        OrderReservedPayload payload = new OrderReservedPayload(
            orderId,
            request.saleId(),
            request.shopperId(),
            request.quantity(),
            reservation.reservationId(),
            reservation.expiresAt()
        );

        OrderEventEnvelope<OrderReservedPayload> envelope = OrderEventEnvelope.create(
            "ORDER",
            orderId.toString(),
            "ORDER_RESERVED",
            correlationId,
            payload
        );

        String jsonPayload;
        try {
            jsonPayload = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox event payload", e);
        }

        OutboxEventEntity outboxEvent = new OutboxEventEntity(
            envelope.eventId(),
            envelope.aggregateType(),
            envelope.aggregateId(),
            envelope.eventType(),
            envelope.schemaVersion(),
            jsonPayload
        );

        try {
            // Commit Order + Reservation + Outbox Event in a single ACID transaction!
            orderPersistenceHelper.persistOrderAndReservation(orderEntity, reservationEntity, outboxEvent);
            ordersCreatedCounter.increment();

            log.info("Persisted order {} with status PENDING, resId: {}, outboxId: {}", 
                orderId, reservation.reservationId(), outboxEvent.getId());
            return mapToOrderResponse(orderEntity, "Order accepted for processing");
        } catch (DataIntegrityViolationException ex) {
            // Race condition: another thread committed with the same key
            log.warn("Concurrent duplicate submission detected for key {}. Reverting reservation {}.", idempotencyKey, reservation.reservationId());
            inventoryClient.releaseStock(request.saleId(), reservation.reservationId(), "CONCURRENT_IDEMPOTENCY_RACE", correlationId);

            OrderEntity winner = findWinnerOrderWithRetry(request.saleId(), request.shopperId(), idempotencyKey);

            if (!winner.getRequestFingerprint().equals(fingerprint)) {
                idempotencyConflictCounter.increment();
                throw new IdempotencyConflictException("Idempotency key has already been used with a different request payload");
            }
            idempotencyHitCounter.increment();
            return mapToOrderResponse(winner, "Order retrieved from concurrent idempotent submission");
        }
    }

    private OrderEntity findWinnerOrderWithRetry(UUID saleId, String shopperId, String idempotencyKey) {
        for (int i = 0; i < 10; i++) {
            Optional<OrderEntity> opt = orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(saleId, shopperId, idempotencyKey);
            if (opt.isPresent()) {
                return opt.get();
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(saleId, shopperId, idempotencyKey)
            .orElseThrow(() -> new IllegalStateException("Winner order not found after concurrency collision"));
    }

    @Transactional(readOnly = true)
    public OrderDetailsResponse getOrder(UUID orderId) {
        OrderEntity order = orderRepository.findById(orderId)
            .orElseThrow(() -> new OrderNotFoundException(orderId));

        return new OrderDetailsResponse(
            order.getId(),
            order.getSaleId(),
            order.getShopperId(),
            order.getQuantity(),
            order.getStatus(),
            order.getReservationId(),
            order.getPaymentReference(),
            order.getFailureCode(),
            order.getCreatedAt(),
            order.getUpdatedAt()
        );
    }

    @Transactional
    public CancelOrderResponse cancelOrder(UUID orderId, String reason, String correlationId) {
        OrderEntity order = orderRepository.findById(orderId)
            .orElseThrow(() -> new OrderNotFoundException(orderId));

        OrderStateMachine.validateTransition(order.getStatus(), OrderStatus.CANCELLED);

        // Compensating release in Inventory Service
        inventoryClient.releaseStock(order.getSaleId(), order.getReservationId(), reason != null ? reason : "USER_CANCELLED", correlationId);

        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);

        reservationRepository.findById(order.getReservationId()).ifPresent(res -> {
            res.setStatus(ReservationStatus.RELEASED);
            reservationRepository.save(res);
        });

        log.info("Order {} successfully cancelled and stock released", orderId);
        return new CancelOrderResponse(orderId, OrderStatus.CANCELLED, "Order successfully cancelled and stock released");
    }

    private OrderResponse mapToOrderResponse(OrderEntity entity, String message) {
        return new OrderResponse(
            entity.getId(),
            entity.getSaleId(),
            entity.getShopperId(),
            entity.getQuantity(),
            entity.getStatus(),
            entity.getReservationId(),
            entity.getCreatedAt(),
            message
        );
    }
}
