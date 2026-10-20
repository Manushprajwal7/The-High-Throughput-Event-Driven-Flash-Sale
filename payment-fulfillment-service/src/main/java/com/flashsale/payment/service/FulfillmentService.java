package com.flashsale.payment.service;

import com.flashsale.common.dto.OrderEventEnvelope;
import com.flashsale.common.dto.OrderReservedPayload;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.client.InventoryClient;
import com.flashsale.payment.entity.OrderEntity;
import com.flashsale.payment.entity.ProcessedEventEntity;
import com.flashsale.payment.entity.ReservationEntity;
import com.flashsale.payment.repository.OrderRepository;
import com.flashsale.payment.repository.ProcessedEventRepository;
import com.flashsale.payment.repository.ReservationRepository;
import com.flashsale.payment.simulation.PaymentResult;
import com.flashsale.payment.simulation.PaymentSimulationService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class FulfillmentService {

    private static final Logger log = LoggerFactory.getLogger(FulfillmentService.class);
    private static final String CONSUMER_NAME = "payment-fulfillment-service";

    private final OrderRepository orderRepository;
    private final ReservationRepository reservationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final PaymentSimulationService paymentSimulationService;
    private final InventoryClient inventoryClient;

    private final Counter confirmedCounter;
    private final Counter declinedCounter;
    private final Counter expiredCounter;
    private final Counter deduplicatedCounter;

    public FulfillmentService(
        OrderRepository orderRepository,
        ReservationRepository reservationRepository,
        ProcessedEventRepository processedEventRepository,
        PaymentSimulationService paymentSimulationService,
        InventoryClient inventoryClient
    ) {
        this(orderRepository, reservationRepository, processedEventRepository, paymentSimulationService, inventoryClient, new SimpleMeterRegistry());
    }

    @Autowired
    public FulfillmentService(
        OrderRepository orderRepository,
        ReservationRepository reservationRepository,
        ProcessedEventRepository processedEventRepository,
        PaymentSimulationService paymentSimulationService,
        InventoryClient inventoryClient,
        MeterRegistry meterRegistry
    ) {
        this.orderRepository = orderRepository;
        this.reservationRepository = reservationRepository;
        this.processedEventRepository = processedEventRepository;
        this.paymentSimulationService = paymentSimulationService;
        this.inventoryClient = inventoryClient;
        this.confirmedCounter = meterRegistry.counter("flashsale.fulfillment.outcomes", "outcome", "confirmed");
        this.declinedCounter = meterRegistry.counter("flashsale.fulfillment.outcomes", "outcome", "declined");
        this.expiredCounter = meterRegistry.counter("flashsale.fulfillment.outcomes", "outcome", "expired");
        this.deduplicatedCounter = meterRegistry.counter("flashsale.fulfillment.outcomes", "outcome", "deduplicated");
    }

    @Transactional
    public void processOrderReserved(OrderEventEnvelope<OrderReservedPayload> envelope) {
        UUID eventId = envelope.eventId();
        String correlationId = envelope.correlationId();
        OrderReservedPayload payload = envelope.payload();

        UUID orderId = payload.orderId();
        UUID saleId = payload.saleId();
        UUID reservationId = payload.reservationId();
        String shopperId = payload.shopperId();
        int quantity = payload.quantity();
        Instant expiresAt = payload.expiresAt();

        // 1. Consumer Idempotency Check
        if (processedEventRepository.isEventProcessed(CONSUMER_NAME, eventId)) {
            deduplicatedCounter.increment();
            log.info("Duplicate event {} already processed for order {}. Skipping.", eventId, orderId);
            return;
        }

        // 2. Fetch order state with row lock to eliminate race conditions
        Optional<OrderEntity> orderOpt = orderRepository.findByIdForUpdate(orderId);
        if (orderOpt == null || orderOpt.isEmpty()) {
            orderOpt = orderRepository.findById(orderId);
        }
        if (orderOpt == null || orderOpt.isEmpty()) {
            log.error("Order {} not found in database for event {}. Cannot fulfill.", orderId, eventId);
            processedEventRepository.save(new ProcessedEventEntity(CONSUMER_NAME, eventId));
            return;
        }

        OrderEntity order = orderOpt.get();
        OrderStatus currentStatus = order.getStatus();

        // 3. Skip if already terminal or already in processing
        if (isTerminal(currentStatus) || currentStatus == OrderStatus.PROCESSING) {
            deduplicatedCounter.increment();
            log.info("Order {} is in state {}. Skipping event {}.", orderId, currentStatus, eventId);
            processedEventRepository.save(new ProcessedEventEntity(CONSUMER_NAME, eventId));
            return;
        }

        Optional<ReservationEntity> resOpt = reservationRepository.findById(reservationId);

        // 4. Reservation Expiry Check
        boolean isExpired = (expiresAt != null && expiresAt.isBefore(Instant.now())) ||
            (resOpt.isPresent() && (resOpt.get().getStatus() == ReservationStatus.EXPIRED || resOpt.get().getExpiresAt().isBefore(Instant.now())));

        if (isExpired) {
            expiredCounter.increment();
            log.warn("Reservation {} for order {} has expired. Executing compensating release.", reservationId, orderId);
            order.transitionTo(OrderStatus.EXPIRED);
            order.setFailureCode("RESERVATION_EXPIRED");
            orderRepository.save(order);

            resOpt.ifPresent(res -> {
                res.setStatus(ReservationStatus.EXPIRED);
                reservationRepository.save(res);
            });

            inventoryClient.releaseStock(saleId, reservationId, "RESERVATION_EXPIRED", correlationId);
            processedEventRepository.save(new ProcessedEventEntity(CONSUMER_NAME, eventId));
            return;
        }

        // 5. Advance to PROCESSING
        order.transitionTo(OrderStatus.PROCESSING);
        orderRepository.save(order);

        // 6. Simulate Payment (Transient errors throw PaymentTimeoutException)
        PaymentResult paymentResult = paymentSimulationService.processPayment(orderId, shopperId, quantity);

        // 7. Saga Resolution based on Payment Outcome
        if (paymentResult.successful()) {
            try {
                // Must atomically commit in Redis FIRST before confirming order in DB
                inventoryClient.commitStock(saleId, reservationId, correlationId);

                confirmedCounter.increment();
                order.transitionTo(OrderStatus.CONFIRMED);
                order.setPaymentReference(paymentResult.paymentReference());
                orderRepository.save(order);

                resOpt.ifPresent(res -> {
                    res.setStatus(ReservationStatus.COMMITTED);
                    reservationRepository.save(res);
                });

                processedEventRepository.save(new ProcessedEventEntity(CONSUMER_NAME, eventId));
                log.info("Order {} successfully CONFIRMED with payment ref: {}", orderId, paymentResult.paymentReference());
            } catch (Exception ex) {
                log.error("CRITICAL: Failed to commit stock for order {} (resId={}). Triggering Compensating Refund Saga!",
                    orderId, reservationId, ex);

                // Compensating Refund Saga: refund captured payment to protect customer funds
                paymentSimulationService.refundPayment(paymentResult.paymentReference());

                order.transitionTo(OrderStatus.EXPIRED);
                order.setFailureCode("RESERVATION_EXPIRED_DURING_PAYMENT");
                orderRepository.save(order);

                resOpt.ifPresent(res -> {
                    res.setStatus(ReservationStatus.EXPIRED);
                    reservationRepository.save(res);
                });

                processedEventRepository.save(new ProcessedEventEntity(CONSUMER_NAME, eventId));
            }
        } else {
            declinedCounter.increment();
            String failureCode = paymentResult.failureCode() != null ? paymentResult.failureCode() : "PAYMENT_DECLINED";
            order.transitionTo(OrderStatus.PAYMENT_FAILED);
            order.setFailureCode(failureCode);
            orderRepository.save(order);

            resOpt.ifPresent(res -> {
                res.setStatus(ReservationStatus.RELEASED);
                reservationRepository.save(res);
            });

            // Compensating Stock Release (Zero overselling / inventory restored)
            inventoryClient.releaseStock(saleId, reservationId, failureCode, correlationId);
            processedEventRepository.save(new ProcessedEventEntity(CONSUMER_NAME, eventId));

            log.warn("Order {} transitioned to PAYMENT_FAILED (code: {}). Compensating stock release triggered.",
                orderId, failureCode);
        }
    }

    private boolean isTerminal(OrderStatus status) {
        return status == OrderStatus.CONFIRMED ||
            status == OrderStatus.PAYMENT_FAILED ||
            status == OrderStatus.EXPIRED ||
            status == OrderStatus.CANCELLED;
    }
}
