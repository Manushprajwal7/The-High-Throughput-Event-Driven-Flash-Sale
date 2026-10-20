package com.flashsale.payment.worker;

import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.client.InventoryClient;
import com.flashsale.payment.entity.OrderEntity;
import com.flashsale.payment.entity.ReservationEntity;
import com.flashsale.payment.repository.OrderRepository;
import com.flashsale.payment.repository.ReservationRepository;
import com.flashsale.payment.simulation.PaymentSimulationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class ReservationExpiryWorker {

    private static final Logger log = LoggerFactory.getLogger(ReservationExpiryWorker.class);

    private final ReservationRepository reservationRepository;
    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;
    private final PaymentSimulationService paymentSimulationService;

    @Value("${flashsale.expiry.enabled:true}")
    private boolean expiryEnabled = true;

    public ReservationExpiryWorker(
        ReservationRepository reservationRepository,
        OrderRepository orderRepository,
        InventoryClient inventoryClient
    ) {
        this(reservationRepository, orderRepository, inventoryClient, new PaymentSimulationService("ALWAYS_SUCCESS", 0));
    }

    @Autowired
    public ReservationExpiryWorker(
        ReservationRepository reservationRepository,
        OrderRepository orderRepository,
        InventoryClient inventoryClient,
        PaymentSimulationService paymentSimulationService
    ) {
        this.reservationRepository = reservationRepository;
        this.orderRepository = orderRepository;
        this.inventoryClient = inventoryClient;
        this.paymentSimulationService = paymentSimulationService;
    }

    public void setExpiryEnabled(boolean expiryEnabled) {
        this.expiryEnabled = expiryEnabled;
    }

    public boolean isExpiryEnabled() {
        return expiryEnabled;
    }

    @Scheduled(fixedDelayString = "${flashsale.expiry.interval-ms:5000}")
    public void scheduledExpiry() {
        if (!expiryEnabled) {
            return;
        }
        expireOrphanedReservations();
    }

    @Transactional
    public int expireOrphanedReservations() {
        Instant now = Instant.now();
        List<ReservationEntity> expiredReservations =
            reservationRepository.findByStatusAndExpiresAtBefore(ReservationStatus.ACTIVE, now);

        if (expiredReservations.isEmpty()) {
            return 0;
        }

        log.info("Found {} expired active reservations to clean up", expiredReservations.size());

        for (ReservationEntity reservation : expiredReservations) {
            boolean shouldReleaseStock = true;

            if (reservation.getOrderId() != null) {
                OrderEntity order = orderRepository.findById(reservation.getOrderId()).orElse(null);
                if (order != null && (order.getStatus() == OrderStatus.PENDING || order.getStatus() == OrderStatus.PROCESSING)) {
                    // Check if payment was already captured (Scenario 06 worker crash recovery)
                    if (order.getPaymentReference() != null || paymentSimulationService.isPaymentCaptured(order.getId())) {
                        log.warn("Order {} was marked for expiry, but payment was already captured! Auto-confirming order.", order.getId());
                        if (order.getStatus() == OrderStatus.PENDING) {
                            order.transitionTo(OrderStatus.PROCESSING);
                        }
                        order.transitionTo(OrderStatus.CONFIRMED);
                        if (order.getPaymentReference() == null) {
                            order.setPaymentReference(paymentSimulationService.getPaymentReference(order.getId()));
                        }
                        orderRepository.save(order);
                        reservation.setStatus(ReservationStatus.COMMITTED);
                        reservationRepository.save(reservation);
                        inventoryClient.commitStock(reservation.getSaleId(), reservation.getId(), "expiry-worker-payment-recovery");
                        shouldReleaseStock = false;
                    } else {
                        order.transitionTo(OrderStatus.EXPIRED);
                        order.setFailureCode("EXPIRED_TTL");
                        orderRepository.save(order);
                        log.info("Expired order {} linked to expired reservation {}", order.getId(), reservation.getId());
                    }
                }
            }

            if (shouldReleaseStock) {
                reservation.setStatus(ReservationStatus.EXPIRED);
                reservationRepository.save(reservation);

                // Compensating release in Redis (idempotent)
                inventoryClient.releaseStock(
                    reservation.getSaleId(),
                    reservation.getId(),
                    "EXPIRED_TTL",
                    "reservation-expiry-worker"
                );
            }
        }

        return expiredReservations.size();
    }
}
