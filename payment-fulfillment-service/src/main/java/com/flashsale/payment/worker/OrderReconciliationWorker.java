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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

@Component
public class OrderReconciliationWorker {

    private static final Logger log = LoggerFactory.getLogger(OrderReconciliationWorker.class);

    private final OrderRepository orderRepository;
    private final ReservationRepository reservationRepository;
    private final InventoryClient inventoryClient;
    private final PaymentSimulationService paymentSimulationService;

    @Value("${flashsale.reconciliation.enabled:true}")
    private boolean reconciliationEnabled = true;

    @Value("${flashsale.reconciliation.stale-cutoff-minutes:5}")
    private int staleCutoffMinutes = 5;

    public OrderReconciliationWorker(
        OrderRepository orderRepository,
        ReservationRepository reservationRepository,
        InventoryClient inventoryClient
    ) {
        this(orderRepository, reservationRepository, inventoryClient, new PaymentSimulationService("ALWAYS_SUCCESS", 0));
    }

    @Autowired
    public OrderReconciliationWorker(
        OrderRepository orderRepository,
        ReservationRepository reservationRepository,
        InventoryClient inventoryClient,
        PaymentSimulationService paymentSimulationService
    ) {
        this.orderRepository = orderRepository;
        this.reservationRepository = reservationRepository;
        this.inventoryClient = inventoryClient;
        this.paymentSimulationService = paymentSimulationService;
    }

    public void setReconciliationEnabled(boolean reconciliationEnabled) {
        this.reconciliationEnabled = reconciliationEnabled;
    }

    public boolean isReconciliationEnabled() {
        return reconciliationEnabled;
    }

    public void setStaleCutoffMinutes(int staleCutoffMinutes) {
        this.staleCutoffMinutes = staleCutoffMinutes;
    }

    @Scheduled(fixedDelayString = "${flashsale.reconciliation.interval-ms:30000}")
    public void scheduledReconciliation() {
        if (!reconciliationEnabled) {
            return;
        }
        reconcileStaleOrdersAndStateMismatches();
    }

    @Transactional
    public int reconcileStaleOrdersAndStateMismatches() {
        int repairedCount = 0;
        Instant cutoff = Instant.now().minus(staleCutoffMinutes, ChronoUnit.MINUTES);

        // 1. Repair stale PENDING orders whose event processing never completed
        List<OrderEntity> staleOrders = orderRepository.findByStatusAndCreatedAtBefore(OrderStatus.PENDING, cutoff);
        for (OrderEntity order : staleOrders) {
            log.warn("Reconciling stale PENDING order {} created at {}", order.getId(), order.getCreatedAt());

            Optional<ReservationEntity> resOpt = reservationRepository.findById(order.getReservationId());

            // Check if payment was captured before expiring
            if (order.getPaymentReference() != null || paymentSimulationService.isPaymentCaptured(order.getId())) {
                log.warn("Reconciling stale order {}: payment was captured! Auto-confirming.", order.getId());
                if (order.getStatus() == OrderStatus.PENDING) {
                    order.transitionTo(OrderStatus.PROCESSING);
                }
                order.transitionTo(OrderStatus.CONFIRMED);
                if (order.getPaymentReference() == null) {
                    order.setPaymentReference(paymentSimulationService.getPaymentReference(order.getId()));
                }
                orderRepository.save(order);
                if (resOpt.isPresent()) {
                    ReservationEntity res = resOpt.get();
                    res.setStatus(ReservationStatus.COMMITTED);
                    reservationRepository.save(res);
                }
                inventoryClient.commitStock(order.getSaleId(), order.getReservationId(), "reconciliation-worker");
                repairedCount++;
                continue;
            }

            order.transitionTo(OrderStatus.EXPIRED);
            order.setFailureCode("RECONCILIATION_STALE_PENDING");
            orderRepository.save(order);

            if (resOpt.isPresent()) {
                ReservationEntity res = resOpt.get();
                if (res.getStatus() == ReservationStatus.ACTIVE) {
                    res.setStatus(ReservationStatus.EXPIRED);
                    reservationRepository.save(res);
                }
            }

            inventoryClient.releaseStock(
                order.getSaleId(),
                order.getReservationId(),
                "RECONCILIATION_STALE_PENDING",
                "reconciliation-worker"
            );
            repairedCount++;
        }

        // 2. Check and fix state mismatches between orders and reservations
        List<ReservationEntity> activeReservations = reservationRepository.findAll();
        for (ReservationEntity res : activeReservations) {
            if (res.getStatus() != ReservationStatus.ACTIVE || res.getOrderId() == null) {
                continue;
            }

            Optional<OrderEntity> orderOpt = orderRepository.findById(res.getOrderId());
            if (orderOpt.isEmpty()) {
                continue;
            }

            OrderEntity order = orderOpt.get();

            if (order.getStatus() == OrderStatus.CONFIRMED) {
                log.warn("Fixing mismatch: reservation {} was ACTIVE for CONFIRMED order {}", res.getId(), order.getId());
                res.setStatus(ReservationStatus.COMMITTED);
                reservationRepository.save(res);
                inventoryClient.commitStock(res.getSaleId(), res.getId(), "reconciliation-worker");
                repairedCount++;
            } else if (order.getStatus() == OrderStatus.PAYMENT_FAILED ||
                       order.getStatus() == OrderStatus.EXPIRED ||
                       order.getStatus() == OrderStatus.CANCELLED) {
                log.warn("Fixing mismatch: reservation {} was ACTIVE for failed/expired order {}", res.getId(), order.getId());
                res.setStatus(ReservationStatus.RELEASED);
                reservationRepository.save(res);
                inventoryClient.releaseStock(res.getSaleId(), res.getId(), "RECONCILIATION_MISMATCH_RELEASE", "reconciliation-worker");
                repairedCount++;
            }
        }

        if (repairedCount > 0) {
            log.info("Reconciliation worker completed with {} repairs applied", repairedCount);
        }

        return repairedCount;
    }
}
