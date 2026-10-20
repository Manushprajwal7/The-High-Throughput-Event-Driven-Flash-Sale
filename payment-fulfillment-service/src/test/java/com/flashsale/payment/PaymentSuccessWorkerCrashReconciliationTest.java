package com.flashsale.payment;

import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.client.InventoryClient;
import com.flashsale.payment.entity.OrderEntity;
import com.flashsale.payment.entity.ReservationEntity;
import com.flashsale.payment.repository.OrderRepository;
import com.flashsale.payment.repository.ReservationRepository;
import com.flashsale.payment.simulation.PaymentSimulationMode;
import com.flashsale.payment.simulation.PaymentSimulationService;
import com.flashsale.payment.worker.ReservationExpiryWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Proves fix for Scenario 06 (REM-03):
 * When payment succeeds but the worker crashes before recording success,
 * subsequent expiry/reconciliation workers verify payment capture status before cancelling.
 * Captured payments are safely auto-confirmed rather than cancelled and resold.
 */
@SpringBootTest
@ActiveProfiles("test")
class PaymentSuccessWorkerCrashReconciliationTest {

    @Autowired
    private ReservationExpiryWorker reservationExpiryWorker;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private PaymentSimulationService paymentSimulationService;

    @MockBean
    private InventoryClient inventoryClient;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAll();
        reservationRepository.deleteAll();
        paymentSimulationService.setMode(PaymentSimulationMode.ALWAYS_SUCCESS);
    }

    @Test
    @DisplayName("Scenario 06: Should auto-confirm order and commit stock if payment was captured prior to crash")
    void shouldAutoConfirmOrderIfPaymentCapturedBeforeCrash() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        String shopperId = "shopper_crash_recovery";

        // Simulate: Order is PENDING or PROCESSING, reservation expired 10 seconds ago
        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "crash_idemp_1", "fp_1", resId, 1);
        order.transitionTo(OrderStatus.PROCESSING);

        // Pre-condition: payment was captured before the crash!
        String capturedPaymentRef = "PAY-SIM-CAPTURED-BEFORE-CRASH";
        order.setPaymentReference(capturedPaymentRef);
        orderRepository.save(order);

        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().minusSeconds(10));
        reservationRepository.save(reservation);

        // Act: Trigger Expiry Worker sweep
        int handled = reservationExpiryWorker.expireOrphanedReservations();

        // Assert: 1 reservation processed
        assertThat(handled).isEqualTo(1);

        // Invariant: Order was AUTO-CONFIRMED, not cancelled!
        OrderEntity recoveredOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(recoveredOrder.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(recoveredOrder.getPaymentReference()).isEqualTo(capturedPaymentRef);

        // Invariant: Reservation COMMITTED, stock committed
        ReservationEntity recoveredRes = reservationRepository.findById(resId).orElseThrow();
        assertThat(recoveredRes.getStatus()).isEqualTo(ReservationStatus.COMMITTED);

        // Invariant: Stock committed, NEVER released!
        verify(inventoryClient, times(1)).commitStock(eq(saleId), eq(resId), eq("expiry-worker-payment-recovery"));
        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }
}
