package com.flashsale.payment.worker;

import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.client.InventoryClient;
import com.flashsale.payment.entity.OrderEntity;
import com.flashsale.payment.entity.ReservationEntity;
import com.flashsale.payment.repository.OrderRepository;
import com.flashsale.payment.repository.ReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderReconciliationWorkerTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private InventoryClient inventoryClient;

    private OrderReconciliationWorker reconciliationWorker;

    @BeforeEach
    void setUp() {
        reconciliationWorker = new OrderReconciliationWorker(orderRepository, reservationRepository, inventoryClient);
    }

    @Test
    void shouldReconcileStalePendingOrders() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        OrderEntity staleOrder = new OrderEntity(orderId, saleId, "shopper_stale", "key_stale", "fp_stale", resId, 1);
        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().minusSeconds(100));

        when(orderRepository.findByStatusAndCreatedAtBefore(eq(OrderStatus.PENDING), any(Instant.class)))
            .thenReturn(List.of(staleOrder));
        when(reservationRepository.findById(resId)).thenReturn(Optional.of(reservation));
        when(reservationRepository.findAll()).thenReturn(List.of());

        int repaired = reconciliationWorker.reconcileStaleOrdersAndStateMismatches();

        assertThat(repaired).isEqualTo(1);
        assertThat(staleOrder.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(staleOrder.getFailureCode()).isEqualTo("RECONCILIATION_STALE_PENDING");
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.EXPIRED);

        verify(orderRepository, times(1)).save(staleOrder);
        verify(reservationRepository, times(1)).save(reservation);
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("RECONCILIATION_STALE_PENDING"), eq("reconciliation-worker"));
    }

    @Test
    void shouldFixMismatchWhenReservationIsActiveForConfirmedOrder() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        OrderEntity confirmedOrder = new OrderEntity(orderId, saleId, "shopper_conf", "key_conf", "fp_conf", resId, 1);
        confirmedOrder.transitionTo(OrderStatus.PROCESSING);
        confirmedOrder.transitionTo(OrderStatus.CONFIRMED);

        // Inconsistency: order is confirmed, but reservation was still ACTIVE
        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(60));

        when(orderRepository.findByStatusAndCreatedAtBefore(eq(OrderStatus.PENDING), any(Instant.class)))
            .thenReturn(List.of());
        when(reservationRepository.findAll()).thenReturn(List.of(reservation));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(confirmedOrder));

        int repaired = reconciliationWorker.reconcileStaleOrdersAndStateMismatches();

        assertThat(repaired).isEqualTo(1);
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.COMMITTED);

        verify(reservationRepository, times(1)).save(reservation);
        verify(inventoryClient, times(1)).commitStock(eq(saleId), eq(resId), eq("reconciliation-worker"));
    }

    @Test
    void shouldFixMismatchWhenReservationIsActiveForFailedOrder() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        OrderEntity failedOrder = new OrderEntity(orderId, saleId, "shopper_failed", "key_fail", "fp_fail", resId, 1);
        failedOrder.transitionTo(OrderStatus.PAYMENT_FAILED);

        // Inconsistency: order failed, but reservation was still ACTIVE
        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(60));

        when(orderRepository.findByStatusAndCreatedAtBefore(eq(OrderStatus.PENDING), any(Instant.class)))
            .thenReturn(List.of());
        when(reservationRepository.findAll()).thenReturn(List.of(reservation));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(failedOrder));

        int repaired = reconciliationWorker.reconcileStaleOrdersAndStateMismatches();

        assertThat(repaired).isEqualTo(1);
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RELEASED);

        verify(reservationRepository, times(1)).save(reservation);
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("RECONCILIATION_MISMATCH_RELEASE"), eq("reconciliation-worker"));
    }

    @Test
    void shouldAutoConfirmStalePendingOrderIfPaymentWasCaptured() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        OrderEntity paidStaleOrder = new OrderEntity(orderId, saleId, "shopper_paid_stale", "key_ps", "fp_ps", resId, 1);
        paidStaleOrder.setPaymentReference("PAY-SIM-STALE-OK"); // Payment succeeded before crash

        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().minusSeconds(100));

        when(orderRepository.findByStatusAndCreatedAtBefore(eq(OrderStatus.PENDING), any(Instant.class)))
            .thenReturn(List.of(paidStaleOrder));
        when(reservationRepository.findById(resId)).thenReturn(Optional.of(reservation));
        when(reservationRepository.findAll()).thenReturn(List.of());

        int repaired = reconciliationWorker.reconcileStaleOrdersAndStateMismatches();

        assertThat(repaired).isEqualTo(1);
        assertThat(paidStaleOrder.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.COMMITTED);

        verify(orderRepository, times(1)).save(paidStaleOrder);
        verify(reservationRepository, times(1)).save(reservation);
        verify(inventoryClient, times(1)).commitStock(eq(saleId), eq(resId), eq("reconciliation-worker"));
        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }
}
