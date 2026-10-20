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
class ReservationExpiryWorkerTest {

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private InventoryClient inventoryClient;

    private ReservationExpiryWorker expiryWorker;

    @BeforeEach
    void setUp() {
        expiryWorker = new ReservationExpiryWorker(reservationRepository, orderRepository, inventoryClient);
    }

    @Test
    void shouldExpireActiveReservationsPastExpiryDate() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().minusSeconds(30));
        OrderEntity order = new OrderEntity(orderId, saleId, "shopper_1", "key_1", "fp_1", resId, 1);

        when(reservationRepository.findByStatusAndExpiresAtBefore(eq(ReservationStatus.ACTIVE), any(Instant.class)))
            .thenReturn(List.of(reservation));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        int cleaned = expiryWorker.expireOrphanedReservations();

        assertThat(cleaned).isEqualTo(1);
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.EXPIRED);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(order.getFailureCode()).isEqualTo("EXPIRED_TTL");

        verify(reservationRepository, times(1)).save(reservation);
        verify(orderRepository, times(1)).save(order);
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("EXPIRED_TTL"), eq("reservation-expiry-worker"));
    }

    @Test
    void shouldReturnZeroWhenNoReservationsExpired() {
        when(reservationRepository.findByStatusAndExpiresAtBefore(eq(ReservationStatus.ACTIVE), any(Instant.class)))
            .thenReturn(List.of());

        int cleaned = expiryWorker.expireOrphanedReservations();

        assertThat(cleaned).isEqualTo(0);
        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }

    @Test
    void shouldAutoConfirmOrderIfPaymentWasCapturedBeforeExpiry() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().minusSeconds(30));
        OrderEntity order = new OrderEntity(orderId, saleId, "shopper_paid", "key_paid", "fp_paid", resId, 1);
        order.setPaymentReference("PAY-SIM-CAPTURED-999"); // Payment succeeded before crash

        when(reservationRepository.findByStatusAndExpiresAtBefore(eq(ReservationStatus.ACTIVE), any(Instant.class)))
            .thenReturn(List.of(reservation));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        int cleaned = expiryWorker.expireOrphanedReservations();

        assertThat(cleaned).isEqualTo(1);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.COMMITTED);

        verify(orderRepository, times(1)).save(order);
        verify(reservationRepository, times(1)).save(reservation);
        verify(inventoryClient, times(1)).commitStock(eq(saleId), eq(resId), eq("expiry-worker-payment-recovery"));
        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }
}
