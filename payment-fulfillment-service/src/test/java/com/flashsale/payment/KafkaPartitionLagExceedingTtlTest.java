package com.flashsale.payment;

import com.flashsale.common.dto.OrderEventEnvelope;
import com.flashsale.common.dto.OrderReservedPayload;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.client.InventoryClient;
import com.flashsale.payment.entity.OrderEntity;
import com.flashsale.payment.entity.ReservationEntity;
import com.flashsale.payment.repository.OrderRepository;
import com.flashsale.payment.repository.ProcessedEventRepository;
import com.flashsale.payment.repository.ReservationRepository;
import com.flashsale.payment.service.FulfillmentService;
import com.flashsale.payment.simulation.PaymentSimulationService;
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
 * Proves fix for Scenario 12 (REM-09):
 * Consumer lag in Kafka causes event delivery after reservation TTL has already expired.
 * The consumer detects the expired reservation, rejects payment, marks order EXPIRED,
 * and triggers compensating release of inventory.
 */
@SpringBootTest
@ActiveProfiles("test")
class KafkaPartitionLagExceedingTtlTest {

    @Autowired
    private FulfillmentService fulfillmentService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @MockBean
    private InventoryClient inventoryClient;

    @MockBean
    private PaymentSimulationService paymentSimulationService;

    @BeforeEach
    void cleanUp() {
        orderRepository.deleteAll();
        reservationRepository.deleteAll();
        processedEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Scenario 12 (REM-09): Lagged Kafka event arriving past TTL must not execute payment and must mark order EXPIRED")
    void laggedKafkaEventArrivingPastTtlMarksOrderExpiredAndReleasesStock() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        String shopperId = "shopper_lagged";
        int quantity = 2;

        // Reservation expired 60 seconds ago due to partition lag
        Instant expiredAt = Instant.now().minusSeconds(60);

        OrderEntity order = new OrderEntity(
            orderId, saleId, shopperId, "idem-lag-key", "fingerprint", reservationId, quantity
        );
        orderRepository.save(order);

        ReservationEntity reservation = new ReservationEntity(
            reservationId, saleId, orderId, quantity, expiredAt
        );
        reservationRepository.save(reservation);

        OrderReservedPayload payload = new OrderReservedPayload(
            orderId, saleId, shopperId, quantity, reservationId, expiredAt
        );
        OrderEventEnvelope<OrderReservedPayload> envelope = OrderEventEnvelope.create(
            "ORDER", orderId.toString(), "ORDER_RESERVED", "corr-lag-test", payload
        );

        fulfillmentService.processOrderReserved(envelope);

        // INVARIANT 1: Payment processing was NEVER called
        verify(paymentSimulationService, never()).processPayment(any(), any(), anyInt());

        // INVARIANT 2: Order is marked EXPIRED with failure code RESERVATION_EXPIRED
        OrderEntity updatedOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(updatedOrder.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(updatedOrder.getFailureCode()).isEqualTo("RESERVATION_EXPIRED");

        // INVARIANT 3: Reservation is marked EXPIRED
        ReservationEntity updatedRes = reservationRepository.findById(reservationId).orElseThrow();
        assertThat(updatedRes.getStatus()).isEqualTo(ReservationStatus.EXPIRED);

        // INVARIANT 4: Compensating release was sent to Inventory Service
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(reservationId), eq("RESERVATION_EXPIRED"), any());
    }
}
