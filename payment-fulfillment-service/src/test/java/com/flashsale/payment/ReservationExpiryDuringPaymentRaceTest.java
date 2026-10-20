package com.flashsale.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashsale.common.constant.KafkaTopics;
import com.flashsale.common.dto.OrderEventEnvelope;
import com.flashsale.common.dto.OrderReservedPayload;
import com.flashsale.common.exception.StockCommitFailedException;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.client.InventoryClient;
import com.flashsale.payment.consumer.OrderReservedConsumer;
import com.flashsale.payment.entity.OrderEntity;
import com.flashsale.payment.entity.ReservationEntity;
import com.flashsale.payment.repository.OrderRepository;
import com.flashsale.payment.repository.ProcessedEventRepository;
import com.flashsale.payment.repository.ReservationRepository;
import com.flashsale.payment.simulation.PaymentSimulationMode;
import com.flashsale.payment.simulation.PaymentSimulationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * Proves fix for Scenario 07 (REM-01):
 * When a reservation expires while payment is in-flight, commitStock fails in Redis.
 * The system must execute a Compensating Refund Saga, transition the order to EXPIRED,
 * and guarantee that overselling is prevented.
 */
@SpringBootTest
@ActiveProfiles("test")
class ReservationExpiryDuringPaymentRaceTest {

    @Autowired
    private OrderReservedConsumer orderReservedConsumer;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentSimulationService paymentSimulationService;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private InventoryClient inventoryClient;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAll();
        reservationRepository.deleteAll();
        processedEventRepository.deleteAll();
        paymentSimulationService.setMode(PaymentSimulationMode.ALWAYS_SUCCESS);
    }

    @Test
    @DisplayName("Scenario 07: Should trigger Compensating Refund Saga and prevent overselling when reservation expires during payment")
    void shouldTriggerCompensatingRefundSagaWhenReservationExpiresDuringPayment() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_slow_payment";

        // 1. Initial State: Order is PENDING, Reservation is ACTIVE
        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "idemp_slow_1", "fp_slow_1", resId, 1);
        orderRepository.save(order);

        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(120));
        reservationRepository.save(reservation);

        // 2. Mock commitStock failure: Redis commit rejected because reservation was expired and released
        doThrow(new StockCommitFailedException("Redis commit failed: reservation " + resId + " is no longer active"))
            .when(inventoryClient).commitStock(eq(saleId), eq(resId), any());

        // 3. Deliver Kafka event
        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "race-corr-1", payload
        );
        String jsonPayload = objectMapper.writeValueAsString(envelope);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            KafkaTopics.ORDERS_TOPIC, 0, 201L, orderId.toString(), jsonPayload
        );
        orderReservedConsumer.onOrderReserved(record);

        // 4. Assert Invariants
        OrderEntity finalOrder = orderRepository.findById(orderId).orElseThrow();

        // INVARIANT 1: Order MUST NOT be CONFIRMED! (Zero overselling)
        assertThat(finalOrder.getStatus()).isNotEqualTo(OrderStatus.CONFIRMED);
        assertThat(finalOrder.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(finalOrder.getFailureCode()).isEqualTo("RESERVATION_EXPIRED_DURING_PAYMENT");

        // INVARIANT 2: Reservation marked EXPIRED
        ReservationEntity finalRes = reservationRepository.findById(resId).orElseThrow();
        assertThat(finalRes.getStatus()).isEqualTo(ReservationStatus.EXPIRED);

        // INVARIANT 3: Deduplication record recorded
        assertThat(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).isTrue();
    }
}
