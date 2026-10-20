package com.flashsale.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashsale.common.constant.KafkaTopics;
import com.flashsale.common.dto.OrderEventEnvelope;
import com.flashsale.common.dto.OrderReservedPayload;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class PaymentFulfillmentIntegrationTest {

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
    void cleanUp() {
        orderRepository.deleteAll();
        reservationRepository.deleteAll();
        processedEventRepository.deleteAll();
        paymentSimulationService.setMode(PaymentSimulationMode.ALWAYS_SUCCESS);
    }

    @Test
    void shouldProcessOrderReservedEventAndConfirmOrderInDatabase() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_it_1";

        // Insert pending order and reservation in database
        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "key_it_1", "fp_1", resId, 1);
        orderRepository.save(order);

        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(120));
        reservationRepository.save(reservation);

        // Construct event envelope
        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "it-corr-1", payload
        );
        String jsonPayload = objectMapper.writeValueAsString(envelope);

        // Simulate Kafka message delivery
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            KafkaTopics.ORDERS_TOPIC, 0, 100L, orderId.toString(), jsonPayload
        );
        orderReservedConsumer.onOrderReserved(record);

        // Verify database state: order is CONFIRMED, reservation is COMMITTED
        OrderEntity updatedOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(updatedOrder.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(updatedOrder.getPaymentReference()).startsWith("PAY-SIM-");

        ReservationEntity updatedReservation = reservationRepository.findById(resId).orElseThrow();
        assertThat(updatedReservation.getStatus()).isEqualTo(ReservationStatus.COMMITTED);

        // Verify consumer deduplication record persisted
        assertThat(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).isTrue();

        // Verify stock commit invoked
        verify(inventoryClient, times(1)).commitStock(eq(saleId), eq(resId), eq("it-corr-1"));
        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }

    @Test
    void shouldCompensateAndReleaseStockWhenPaymentFails() throws Exception {
        paymentSimulationService.setMode(PaymentSimulationMode.ALWAYS_DECLINE);

        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_it_decline";

        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "key_it_2", "fp_2", resId, 1);
        orderRepository.save(order);

        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(120));
        reservationRepository.save(reservation);

        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "it-corr-2", payload
        );
        String jsonPayload = objectMapper.writeValueAsString(envelope);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            KafkaTopics.ORDERS_TOPIC, 0, 101L, orderId.toString(), jsonPayload
        );
        orderReservedConsumer.onOrderReserved(record);

        // Verify database state: order is PAYMENT_FAILED, reservation is RELEASED
        OrderEntity updatedOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(updatedOrder.getStatus()).isEqualTo(OrderStatus.PAYMENT_FAILED);
        assertThat(updatedOrder.getFailureCode()).isEqualTo("INSUFFICIENT_FUNDS");

        ReservationEntity updatedReservation = reservationRepository.findById(resId).orElseThrow();
        assertThat(updatedReservation.getStatus()).isEqualTo(ReservationStatus.RELEASED);

        // Verify compensating stock release called
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("INSUFFICIENT_FUNDS"), eq("it-corr-2"));
        verify(inventoryClient, never()).commitStock(any(), any(), any());
    }
}
