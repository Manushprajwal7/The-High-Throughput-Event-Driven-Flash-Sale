package com.flashsale.payment.service;

import com.flashsale.common.dto.OrderEventEnvelope;
import com.flashsale.common.dto.OrderReservedPayload;
import com.flashsale.common.exception.StockCommitFailedException;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.payment.client.InventoryClient;
import com.flashsale.payment.entity.OrderEntity;
import com.flashsale.payment.entity.ProcessedEventEntity;
import com.flashsale.payment.entity.ProcessedEventId;
import com.flashsale.payment.entity.ReservationEntity;
import com.flashsale.payment.repository.OrderRepository;
import com.flashsale.payment.repository.ProcessedEventRepository;
import com.flashsale.payment.repository.ReservationRepository;
import com.flashsale.payment.simulation.PaymentResult;
import com.flashsale.payment.simulation.PaymentSimulationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FulfillmentServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private PaymentSimulationService paymentSimulationService;

    @Mock
    private InventoryClient inventoryClient;

    private FulfillmentService fulfillmentService;

    @BeforeEach
    void setUp() {
        fulfillmentService = new FulfillmentService(
            orderRepository,
            reservationRepository,
            processedEventRepository,
            paymentSimulationService,
            inventoryClient
        );
    }

    @Test
    void shouldConfirmOrderAndCommitStockOnPaymentSuccess() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_123";

        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "key_1", "fp_1", resId, 1);
        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(120));

        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "corr-1", payload
        );

        when(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).thenReturn(false);
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(reservationRepository.findById(resId)).thenReturn(Optional.of(reservation));
        when(paymentSimulationService.processPayment(eq(orderId), eq(shopperId), eq(1)))
            .thenReturn(PaymentResult.success("PAY-SIM-999"));

        fulfillmentService.processOrderReserved(envelope);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(order.getPaymentReference()).isEqualTo("PAY-SIM-999");
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.COMMITTED);

        verify(orderRepository, atLeastOnce()).save(order);
        verify(reservationRepository, atLeastOnce()).save(reservation);
        verify(inventoryClient, times(1)).commitStock(eq(saleId), eq(resId), eq("corr-1"));
        verify(processedEventRepository, times(1)).save(any(ProcessedEventEntity.class));
        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }

    @Test
    void shouldFailOrderAndReleaseStockOnPaymentDecline() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_declined";

        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "key_2", "fp_2", resId, 1);
        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(120));

        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "corr-2", payload
        );

        when(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).thenReturn(false);
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(reservationRepository.findById(resId)).thenReturn(Optional.of(reservation));
        when(paymentSimulationService.processPayment(eq(orderId), eq(shopperId), eq(1)))
            .thenReturn(PaymentResult.declined("CARD_DECLINED", "Card was declined"));

        fulfillmentService.processOrderReserved(envelope);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_FAILED);
        assertThat(order.getFailureCode()).isEqualTo("CARD_DECLINED");
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RELEASED);

        verify(orderRepository, atLeastOnce()).save(order);
        verify(reservationRepository, atLeastOnce()).save(reservation);
        // Compensating stock release must be called!
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("CARD_DECLINED"), eq("corr-2"));
        verify(processedEventRepository, times(1)).save(any(ProcessedEventEntity.class));
        verify(inventoryClient, never()).commitStock(any(), any(), any());
    }

    @Test
    void shouldSkipDuplicateEventIdempotently() {
        UUID eventId = UUID.randomUUID();
        OrderReservedPayload payload = new OrderReservedPayload(UUID.randomUUID(), UUID.randomUUID(), "shopper", 1, UUID.randomUUID(), Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", payload.orderId().toString(), "ORDER_RESERVED", 1, Instant.now(), "corr-dup", payload
        );

        when(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).thenReturn(true);

        fulfillmentService.processOrderReserved(envelope);

        verify(orderRepository, never()).findById(any());
        verify(paymentSimulationService, never()).processPayment(any(), any(), anyInt());
        verify(inventoryClient, never()).commitStock(any(), any(), any());
        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }

    @Test
    void shouldExpireOrderAndReleaseStockWhenReservationExpired() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_expired";

        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "key_3", "fp_3", resId, 1);
        // Reservation expired 10 seconds ago
        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().minusSeconds(10));

        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().minusSeconds(10));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "corr-exp", payload
        );

        when(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).thenReturn(false);
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(reservationRepository.findById(resId)).thenReturn(Optional.of(reservation));

        fulfillmentService.processOrderReserved(envelope);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(order.getFailureCode()).isEqualTo("RESERVATION_EXPIRED");
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.EXPIRED);

        // Never attempt payment or commit on expired reservation
        verify(paymentSimulationService, never()).processPayment(any(), any(), anyInt());
        verify(inventoryClient, never()).commitStock(any(), any(), any());
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("RESERVATION_EXPIRED"), eq("corr-exp"));
    }

    @Test
    void shouldTriggerCompensatingRefundSagaWhenCommitStockFails() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_late_payment";

        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "key_refund", "fp_refund", resId, 1);
        ReservationEntity reservation = new ReservationEntity(resId, saleId, orderId, 1, Instant.now().plusSeconds(120));

        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "corr-refund", payload
        );

        when(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).thenReturn(false);
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(reservationRepository.findById(resId)).thenReturn(Optional.of(reservation));
        when(paymentSimulationService.processPayment(eq(orderId), eq(shopperId), eq(1)))
            .thenReturn(PaymentResult.success("PAY-SIM-REFUND-123"));

        // Simulate stock commit failure because reservation expired during payment
        doThrow(new StockCommitFailedException("Redis commit rejected: reservation expired"))
            .when(inventoryClient).commitStock(eq(saleId), eq(resId), eq("corr-refund"));

        fulfillmentService.processOrderReserved(envelope);

        // Assert order was NOT confirmed! Transitioned to EXPIRED with failure code
        assertThat(order.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(order.getFailureCode()).isEqualTo("RESERVATION_EXPIRED_DURING_PAYMENT");
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.EXPIRED);

        // Assert payment was refunded via Compensating Saga
        verify(paymentSimulationService, times(1)).refundPayment("PAY-SIM-REFUND-123");
        verify(orderRepository, atLeastOnce()).save(order);
        verify(processedEventRepository, times(1)).save(any(ProcessedEventEntity.class));
    }

    @Test
    void shouldSkipDuplicateDeliveryWhenOrderAlreadyProcessingOrTerminal() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String shopperId = "shopper_dup";

        // Order already in PROCESSING state by another consumer thread
        OrderEntity order = new OrderEntity(orderId, saleId, shopperId, "key_dup", "fp_dup", resId, 1);
        order.transitionTo(OrderStatus.PROCESSING);

        OrderReservedPayload payload = new OrderReservedPayload(orderId, saleId, shopperId, 1, resId, Instant.now().plusSeconds(120));
        OrderEventEnvelope<OrderReservedPayload> envelope = new OrderEventEnvelope<>(
            eventId, "ORDER", orderId.toString(), "ORDER_RESERVED", 1, Instant.now(), "corr-dup", payload
        );

        when(processedEventRepository.isEventProcessed("payment-fulfillment-service", eventId)).thenReturn(false);
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        fulfillmentService.processOrderReserved(envelope);

        // Never charge card on duplicate delivery
        verify(paymentSimulationService, never()).processPayment(any(), any(), anyInt());
        verify(inventoryClient, never()).commitStock(any(), any(), any());
        verify(processedEventRepository, times(1)).save(any(ProcessedEventEntity.class));
    }
}
