package com.flashsale.order.service;

import com.flashsale.common.dto.CreateOrderRequest;
import com.flashsale.common.dto.OrderResponse;
import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.common.exception.IdempotencyConflictException;
import com.flashsale.common.exception.IllegalStateTransitionException;
import com.flashsale.common.exception.OrderNotFoundException;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.common.model.ReservationStatus;
import com.flashsale.order.client.InventoryClient;
import com.flashsale.order.entity.OrderEntity;
import com.flashsale.order.entity.ReservationEntity;
import com.flashsale.order.repository.OrderRepository;
import com.flashsale.order.repository.ReservationRepository;
import com.flashsale.order.util.RequestFingerprintUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceIdempotencyTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private InventoryClient inventoryClient;

    @Mock
    private OrderPersistenceHelper orderPersistenceHelper;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

        orderService = new OrderService(
            orderRepository,
            reservationRepository,
            inventoryClient,
            orderPersistenceHelper,
            mapper
        );
    }

    @Test
    void shouldCreateNewOrderWhenKeyIsFresh() {
        UUID saleId = UUID.randomUUID();
        String shopperId = "shopper_1";
        String key = "key_abc123";
        CreateOrderRequest request = new CreateOrderRequest(saleId, shopperId, 1);

        when(orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(saleId, shopperId, key))
            .thenReturn(Optional.empty());

        UUID resId = UUID.randomUUID();
        when(inventoryClient.reserveStock(eq(saleId), eq(shopperId), eq(1), any(), any()))
            .thenReturn(new ReservationResponse(resId, saleId, 1, Instant.now().plusSeconds(120)));

        doNothing().when(orderPersistenceHelper).persistOrderAndReservation(any(), any(), any());

        OrderResponse response = orderService.createOrder(key, request, "corr-1");

        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(response.reservationId()).isEqualTo(resId);
        assertThat(response.message()).contains("accepted");

        verify(inventoryClient, times(1)).reserveStock(any(), any(), anyInt(), any(), any());
        verify(orderPersistenceHelper, times(1)).persistOrderAndReservation(any(), any(), any());
    }

    @Test
    void shouldReturnCachedOrderForSameKeyAndSamePayloadWithoutReReservations() {
        UUID saleId = UUID.randomUUID();
        String shopperId = "shopper_1";
        String key = "key_abc123";
        CreateOrderRequest request = new CreateOrderRequest(saleId, shopperId, 1);
        String fingerprint = RequestFingerprintUtils.computeFingerprint(request);

        UUID orderId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        OrderEntity existing = new OrderEntity(
            orderId, saleId, shopperId, key, fingerprint, resId, 1, OrderStatus.PENDING
        );

        when(orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(saleId, shopperId, key))
            .thenReturn(Optional.of(existing));

        OrderResponse response = orderService.createOrder(key, request, "corr-1");

        assertThat(response.orderId()).isEqualTo(orderId);
        assertThat(response.reservationId()).isEqualTo(resId);
        assertThat(response.message()).contains("existing");

        // Inventory reservation must NOT be called for duplicate requests
        verify(inventoryClient, never()).reserveStock(any(), any(), anyInt(), any(), any());
        verify(orderRepository, never()).saveAndFlush(any());
    }

    @Test
    void shouldRejectWhenSameKeyReusedWithDifferentPayload() {
        UUID saleId = UUID.randomUUID();
        String shopperId = "shopper_1";
        String key = "key_abc123";

        // Existing order was for quantity 1
        CreateOrderRequest originalReq = new CreateOrderRequest(saleId, shopperId, 1);
        String originalFp = RequestFingerprintUtils.computeFingerprint(originalReq);

        OrderEntity existing = new OrderEntity(
            UUID.randomUUID(), saleId, shopperId, key, originalFp, UUID.randomUUID(), 1, OrderStatus.PENDING
        );

        when(orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(saleId, shopperId, key))
            .thenReturn(Optional.of(existing));

        // New request attempts to re-use same key for quantity 2
        CreateOrderRequest modifiedReq = new CreateOrderRequest(saleId, shopperId, 2);

        assertThatThrownBy(() -> orderService.createOrder(key, modifiedReq, "corr-1"))
            .isInstanceOf(IdempotencyConflictException.class)
            .hasMessageContaining("different request payload");

        verify(inventoryClient, never()).reserveStock(any(), any(), anyInt(), any(), any());
    }

    @Test
    void shouldHandleDatabaseRaceConditionSafely() {
        UUID saleId = UUID.randomUUID();
        String shopperId = "shopper_1";
        String key = "key_abc123";
        CreateOrderRequest request = new CreateOrderRequest(saleId, shopperId, 1);
        String fingerprint = RequestFingerprintUtils.computeFingerprint(request);

        // Step 1: initial check returns empty
        when(orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(saleId, shopperId, key))
            .thenReturn(Optional.empty());

        UUID resId = UUID.randomUUID();
        when(inventoryClient.reserveStock(eq(saleId), eq(shopperId), eq(1), any(), any()))
            .thenReturn(new ReservationResponse(resId, saleId, 1, Instant.now().plusSeconds(120)));

        // Step 2: orderPersistenceHelper throws DataIntegrityViolationException (another concurrent thread won the race)
        doThrow(new DataIntegrityViolationException("Duplicate key violation"))
            .when(orderPersistenceHelper).persistOrderAndReservation(any(), any(), any());

        UUID winnerOrderId = UUID.randomUUID();
        OrderEntity winner = new OrderEntity(
            winnerOrderId, saleId, shopperId, key, fingerprint, UUID.randomUUID(), 1, OrderStatus.PENDING
        );

        // Subsequent lookup returns the winner
        when(orderRepository.findBySaleIdAndShopperIdAndIdempotencyKey(saleId, shopperId, key))
            .thenReturn(Optional.empty()) // first call
            .thenReturn(Optional.of(winner)); // second call in catch block

        OrderResponse response = orderService.createOrder(key, request, "corr-1");

        assertThat(response.orderId()).isEqualTo(winnerOrderId);

        // Orphaned reservation MUST be compensated/released back to Inventory Service
        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("CONCURRENT_IDEMPOTENCY_RACE"), any());
    }

    @Test
    void shouldCancelPendingOrderAndReleaseStock() {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        OrderEntity order = new OrderEntity(
            orderId, saleId, "shopper_1", "key_1", "fp", resId, 1, OrderStatus.PENDING
        );
        ReservationEntity res = new ReservationEntity(
            resId, saleId, orderId, 1, ReservationStatus.ACTIVE, Instant.now().plusSeconds(120)
        );

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(reservationRepository.findById(resId)).thenReturn(Optional.of(res));

        var response = orderService.cancelOrder(orderId, "USER_CANCELLED", "corr-1");

        assertThat(response.status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(res.getStatus()).isEqualTo(ReservationStatus.RELEASED);

        verify(inventoryClient, times(1)).releaseStock(eq(saleId), eq(resId), eq("USER_CANCELLED"), any());
        verify(orderRepository, times(1)).save(order);
    }

    @Test
    void shouldRejectCancellationWhenOrderIsAlreadyConfirmed() {
        UUID orderId = UUID.randomUUID();
        OrderEntity order = new OrderEntity(
            orderId, UUID.randomUUID(), "shopper_1", "key_1", "fp", UUID.randomUUID(), 1, OrderStatus.CONFIRMED
        );

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        assertThatThrownBy(() -> orderService.cancelOrder(orderId, "USER_CANCELLED", "corr-1"))
            .isInstanceOf(IllegalStateTransitionException.class);

        verify(inventoryClient, never()).releaseStock(any(), any(), any(), any());
    }
}
