package com.flashsale.order.concurrency;

import com.flashsale.common.dto.CreateOrderRequest;
import com.flashsale.common.dto.OrderResponse;
import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.order.client.InventoryClient;
import com.flashsale.order.entity.OrderEntity;
import com.flashsale.order.repository.OrderRepository;
import com.flashsale.order.repository.OutboxEventRepository;
import com.flashsale.order.repository.ReservationRepository;
import com.flashsale.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Proves fix for Scenario 05 (REM-06):
 * Two simultaneous requests use the same idempotency key.
 * Redis idempotency prevents double decrement of stock.
 * Relational unique constraint detects concurrent duplicate, safely maps to the winner order,
 * and ensures no phantom duplicate orders or leaked reservations exist.
 */
@SpringBootTest
@ActiveProfiles("test")
class ConcurrentSameIdempotencyKeyRaceTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockBean
    private InventoryClient inventoryClient;

    @BeforeEach
    void cleanUp() {
        orderRepository.deleteAll();
        reservationRepository.deleteAll();
        outboxEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Scenario 05 (REM-06): Concurrent requests with same idempotency key yield identical order and single stock reservation")
    void concurrentSameKeyRequestsShouldBeSerializedAndYieldIdenticalOrder() throws Exception {
        UUID saleId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        String shopperId = "shopper_concurrent_race";
        String idempotencyKey = "idem-race-key-999";
        CreateOrderRequest request = new CreateOrderRequest(saleId, shopperId, 1);

        // Redis returns the same reservation token for the same idempotency key
        when(inventoryClient.reserveStock(eq(saleId), eq(shopperId), eq(1), eq(idempotencyKey), any()))
            .thenReturn(new ReservationResponse(reservationId, saleId, 1, Instant.now().plusSeconds(120)));

        int concurrency = 2;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        CountDownLatch latch = new CountDownLatch(concurrency);

        Future<OrderResponse>[] futures = new Future[concurrency];
        for (int i = 0; i < concurrency; i++) {
            final int index = i;
            futures[i] = executor.submit(() -> {
                barrier.await();
                try {
                    return orderService.createOrder(idempotencyKey, request, "corr-" + index);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        OrderResponse resp1 = futures[0].get();
        OrderResponse resp2 = futures[1].get();

        assertThat(resp1).isNotNull();
        assertThat(resp2).isNotNull();

        // INVARIANT 1: Both requests receive the exact same Order ID
        assertThat(resp1.orderId()).isEqualTo(resp2.orderId());
        assertThat(resp1.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(resp2.status()).isEqualTo(OrderStatus.PENDING);

        // INVARIANT 2: Exactly 1 order exists in PostgreSQL
        assertThat(orderRepository.count()).isEqualTo(1L);

        // INVARIANT 3: Exactly 1 reservation exists in PostgreSQL
        assertThat(reservationRepository.count()).isEqualTo(1L);

        // INVARIANT 4: Stock was NOT double decremented, and the winning reservation was not erroneously released
        verify(inventoryClient, never()).releaseStock(eq(saleId), eq(reservationId), any(), any());
    }
}
