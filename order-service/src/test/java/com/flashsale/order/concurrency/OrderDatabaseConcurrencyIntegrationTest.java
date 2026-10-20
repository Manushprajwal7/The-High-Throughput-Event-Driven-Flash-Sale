package com.flashsale.order.concurrency;

import com.flashsale.common.dto.CreateOrderRequest;
import com.flashsale.common.dto.OrderResponse;
import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.order.client.InventoryClient;
import com.flashsale.order.repository.OrderRepository;
import com.flashsale.order.repository.ReservationRepository;
import com.flashsale.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("test")
class OrderDatabaseConcurrencyIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @MockBean
    private InventoryClient inventoryClient;

    @BeforeEach
    void cleanUp() {
        orderRepository.deleteAll();
        reservationRepository.deleteAll();
    }

    @Test
    void shouldAllowOnlyOneOrderUnderConcurrentDuplicateSubmissions() throws InterruptedException {
        UUID saleId = UUID.randomUUID();
        String shopperId = "shopper_concurrent_1";
        String idempotencyKey = "idemp_concurrency_key_999";
        CreateOrderRequest request = new CreateOrderRequest(saleId, shopperId, 1);

        // Inventory client simulates generating a reservation
        when(inventoryClient.reserveStock(eq(saleId), eq(shopperId), eq(1), any()))
            .thenAnswer(inv -> new ReservationResponse(UUID.randomUUID(), saleId, 1, Instant.now().plusSeconds(120)));

        int concurrentThreads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrentThreads);

        ConcurrentLinkedQueue<UUID> returnedOrderIds = new ConcurrentLinkedQueue<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < concurrentThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Synchronized start
                    OrderResponse response = orderService.createOrder(idempotencyKey, request, "test-correlation");
                    if (response != null) {
                        successCount.incrementAndGet();
                        returnedOrderIds.add(response.orderId());
                    }
                } catch (Exception ex) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = doneLatch.await(15, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(finished).isTrue();
        assertThat(successCount.get()).isEqualTo(concurrentThreads);
        assertThat(errorCount.get()).isEqualTo(0);

        // Core Invariant Check: Exactly 1 order in PostgreSQL database!
        assertThat(orderRepository.count()).isEqualTo(1L);

        // All 20 threads received the identical logical order ID!
        UUID uniqueOrderId = returnedOrderIds.peek();
        assertThat(uniqueOrderId).isNotNull();
        assertThat(returnedOrderIds).allMatch(id -> id.equals(uniqueOrderId));
    }
}
