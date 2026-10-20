package com.flashsale.inventory.concurrency;

import com.flashsale.common.dto.CreateSaleRequest;
import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.common.exception.SoldOutException;
import com.flashsale.common.model.SaleStatus;
import com.flashsale.inventory.config.RedisConfig;
import com.flashsale.inventory.service.ReservationService;
import com.flashsale.inventory.service.SaleService;
import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryConcurrencyIntegrationTest {

    private static RedisServer redisServer;
    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static SaleService saleService;
    private static ReservationService reservationService;

    @BeforeAll
    static void startRedis() throws IOException {
        redisServer = RedisServer.newRedisServer().start();
        int port = redisServer.getBindPort();

        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("127.0.0.1", port);
        connectionFactory = new LettuceConnectionFactory(config);
        connectionFactory.afterPropertiesSet();

        redisTemplate = new ReactiveStringRedisTemplate(connectionFactory);

        RedisConfig redisConfig = new RedisConfig();
        RedisScript<Long> reserveScript = redisConfig.reserveStockScript();
        RedisScript<Long> releaseScript = redisConfig.releaseStockScript();
        RedisScript<Long> commitScript = redisConfig.commitStockScript();

        saleService = new SaleService(redisTemplate);
        reservationService = new ReservationService(
            redisTemplate,
            reserveScript,
            releaseScript,
            commitScript
        );
    }

    @AfterAll
    static void stopRedis() throws IOException {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    void shouldPreventOversellingUnderHighConcurrency() throws InterruptedException {
        // 1. Setup a sale with strictly 10 units capacity
        int totalCapacity = 10;
        Instant now = Instant.now();
        CreateSaleRequest createReq = new CreateSaleRequest(
            "Flash Sale PS5",
            totalCapacity,
            now.minusSeconds(10),
            now.plusSeconds(3600)
        );

        var sale = saleService.createSale(createReq).block();
        assertThat(sale).isNotNull();
        UUID saleId = sale.saleId();

        // Open the sale
        saleService.openSale(saleId).block();

        // 2. Spawn 50 concurrent threads attempting to reserve 1 unit each
        int totalAttempts = 50;
        ExecutorService executor = Executors.newFixedThreadPool(16);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalAttempts);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger soldOutCount = new AtomicInteger(0);
        AtomicInteger unexpectedErrorCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<UUID> successfulReservationIds = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < totalAttempts; i++) {
            final String shopperId = "shopper_" + i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Synchronize thread start
                    reservationService.reserveStock(saleId, new ReserveStockRequest(shopperId, 1, 120))
                        .doOnSuccess(res -> {
                            successCount.incrementAndGet();
                            successfulReservationIds.add(res.reservationId());
                        })
                        .doOnError(SoldOutException.class, ex -> soldOutCount.incrementAndGet())
                        .doOnError(ex -> {
                            if (!(ex instanceof SoldOutException)) {
                                unexpectedErrorCount.incrementAndGet();
                            }
                        })
                        .block();
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Trigger simultaneous start
        startLatch.countDown();
        boolean completed = doneLatch.await(15, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();

        // 3. Verify INVARIANTS:
        // - Exactly 10 successes
        assertThat(successCount.get()).isEqualTo(totalCapacity);
        // - Exactly 40 sold out rejections
        assertThat(soldOutCount.get()).isEqualTo(totalAttempts - totalCapacity);
        assertThat(unexpectedErrorCount.get()).isEqualTo(0);

        // - Verify Redis final stock is strictly 0 (never negative)
        String remainingStockStr = redisTemplate.opsForValue().get("flashsale:stock:" + saleId).block();
        assertThat(remainingStockStr).isEqualTo("0");

        // 4. Test Idempotent Release:
        // Releasing 1 successful reservation restores stock to 1
        UUID firstReservationId = successfulReservationIds.poll();
        assertThat(firstReservationId).isNotNull();

        reservationService.releaseStock(saleId, firstReservationId, "TEST_PAYMENT_FAILED").block();
        String stockAfterRelease = redisTemplate.opsForValue().get("flashsale:stock:" + saleId).block();
        assertThat(stockAfterRelease).isEqualTo("1");

        // Repeating release on the same reservation MUST NOT increase stock twice!
        reservationService.releaseStock(saleId, firstReservationId, "DUPLICATE_RELEASE").block();
        String stockAfterDoubleRelease = redisTemplate.opsForValue().get("flashsale:stock:" + saleId).block();
        assertThat(stockAfterDoubleRelease).isEqualTo("1");
    }
}
