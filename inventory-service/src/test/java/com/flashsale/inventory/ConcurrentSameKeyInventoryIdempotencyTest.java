package com.flashsale.inventory;

import com.flashsale.common.dto.CreateSaleRequest;
import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.inventory.config.RedisConfig;
import com.flashsale.inventory.service.ReservationService;
import com.flashsale.inventory.service.SaleService;
import com.flashsale.inventory.util.InventoryKeyUtils;
import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves fix for Scenario 05 (REM-06) at the Redis Inventory layer:
 * Two requests using the same idempotency key reuse the existing reservation
 * without double-decrementing stock in Redis.
 */
class ConcurrentSameKeyInventoryIdempotencyTest {

    private static RedisServer redisServer;
    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static SaleService saleService;
    private static ReservationService reservationService;

    @BeforeAll
    static void setUp() throws IOException {
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
    static void tearDown() throws IOException {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    @DisplayName("Scenario 05 (REM-06): Same idempotency key reuses reservation and prevents double stock decrement")
    void sameIdempotencyKeyReusesReservationAndPreventsDoubleDecrement() {
        int initialCapacity = 10;
        Instant now = Instant.now();
        var sale = saleService.createSale(new CreateSaleRequest(
            "Idempotency Race Test Sale",
            initialCapacity,
            now.minusSeconds(10),
            now.plusSeconds(3600)
        )).block();
        assertThat(sale).isNotNull();
        UUID saleId = sale.saleId();

        saleService.openSale(saleId).block();

        String idempotencyKey = "client-tx-12345";

        // First reservation request
        var res1 = reservationService.reserveStock(
            saleId,
            new ReserveStockRequest("shopper_concurrent", 2, 120, idempotencyKey)
        ).block();
        assertThat(res1).isNotNull();

        // Stock in Redis drops by 2: 10 -> 8
        String stockAfterFirst = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(stockAfterFirst).isEqualTo("8");

        // Second reservation request with IDENTICAL idempotency key
        var res2 = reservationService.reserveStock(
            saleId,
            new ReserveStockRequest("shopper_concurrent", 2, 120, idempotencyKey)
        ).block();
        assertThat(res2).isNotNull();

        // INVARIANT 1: Both requests receive the exact same reservation ID
        assertThat(res2.reservationId()).isEqualTo(res1.reservationId());

        // INVARIANT 2: Stock in Redis was NOT decremented again (still 8, NOT 6!)
        String stockAfterSecond = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(stockAfterSecond).isEqualTo("8");
    }
}
