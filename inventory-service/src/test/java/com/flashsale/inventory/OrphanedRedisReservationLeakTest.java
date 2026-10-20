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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves fix for Scenario 01 (REM-02):
 * When Redis reserves inventory but the application crashes before persisting to PostgreSQL,
 * the active Redis lease reaper detects the expired uncommitted reservation in the ZSET
 * and restores the stock counter atomically without permanent inventory leaks.
 */
class OrphanedRedisReservationLeakTest {

    private static RedisServer redisServer;
    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static SaleService saleService;
    private static ReservationService reservationService;
    private static RedisScript<Long> reapScript;

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
        reapScript = redisConfig.reapExpiredReservationsScript();

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
    @DisplayName("Scenario 01: Active Redis reaper must reclaim orphaned reservations after process crash and restore stock")
    void shouldReapOrphanedReservationAndRestoreStockAfterCrash() {
        // 1. Create a sale with 10 units capacity
        int initialCapacity = 10;
        Instant now = Instant.now();
        var sale = saleService.createSale(new CreateSaleRequest(
            "Crash Window Test Sale",
            initialCapacity,
            now.minusSeconds(10),
            now.plusSeconds(3600)
        )).block();
        assertThat(sale).isNotNull();
        UUID saleId = sale.saleId();

        saleService.openSale(saleId).block();

        // 2. Reserve 4 units
        int reservedQty = 4;
        var reservation = reservationService.reserveStock(
            saleId,
            new ReserveStockRequest("shopper_crash_victim", reservedQty, 120)
        ).block();
        assertThat(reservation).isNotNull();
        UUID resId = reservation.reservationId();

        // Stock in Redis immediately drops to 6
        String stockValAfterReserve = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(stockValAfterReserve).isEqualTo("6");

        // 3. Simulate App Crash:
        // Process crashed, so no PostgreSQL order/reservation was persisted.
        // Fast-forward time past expiration (+130 seconds)
        long futureEpochMillis = Instant.now().plusSeconds(130).toEpochMilli();

        // 4. Trigger active Redis reaper
        String stockKey = InventoryKeyUtils.stockKey(saleId);
        String expKey = InventoryKeyUtils.expirationsKey(saleId);

        Long reapedCount = redisTemplate.execute(
            reapScript,
            List.of(stockKey, expKey),
            List.of(String.valueOf(futureEpochMillis), saleId.toString(), "100")
        ).next().block();

        assertThat(reapedCount).isNotNull();
        assertThat(reapedCount).isGreaterThanOrEqualTo(1L);

        // 5. INVARIANT: Stock counter in Redis is fully restored to original capacity (10)
        String restoredStock = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(restoredStock).isEqualTo("10");

        // ZSET entry removed
        Double zscore = redisTemplate.opsForZSet().score(expKey, resId.toString() + ":" + reservedQty).block();
        assertThat(zscore).isNull();
    }
}
