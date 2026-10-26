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
 * Proves fix for Scenario 08 (REM-11):
 * A retry attempts to release the same reservation twice.
 * The extended 300s release tombstone ensures that subsequent release calls
 * detect the RELEASED state and do NOT increment stock a second time.
 */
class IdempotentStockReleaseRetryTest {

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
    @DisplayName("Scenario 08 (REM-11): Repeated release of same reservation must be idempotent and not increment stock twice")
    void repeatedReleaseOfSameReservationDoesNotIncrementStockTwice() {
        int initialCapacity = 10;
        Instant now = Instant.now();
        var sale = saleService.createSale(new CreateSaleRequest(
            "Tombstone Test Sale",
            initialCapacity,
            now.minusSeconds(10),
            now.plusSeconds(3600)
        )).block();
        assertThat(sale).isNotNull();
        UUID saleId = sale.saleId();

        saleService.openSale(saleId).block();

        // 1. Reserve 3 units: stock goes from 10 to 7
        int reservedQty = 3;
        var reservation = reservationService.reserveStock(
            saleId,
            new ReserveStockRequest("shopper_tombstone", reservedQty, 120)
        ).block();
        assertThat(reservation).isNotNull();
        UUID resId = reservation.reservationId();

        String stockAfterReserve = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(stockAfterReserve).isEqualTo("7");

        // 2. First release: stock restores from 7 to 10
        reservationService.releaseStock(saleId, resId, "PAYMENT_CANCELLED").block();

        String stockAfterFirstRelease = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(stockAfterFirstRelease).isEqualTo("10");

        // 3. Second release (retry or duplicate event): MUST BE IDEMPOTENT NO-OP
        reservationService.releaseStock(saleId, resId, "RETRY_DUPLICATE").block();

        // 4. INVARIANT: Stock remains 10 (NOT 13!)
        String stockAfterSecondRelease = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(stockAfterSecondRelease).isEqualTo("10");

        // 5. Third release: STILL NO-OP
        reservationService.releaseStock(saleId, resId, "THIRD_ATTEMPT").block();
        String stockAfterThirdRelease = redisTemplate.opsForValue().get(InventoryKeyUtils.stockKey(saleId)).block();
        assertThat(stockAfterThirdRelease).isEqualTo("10");
    }
}
