package com.flashsale.inventory;

import com.flashsale.common.dto.CreateSaleRequest;
import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.inventory.config.RedisConfig;
import com.flashsale.inventory.service.ReservationService;
import com.flashsale.inventory.service.SaleService;
import com.flashsale.inventory.service.StockRehydrationService;
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
 * Proves fix for Scenario 09 (REM-07):
 * Redis restarts or loses recently acknowledged reservation state (failover).
 * The stock rehydration mechanism recomputes available stock:
 * available = max(0, totalCapacity - committedUnits - activeReservedUnits)
 * and authoritatively restores the Redis stock key.
 */
class RedisFailoverStateLossRehydrationTest {

    private static RedisServer redisServer;
    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static SaleService saleService;
    private static ReservationService reservationService;
    private static StockRehydrationService stockRehydrationService;

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
        stockRehydrationService = new StockRehydrationService(redisTemplate);
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
    @DisplayName("Scenario 09 (REM-07): Stock rehydration correctly restores stock key after failover or cache loss")
    void shouldRehydrateStockCorrectlyAfterRedisStateLoss() {
        int totalCapacity = 20;
        Instant now = Instant.now();
        var sale = saleService.createSale(new CreateSaleRequest(
            "Rehydration Sale",
            totalCapacity,
            now.minusSeconds(10),
            now.plusSeconds(3600)
        )).block();
        assertThat(sale).isNotNull();
        UUID saleId = sale.saleId();

        saleService.openSale(saleId).block();

        // 1. Reserve 4 units across two shoppers
        reservationService.reserveStock(saleId, new ReserveStockRequest("shopper_1", 2, 120)).block();
        reservationService.reserveStock(saleId, new ReserveStockRequest("shopper_2", 2, 120)).block();

        // Stock in Redis is 16
        String stockKey = InventoryKeyUtils.stockKey(saleId);
        assertThat(redisTemplate.opsForValue().get(stockKey).block()).isEqualTo("16");

        // 2. Simulate Failover/Crash where stock key was corrupted or lost (e.g., reset back to total capacity 20)
        redisTemplate.opsForValue().set(stockKey, "20").block();

        // 3. 5 units are committed in relational database (orders with CONFIRMED status)
        int committedOrdersInDb = 5;

        // 4. Trigger authoritative Stock Rehydration
        // Expected available stock = 20 (total) - 5 (committed) - 4 (active reserved in ZSET) = 11
        Integer rehydratedStock = stockRehydrationService.rehydrateStock(saleId, committedOrdersInDb).block();

        assertThat(rehydratedStock).isEqualTo(11);

        // 5. INVARIANT: Redis stock key now accurately reflects 11 available units
        String updatedStockInRedis = redisTemplate.opsForValue().get(stockKey).block();
        assertThat(updatedStockInRedis).isEqualTo("11");
    }
}
