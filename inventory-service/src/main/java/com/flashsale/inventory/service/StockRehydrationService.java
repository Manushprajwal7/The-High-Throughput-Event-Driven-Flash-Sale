package com.flashsale.inventory.service;

import com.flashsale.common.exception.SaleNotFoundException;
import com.flashsale.inventory.util.InventoryKeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Service to rehydrate Redis stock after restart, replica failover, or loss of volatile cache state.
 * (Scenario 09 / REM-07)
 */
@Service
public class StockRehydrationService {
    private static final Logger log = LoggerFactory.getLogger(StockRehydrationService.class);

    private final ReactiveStringRedisTemplate redisTemplate;

    public StockRehydrationService(ReactiveStringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Rehydrates Redis stock counter from authoritative totalCapacity, committed units,
     * and active ZSET lease states.
     * Computes available stock = max(0, totalCapacity - committedUnits - activeReservedUnits).
     */
    public Mono<Integer> rehydrateStock(UUID saleId, int committedUnits) {
        String saleKey = InventoryKeyUtils.saleKey(saleId);
        String stockKey = InventoryKeyUtils.stockKey(saleId);
        String expKey = InventoryKeyUtils.expirationsKey(saleId);

        return redisTemplate.opsForHash().get(saleKey, "totalCapacity")
            .switchIfEmpty(Mono.error(new SaleNotFoundException(saleId)))
            .flatMap(totalCapacityObj -> {
                int totalCapacity = Integer.parseInt(totalCapacityObj.toString());
                long nowMillis = Instant.now().toEpochMilli();

                return redisTemplate.opsForZSet().rangeByScore(expKey, Range.closed((double) nowMillis, Double.POSITIVE_INFINITY))
                    .map(member -> {
                        String[] parts = member.split(":");
                        return parts.length >= 2 ? Integer.parseInt(parts[1]) : 1;
                    })
                    .reduce(0, Integer::sum)
                    .defaultIfEmpty(0)
                    .flatMap(activeReservedUnits -> {
                        int recalculatedStock = Math.max(0, totalCapacity - committedUnits - activeReservedUnits);
                        return redisTemplate.opsForValue().set(stockKey, String.valueOf(recalculatedStock))
                            .doOnSuccess(v -> log.info("Authoritatively rehydrated stock for sale {}: totalCapacity={}, committed={}, activeReserved={}, availableStock={}",
                                saleId, totalCapacity, committedUnits, activeReservedUnits, recalculatedStock))
                            .thenReturn(recalculatedStock);
                    });
            });
    }
}
