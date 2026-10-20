package com.flashsale.inventory.service;

import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.common.exception.InvalidReservationException;
import com.flashsale.common.exception.ReservationCommitFailedException;
import com.flashsale.common.exception.SaleNotFoundException;
import com.flashsale.common.exception.SaleNotOpenException;
import com.flashsale.common.exception.SoldOutException;
import com.flashsale.inventory.util.InventoryKeyUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class ReservationService {
    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final RedisScript<Long> reserveStockScript;
    private final RedisScript<Long> releaseStockScript;
    private final RedisScript<Long> commitStockScript;
    private final RedisScript<Long> reapExpiredReservationsScript;

    private final Counter attemptsCounter;
    private final Counter successCounter;
    private final Counter soldOutCounter;
    private final Counter rejectedCounter;
    private final Counter releasesCounter;
    private final Counter commitsCounter;

    public ReservationService(
        ReactiveStringRedisTemplate redisTemplate,
        RedisScript<Long> reserveStockScript,
        RedisScript<Long> releaseStockScript,
        RedisScript<Long> commitStockScript
    ) {
        this(redisTemplate, reserveStockScript, releaseStockScript, commitStockScript,
            RedisScript.of(new ClassPathResource("scripts/reap_expired_reservations.lua"), Long.class),
            new SimpleMeterRegistry());
    }

    public ReservationService(
        ReactiveStringRedisTemplate redisTemplate,
        RedisScript<Long> reserveStockScript,
        RedisScript<Long> releaseStockScript,
        RedisScript<Long> commitStockScript,
        MeterRegistry meterRegistry
    ) {
        this(redisTemplate, reserveStockScript, releaseStockScript, commitStockScript,
            RedisScript.of(new ClassPathResource("scripts/reap_expired_reservations.lua"), Long.class),
            meterRegistry);
    }

    @Autowired
    public ReservationService(
        ReactiveStringRedisTemplate redisTemplate,
        RedisScript<Long> reserveStockScript,
        RedisScript<Long> releaseStockScript,
        RedisScript<Long> commitStockScript,
        RedisScript<Long> reapExpiredReservationsScript,
        MeterRegistry meterRegistry
    ) {
        this.redisTemplate = redisTemplate;
        this.reserveStockScript = reserveStockScript;
        this.releaseStockScript = releaseStockScript;
        this.commitStockScript = commitStockScript;
        this.reapExpiredReservationsScript = reapExpiredReservationsScript;

        this.attemptsCounter = meterRegistry.counter("flashsale.reservations.attempts");
        this.successCounter = meterRegistry.counter("flashsale.reservations.outcomes", "outcome", "success");
        this.soldOutCounter = meterRegistry.counter("flashsale.reservations.outcomes", "outcome", "sold_out");
        this.rejectedCounter = meterRegistry.counter("flashsale.reservations.outcomes", "outcome", "rejected");
        this.releasesCounter = meterRegistry.counter("flashsale.reservations.outcomes", "outcome", "released");
        this.commitsCounter = meterRegistry.counter("flashsale.reservations.outcomes", "outcome", "committed");
    }

    public Mono<ReservationResponse> reserveStock(UUID saleId, ReserveStockRequest req) {
        if (req.quantity() <= 0) {
            rejectedCounter.increment();
            return Mono.error(new InvalidReservationException("Requested quantity must be greater than zero"));
        }

        attemptsCounter.increment();

        UUID reservationId = UUID.randomUUID();
        Instant expiresAt = Instant.now().plusSeconds(req.reservationTtlSeconds());

        String stockKey = InventoryKeyUtils.stockKey(saleId);
        String resKey = InventoryKeyUtils.reservationKey(saleId, reservationId);
        String saleKey = InventoryKeyUtils.saleKey(saleId);
        String expKey = InventoryKeyUtils.expirationsKey(saleId);

        List<String> keys = List.of(stockKey, resKey, saleKey, expKey);
        List<String> args = List.of(
            String.valueOf(req.quantity()),
            String.valueOf(req.reservationTtlSeconds()),
            req.shopperId(),
            expiresAt.toString(),
            String.valueOf(expiresAt.toEpochMilli()),
            reservationId.toString()
        );

        return redisTemplate.execute(reserveStockScript, keys, args)
            .next()
            .flatMap(res -> {
                long code = res != null ? res : -99;
                if (code == 1L) {
                    successCounter.increment();
                    log.info("Reserved stock: sale={}, res={}, qty={}, shopper={}", saleId, reservationId, req.quantity(), req.shopperId());
                    return Mono.just(new ReservationResponse(reservationId, saleId, req.quantity(), expiresAt));
                } else if (code == 0L) {
                    soldOutCounter.increment();
                    return Mono.error(new SoldOutException(saleId));
                } else if (code == -1L) {
                    rejectedCounter.increment();
                    return Mono.error(new SaleNotFoundException(saleId));
                } else if (code == -2L) {
                    rejectedCounter.increment();
                    return Mono.error(new SaleNotOpenException(saleId, "Sale is not OPEN for reservation"));
                } else {
                    rejectedCounter.increment();
                    return Mono.error(new InvalidReservationException("Unexpected Lua reservation error: " + code));
                }
            });
    }

    public Mono<Void> releaseStock(UUID saleId, UUID reservationId, String reason) {
        String stockKey = InventoryKeyUtils.stockKey(saleId);
        String resKey = InventoryKeyUtils.reservationKey(saleId, reservationId);
        String expKey = InventoryKeyUtils.expirationsKey(saleId);

        List<String> keys = List.of(stockKey, resKey, expKey);
        List<String> args = List.of(
            reason != null ? reason : "MANUAL_OR_TIMEOUT",
            reservationId.toString()
        );

        return redisTemplate.execute(releaseStockScript, keys, args)
            .next()
            .doOnNext(res -> {
                if (res != null && res == 1L) {
                    releasesCounter.increment();
                    log.info("Successfully released stock: sale={}, res={}, reason={}", saleId, reservationId, reason);
                } else {
                    log.debug("Release stock skipped or already released: sale={}, res={}", saleId, reservationId);
                }
            })
            .then();
    }

    public Mono<Void> commitStock(UUID saleId, UUID reservationId) {
        String resKey = InventoryKeyUtils.reservationKey(saleId, reservationId);
        String expKey = InventoryKeyUtils.expirationsKey(saleId);

        List<String> keys = List.of(resKey, expKey);
        List<String> args = List.of(reservationId.toString());

        return redisTemplate.execute(commitStockScript, keys, args)
            .next()
            .flatMap(res -> {
                if (res != null && res == 1L) {
                    commitsCounter.increment();
                    log.info("Successfully committed reservation: sale={}, res={}", saleId, reservationId);
                    return Mono.empty();
                } else {
                    log.warn("Commit reservation rejected (not active or expired): sale={}, res={}", saleId, reservationId);
                    return Mono.error(new ReservationCommitFailedException(
                        "Cannot commit reservation " + reservationId + " on sale " + saleId + ": not active or expired"
                    ));
                }
            });
    }

    public Mono<Integer> reapExpiredReservations(UUID saleId) {
        String stockKey = InventoryKeyUtils.stockKey(saleId);
        String expKey = InventoryKeyUtils.expirationsKey(saleId);
        long nowMillis = Instant.now().toEpochMilli();

        List<String> keys = List.of(stockKey, expKey);
        List<String> args = List.of(String.valueOf(nowMillis), saleId.toString(), "100");

        return redisTemplate.execute(reapExpiredReservationsScript, keys, args)
            .next()
            .map(Long::intValue)
            .defaultIfEmpty(0)
            .doOnNext(reaped -> {
                if (reaped > 0) {
                    log.info("Active reaper reclaimed {} expired orphaned reservations for sale {}", reaped, saleId);
                }
            });
    }
}
