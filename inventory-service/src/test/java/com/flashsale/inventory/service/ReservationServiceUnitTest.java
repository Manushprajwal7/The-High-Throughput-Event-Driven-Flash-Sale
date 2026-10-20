package com.flashsale.inventory.service;

import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.common.exception.InvalidReservationException;
import com.flashsale.common.exception.ReservationCommitFailedException;
import com.flashsale.common.exception.SaleNotFoundException;
import com.flashsale.common.exception.SaleNotOpenException;
import com.flashsale.common.exception.SoldOutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReservationServiceUnitTest {

    @Mock
    private ReactiveStringRedisTemplate redisTemplate;

    @Mock
    private RedisScript<Long> reserveStockScript;

    @Mock
    private RedisScript<Long> releaseStockScript;

    @Mock
    private RedisScript<Long> commitStockScript;

    private ReservationService reservationService;

    @BeforeEach
    void setUp() {
        reservationService = new ReservationService(
            redisTemplate,
            reserveStockScript,
            releaseStockScript,
            commitStockScript
        );
    }

    @Test
    void shouldSuccessfullyReserveStockWhenAvailable() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(1L));

        ReserveStockRequest req = new ReserveStockRequest("shopper_1", 1, 120);

        StepVerifier.create(reservationService.reserveStock(saleId, req))
            .assertNext(res -> {
                assertThat(res.saleId()).isEqualTo(saleId);
                assertThat(res.reservationId()).isNotNull();
                assertThat(res.quantity()).isEqualTo(1);
                assertThat(res.expiresAt()).isNotNull();
            })
            .verifyComplete();
    }

    @Test
    void shouldThrowSoldOutWhenStockIsInsufficient() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(0L));

        ReserveStockRequest req = new ReserveStockRequest("shopper_1", 2, 120);

        StepVerifier.create(reservationService.reserveStock(saleId, req))
            .expectError(SoldOutException.class)
            .verify();
    }

    @Test
    void shouldThrowSaleNotFoundWhenSaleKeyDoesNotExist() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(-1L));

        ReserveStockRequest req = new ReserveStockRequest("shopper_1", 1, 120);

        StepVerifier.create(reservationService.reserveStock(saleId, req))
            .expectError(SaleNotFoundException.class)
            .verify();
    }

    @Test
    void shouldThrowSaleNotOpenWhenSaleIsNotActive() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(-2L));

        ReserveStockRequest req = new ReserveStockRequest("shopper_1", 1, 120);

        StepVerifier.create(reservationService.reserveStock(saleId, req))
            .expectError(SaleNotOpenException.class)
            .verify();
    }

    @Test
    void shouldRejectInvalidQuantityImmediately() {
        UUID saleId = UUID.randomUUID();
        ReserveStockRequest req = new ReserveStockRequest("shopper_1", 0, 120);

        StepVerifier.create(reservationService.reserveStock(saleId, req))
            .expectError(InvalidReservationException.class)
            .verify();
    }

    @Test
    void shouldReleaseStockIdempotently() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(1L));

        StepVerifier.create(reservationService.releaseStock(saleId, resId, "PAYMENT_FAILED"))
            .verifyComplete();

        // Repeated release (returning 0L) also completes normally without error
        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(0L));

        StepVerifier.create(reservationService.releaseStock(saleId, resId, "REPEATED_ATTEMPT"))
            .verifyComplete();
    }

    @Test
    void shouldCommitStockIdempotently() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(1L));

        StepVerifier.create(reservationService.commitStock(saleId, resId))
            .verifyComplete();
    }

    @Test
    void shouldThrowWhenCommitStockFails() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(0L));

        StepVerifier.create(reservationService.commitStock(saleId, resId))
            .expectError(ReservationCommitFailedException.class)
            .verify();
    }

    @Test
    void shouldReapExpiredReservations() {
        UUID saleId = UUID.randomUUID();

        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
            .thenReturn(Flux.just(3L));

        StepVerifier.create(reservationService.reapExpiredReservations(saleId))
            .expectNext(3)
            .verifyComplete();
    }
}
