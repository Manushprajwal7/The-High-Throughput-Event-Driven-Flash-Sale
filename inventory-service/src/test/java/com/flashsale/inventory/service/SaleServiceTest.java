package com.flashsale.inventory.service;

import com.flashsale.common.dto.CreateSaleRequest;
import com.flashsale.common.dto.SaleResponse;
import com.flashsale.common.exception.IllegalStateTransitionException;
import com.flashsale.common.exception.SaleNotFoundException;
import com.flashsale.common.model.SaleStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SaleServiceTest {

    @Mock
    private ReactiveStringRedisTemplate redisTemplate;

    @Mock
    private ReactiveHashOperations<String, Object, Object> hashOps;

    @Mock
    private ReactiveValueOperations<String, String> valueOps;

    @Mock
    private ReactiveSetOperations<String, String> setOps;

    private SaleService saleService;

    @BeforeEach
    void setUp() {
        saleService = new SaleService(redisTemplate);
    }

    @Test
    void shouldCreateSaleSuccessfully() {
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(redisTemplate.opsForSet()).thenReturn(setOps);

        when(hashOps.putAll(anyString(), anyMap())).thenReturn(Mono.just(true));
        when(valueOps.set(anyString(), anyString())).thenReturn(Mono.just(true));
        when(setOps.add(anyString(), anyString())).thenReturn(Mono.just(1L));

        Instant now = Instant.now();
        CreateSaleRequest req = new CreateSaleRequest(
            "PlayStation 5 Pro",
            100,
            now.plusSeconds(60),
            now.plusSeconds(3600)
        );

        StepVerifier.create(saleService.createSale(req))
            .assertNext(res -> {
                assertThat(res.saleId()).isNotNull();
                assertThat(res.name()).isEqualTo("PlayStation 5 Pro");
                assertThat(res.totalCapacity()).isEqualTo(100);
                assertThat(res.availableStock()).isEqualTo(100);
                assertThat(res.status()).isEqualTo(SaleStatus.DRAFT);
            })
            .verifyComplete();
    }

    @Test
    void shouldReturnSaleWhenFound() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);

        Instant now = Instant.now();
        when(hashOps.entries("flashsale:sale:" + saleId)).thenReturn(Flux.just(
            new AbstractMap.SimpleEntry<>("id", saleId.toString()),
            new AbstractMap.SimpleEntry<>("name", "Flash Drop"),
            new AbstractMap.SimpleEntry<>("totalCapacity", "50"),
            new AbstractMap.SimpleEntry<>("status", "OPEN"),
            new AbstractMap.SimpleEntry<>("startsAt", now.toString()),
            new AbstractMap.SimpleEntry<>("endsAt", now.plusSeconds(600).toString()),
            new AbstractMap.SimpleEntry<>("createdAt", now.toString())
        ));
        when(valueOps.get("flashsale:stock:" + saleId)).thenReturn(Mono.just("42"));

        StepVerifier.create(saleService.getSale(saleId))
            .assertNext(res -> {
                assertThat(res.saleId()).isEqualTo(saleId);
                assertThat(res.availableStock()).isEqualTo(42);
                assertThat(res.status()).isEqualTo(SaleStatus.OPEN);
            })
            .verifyComplete();
    }

    @Test
    void shouldThrowNotFoundWhenSaleDoesNotExist() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries("flashsale:sale:" + saleId)).thenReturn(Flux.empty());

        StepVerifier.create(saleService.getSale(saleId))
            .expectError(SaleNotFoundException.class)
            .verify();
    }

    @Test
    void shouldTransitionDraftToOpen() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);

        when(hashOps.get("flashsale:sale:" + saleId, "status")).thenReturn(Mono.just("DRAFT"));
        when(hashOps.put("flashsale:sale:" + saleId, "status", "OPEN")).thenReturn(Mono.just(true));

        Instant now = Instant.now();
        when(hashOps.entries("flashsale:sale:" + saleId)).thenReturn(Flux.just(
            new AbstractMap.SimpleEntry<>("id", saleId.toString()),
            new AbstractMap.SimpleEntry<>("name", "Flash Drop"),
            new AbstractMap.SimpleEntry<>("totalCapacity", "50"),
            new AbstractMap.SimpleEntry<>("status", "OPEN"),
            new AbstractMap.SimpleEntry<>("startsAt", now.toString()),
            new AbstractMap.SimpleEntry<>("endsAt", now.plusSeconds(600).toString()),
            new AbstractMap.SimpleEntry<>("createdAt", now.toString())
        ));
        when(valueOps.get("flashsale:stock:" + saleId)).thenReturn(Mono.just("50"));

        StepVerifier.create(saleService.openSale(saleId))
            .assertNext(res -> assertThat(res.status()).isEqualTo(SaleStatus.OPEN))
            .verifyComplete();
    }

    @Test
    void shouldRejectOpeningClosedSale() {
        UUID saleId = UUID.randomUUID();
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(hashOps.get("flashsale:sale:" + saleId, "status")).thenReturn(Mono.just("CLOSED"));

        StepVerifier.create(saleService.openSale(saleId))
            .expectError(IllegalStateTransitionException.class)
            .verify();
    }
}
