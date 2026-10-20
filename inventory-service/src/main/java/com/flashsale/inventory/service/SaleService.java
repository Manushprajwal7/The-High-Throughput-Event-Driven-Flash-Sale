package com.flashsale.inventory.service;

import com.flashsale.common.dto.CreateSaleRequest;
import com.flashsale.common.dto.SaleResponse;
import com.flashsale.common.exception.IllegalStateTransitionException;
import com.flashsale.common.exception.SaleNotFoundException;
import com.flashsale.common.model.SaleStatus;
import com.flashsale.inventory.util.InventoryKeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class SaleService {
    private static final Logger log = LoggerFactory.getLogger(SaleService.class);

    private final ReactiveStringRedisTemplate redisTemplate;

    public SaleService(ReactiveStringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Mono<SaleResponse> createSale(CreateSaleRequest req) {
        UUID saleId = UUID.randomUUID();
        Instant now = Instant.now();
        String saleKey = InventoryKeyUtils.saleKey(saleId);
        String stockKey = InventoryKeyUtils.stockKey(saleId);

        Map<String, String> fields = new HashMap<>();
        fields.put("id", saleId.toString());
        fields.put("name", req.name());
        fields.put("totalCapacity", String.valueOf(req.totalCapacity()));
        fields.put("status", SaleStatus.DRAFT.name());
        fields.put("startsAt", req.startsAt().toString());
        fields.put("endsAt", req.endsAt().toString());
        fields.put("createdAt", now.toString());

        return redisTemplate.opsForHash().putAll(saleKey, fields)
            .then(redisTemplate.opsForValue().set(stockKey, String.valueOf(req.totalCapacity())))
            .then(redisTemplate.opsForSet().add(InventoryKeyUtils.salesIndexKey(), saleId.toString()))
            .thenReturn(new SaleResponse(
                saleId,
                req.name(),
                req.totalCapacity(),
                req.totalCapacity(),
                SaleStatus.DRAFT,
                req.startsAt(),
                req.endsAt(),
                now
            ))
            .doOnSuccess(res -> log.info("Created flash sale: id={}, name={}, capacity={}", saleId, req.name(), req.totalCapacity()));
    }

    public Mono<SaleResponse> getSale(UUID saleId) {
        String saleKey = InventoryKeyUtils.saleKey(saleId);
        String stockKey = InventoryKeyUtils.stockKey(saleId);

        return redisTemplate.opsForHash().entries(saleKey)
            .collectMap(e -> e.getKey().toString(), e -> e.getValue().toString())
            .flatMap(map -> {
                if (map.isEmpty()) {
                    return Mono.error(new SaleNotFoundException(saleId));
                }
                return redisTemplate.opsForValue().get(stockKey)
                    .defaultIfEmpty("0")
                    .map(stockStr -> mapToSaleResponse(map, Integer.parseInt(stockStr)));
            });
    }

    public Flux<SaleResponse> getAllSales() {
        return redisTemplate.opsForSet().members(InventoryKeyUtils.salesIndexKey())
            .flatMap(idStr -> getSale(UUID.fromString(idStr)).onErrorResume(SaleNotFoundException.class, ex -> Mono.empty()));
    }

    public Mono<SaleResponse> openSale(UUID saleId) {
        return transitionSaleStatus(saleId, SaleStatus.OPEN, (current) -> {
            if (current == SaleStatus.CLOSED) {
                return "Cannot open a CLOSED sale";
            }
            return null;
        });
    }

    public Mono<SaleResponse> pauseSale(UUID saleId) {
        return transitionSaleStatus(saleId, SaleStatus.PAUSED, (current) -> {
            if (current != SaleStatus.OPEN) {
                return "Only OPEN sales can be paused (current: " + current + ")";
            }
            return null;
        });
    }

    public Mono<SaleResponse> closeSale(UUID saleId) {
        return transitionSaleStatus(saleId, SaleStatus.CLOSED, (current) -> null);
    }

    private interface TransitionValidator {
        String validate(SaleStatus currentStatus);
    }

    private Mono<SaleResponse> transitionSaleStatus(UUID saleId, SaleStatus newStatus, TransitionValidator validator) {
        String saleKey = InventoryKeyUtils.saleKey(saleId);

        return redisTemplate.opsForHash().get(saleKey, "status")
            .switchIfEmpty(Mono.error(new SaleNotFoundException(saleId)))
            .flatMap(statusObj -> {
                SaleStatus current = SaleStatus.valueOf(statusObj.toString());
                String error = validator.validate(current);
                if (error != null) {
                    return Mono.error(new IllegalStateTransitionException(error));
                }
                return redisTemplate.opsForHash().put(saleKey, "status", newStatus.name())
                    .then(getSale(saleId))
                    .doOnSuccess(s -> log.info("Transitioned sale {} from {} to {}", saleId, current, newStatus));
            });
    }

    private SaleResponse mapToSaleResponse(Map<String, String> map, int availableStock) {
        return new SaleResponse(
            UUID.fromString(map.get("id")),
            map.get("name"),
            Integer.parseInt(map.get("totalCapacity")),
            availableStock,
            SaleStatus.valueOf(map.get("status")),
            Instant.parse(map.get("startsAt")),
            Instant.parse(map.get("endsAt")),
            Instant.parse(map.get("createdAt"))
        );
    }
}
