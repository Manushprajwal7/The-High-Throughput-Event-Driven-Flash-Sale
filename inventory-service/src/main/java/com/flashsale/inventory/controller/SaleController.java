package com.flashsale.inventory.controller;

import com.flashsale.common.dto.CreateSaleRequest;
import com.flashsale.common.dto.SaleResponse;
import com.flashsale.inventory.service.SaleService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
public class SaleController {

    private final SaleService saleService;
    private final com.flashsale.inventory.service.StockRehydrationService stockRehydrationService;

    public SaleController(
        SaleService saleService,
        com.flashsale.inventory.service.StockRehydrationService stockRehydrationService
    ) {
        this.saleService = saleService;
        this.stockRehydrationService = stockRehydrationService;
    }

    @GetMapping("/api/v1/sales")
    public Flux<SaleResponse> getAllSales() {
        return saleService.getAllSales();
    }

    @GetMapping("/api/v1/sales/{saleId}")
    public Mono<SaleResponse> getSale(@PathVariable UUID saleId) {
        return saleService.getSale(saleId);
    }

    @PostMapping("/api/v1/admin/sales")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<SaleResponse> createSale(@Valid @RequestBody CreateSaleRequest request) {
        return saleService.createSale(request);
    }

    @PostMapping("/api/v1/admin/sales/{saleId}/open")
    public Mono<SaleResponse> openSale(@PathVariable UUID saleId) {
        return saleService.openSale(saleId);
    }

    @PostMapping("/api/v1/admin/sales/{saleId}/pause")
    public Mono<SaleResponse> pauseSale(@PathVariable UUID saleId) {
        return saleService.pauseSale(saleId);
    }

    @PostMapping("/api/v1/admin/sales/{saleId}/close")
    public Mono<SaleResponse> closeSale(@PathVariable UUID saleId) {
        return saleService.closeSale(saleId);
    }

    @PostMapping("/api/v1/admin/sales/{saleId}/rehydrate")
    public Mono<java.util.Map<String, Object>> rehydrateSale(
        @PathVariable UUID saleId,
        @RequestParam(defaultValue = "0") int committedUnits
    ) {
        return stockRehydrationService.rehydrateStock(saleId, committedUnits)
            .map(stock -> java.util.Map.of("saleId", saleId, "rehydratedStock", stock));
    }
}
