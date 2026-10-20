package com.flashsale.inventory.controller;

import com.flashsale.common.dto.CommitReservationRequest;
import com.flashsale.common.dto.ReleaseReservationRequest;
import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.inventory.service.ReservationService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/sales/{saleId}/reservations")
public class ReservationController {

    private final ReservationService reservationService;

    public ReservationController(ReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ReservationResponse> reserveStock(
        @PathVariable UUID saleId,
        @Valid @RequestBody ReserveStockRequest request
    ) {
        return reservationService.reserveStock(saleId, request);
    }

    @PostMapping("/{reservationId}/commit")
    @ResponseStatus(HttpStatus.OK)
    public Mono<Void> commitReservation(
        @PathVariable UUID saleId,
        @PathVariable UUID reservationId,
        @RequestBody(required = false) CommitReservationRequest request
    ) {
        return reservationService.commitStock(saleId, reservationId);
    }

    @PostMapping("/{reservationId}/release")
    @ResponseStatus(HttpStatus.OK)
    public Mono<Void> releaseReservation(
        @PathVariable UUID saleId,
        @PathVariable UUID reservationId,
        @RequestBody(required = false) ReleaseReservationRequest request
    ) {
        String reason = (request != null && request.reason() != null) ? request.reason() : "MANUAL_OR_TIMEOUT";
        return reservationService.releaseStock(saleId, reservationId, reason);
    }

    @PostMapping("/reap-expired")
    @ResponseStatus(HttpStatus.OK)
    public Mono<Integer> reapExpiredReservations(@PathVariable UUID saleId) {
        return reservationService.reapExpiredReservations(saleId);
    }
}
