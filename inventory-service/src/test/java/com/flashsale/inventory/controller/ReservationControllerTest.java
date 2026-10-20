package com.flashsale.inventory.controller;

import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.common.exception.SaleNotOpenException;
import com.flashsale.common.exception.SoldOutException;
import com.flashsale.inventory.exception.GlobalExceptionHandler;
import com.flashsale.inventory.service.ReservationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReservationControllerTest {

    @Mock
    private ReservationService reservationService;

    @InjectMocks
    private ReservationController reservationController;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(reservationController)
            .controllerAdvice(new GlobalExceptionHandler())
            .build();
    }

    @Test
    void shouldReturn201WhenReservationSucceeds() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        Instant expiresAt = Instant.now().plusSeconds(120);

        ReservationResponse response = new ReservationResponse(resId, saleId, 1, expiresAt);
        when(reservationService.reserveStock(eq(saleId), any(ReserveStockRequest.class)))
            .thenReturn(Mono.just(response));

        webTestClient.post()
            .uri("/api/v1/sales/{saleId}/reservations", saleId)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""
                {
                    "shopperId": "user_42",
                    "quantity": 1,
                    "reservationTtlSeconds": 120
                }
            """)
            .exchange()
            .expectStatus().isCreated()
            .expectBody()
            .jsonPath("$.reservationId").isEqualTo(resId.toString())
            .jsonPath("$.saleId").isEqualTo(saleId.toString())
            .jsonPath("$.quantity").isEqualTo(1);
    }

    @Test
    void shouldReturn409WhenSoldOut() {
        UUID saleId = UUID.randomUUID();
        when(reservationService.reserveStock(eq(saleId), any(ReserveStockRequest.class)))
            .thenReturn(Mono.error(new SoldOutException(saleId)));

        webTestClient.post()
            .uri("/api/v1/sales/{saleId}/reservations", saleId)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""
                {
                    "shopperId": "user_42",
                    "quantity": 1
                }
            """)
            .exchange()
            .expectStatus().isEqualTo(409)
            .expectBody()
            .jsonPath("$.code").isEqualTo("SOLD_OUT");
    }

    @Test
    void shouldReturn409WhenSaleNotOpen() {
        UUID saleId = UUID.randomUUID();
        when(reservationService.reserveStock(eq(saleId), any(ReserveStockRequest.class)))
            .thenReturn(Mono.error(new SaleNotOpenException(saleId, "CLOSED")));

        webTestClient.post()
            .uri("/api/v1/sales/{saleId}/reservations", saleId)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""
                {
                    "shopperId": "user_42",
                    "quantity": 1
                }
            """)
            .exchange()
            .expectStatus().isEqualTo(409)
            .expectBody()
            .jsonPath("$.code").isEqualTo("SALE_NOT_OPEN");
    }

    @Test
    void shouldReturn200OnRelease() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        when(reservationService.releaseStock(eq(saleId), eq(resId), any()))
            .thenReturn(Mono.empty());

        webTestClient.post()
            .uri("/api/v1/sales/{saleId}/reservations/{resId}/release", saleId, resId)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""
                {
                    "reason": "PAYMENT_FAILED"
                }
            """)
            .exchange()
            .expectStatus().isOk();
    }
}
