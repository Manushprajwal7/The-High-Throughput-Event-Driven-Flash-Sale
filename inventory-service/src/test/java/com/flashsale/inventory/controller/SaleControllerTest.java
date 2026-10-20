package com.flashsale.inventory.controller;

import com.flashsale.common.dto.CreateSaleRequest;
import com.flashsale.common.dto.SaleResponse;
import com.flashsale.common.exception.SaleNotFoundException;
import com.flashsale.common.model.SaleStatus;
import com.flashsale.inventory.exception.GlobalExceptionHandler;
import com.flashsale.inventory.service.SaleService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SaleControllerTest {

    @Mock
    private SaleService saleService;

    @InjectMocks
    private SaleController saleController;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(saleController)
            .controllerAdvice(new GlobalExceptionHandler())
            .build();
    }

    @Test
    void shouldCreateSaleWith201Status() {
        UUID saleId = UUID.randomUUID();
        Instant now = Instant.now();
        SaleResponse response = new SaleResponse(
            saleId,
            "PS5 Drop",
            100,
            100,
            SaleStatus.DRAFT,
            now.plusSeconds(60),
            now.plusSeconds(3600),
            now
        );

        when(saleService.createSale(any(CreateSaleRequest.class))).thenReturn(Mono.just(response));

        webTestClient.post()
            .uri("/api/v1/admin/sales")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""
                {
                    "name": "PS5 Drop",
                    "totalCapacity": 100,
                    "startsAt": "%s",
                    "endsAt": "%s"
                }
            """.formatted(now.plusSeconds(60), now.plusSeconds(3600)))
            .exchange()
            .expectStatus().isCreated()
            .expectBody()
            .jsonPath("$.saleId").isEqualTo(saleId.toString())
            .jsonPath("$.name").isEqualTo("PS5 Drop")
            .jsonPath("$.totalCapacity").isEqualTo(100)
            .jsonPath("$.status").isEqualTo("DRAFT");
    }

    @Test
    void shouldReturn404WhenSaleNotFound() {
        UUID saleId = UUID.randomUUID();
        when(saleService.getSale(saleId)).thenReturn(Mono.error(new SaleNotFoundException(saleId)));

        webTestClient.get()
            .uri("/api/v1/sales/{saleId}", saleId)
            .exchange()
            .expectStatus().isNotFound()
            .expectBody()
            .jsonPath("$.code").isEqualTo("SALE_NOT_FOUND");
    }

    @Test
    void shouldOpenSaleWith200Status() {
        UUID saleId = UUID.randomUUID();
        Instant now = Instant.now();
        SaleResponse response = new SaleResponse(
            saleId,
            "PS5 Drop",
            100,
            100,
            SaleStatus.OPEN,
            now.minusSeconds(60),
            now.plusSeconds(3600),
            now
        );

        when(saleService.openSale(saleId)).thenReturn(Mono.just(response));

        webTestClient.post()
            .uri("/api/v1/admin/sales/{saleId}/open", saleId)
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.status").isEqualTo("OPEN");
    }
}
