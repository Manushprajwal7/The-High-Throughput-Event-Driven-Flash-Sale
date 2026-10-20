package com.flashsale.order.client;

import com.flashsale.common.dto.ReleaseReservationRequest;
import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.common.dto.ReserveStockRequest;
import com.flashsale.common.exception.SaleNotFoundException;
import com.flashsale.common.exception.SaleNotOpenException;
import com.flashsale.common.exception.SoldOutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.UUID;

@Component
public class InventoryClient {
    private static final Logger log = LoggerFactory.getLogger(InventoryClient.class);

    private final RestClient restClient;

    public InventoryClient(
        @Value("${flashsale.services.inventory-url:http://localhost:8082}") String inventoryBaseUrl,
        RestClient.Builder restClientBuilder
    ) {
        this.restClient = restClientBuilder
            .baseUrl(inventoryBaseUrl)
            .build();
    }

    public ReservationResponse reserveStock(UUID saleId, String shopperId, int quantity, String correlationId) {
        ReserveStockRequest request = new ReserveStockRequest(shopperId, quantity, 120);

        return restClient.post()
            .uri("/api/v1/sales/{saleId}/reservations", saleId)
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Correlation-Id", correlationId)
            .body(request)
            .retrieve()
            .onStatus(HttpStatusCode::is4xxClientError, (req, res) -> {
                int status = res.getStatusCode().value();
                if (status == 404) {
                    throw new SaleNotFoundException(saleId);
                } else if (status == 409) {
                    throw new SoldOutException(saleId);
                }
            })
            .body(ReservationResponse.class);
    }

    public void releaseStock(UUID saleId, UUID reservationId, String reason, String correlationId) {
        try {
            ReleaseReservationRequest request = new ReleaseReservationRequest(reason);

            restClient.post()
                .uri("/api/v1/sales/{saleId}/reservations/{reservationId}/release", saleId, reservationId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Correlation-Id", correlationId)
                .body(request)
                .retrieve()
                .toBodilessEntity();

            log.info("Released stock for reservation {} on sale {}", reservationId, saleId);
        } catch (Exception ex) {
            log.warn("Failed to release stock for reservation {} on sale {}: {}", reservationId, saleId, ex.getMessage());
        }
    }
}
