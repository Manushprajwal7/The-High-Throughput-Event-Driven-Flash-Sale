package com.flashsale.payment.client;

import com.flashsale.common.dto.CommitReservationRequest;
import com.flashsale.common.dto.ReleaseReservationRequest;
import com.flashsale.common.exception.StockCommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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

    public void commitStock(UUID saleId, UUID reservationId, String correlationId) {
        try {
            restClient.post()
                .uri("/api/v1/sales/{saleId}/reservations/{reservationId}/commit", saleId, reservationId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Correlation-Id", correlationId != null ? correlationId : UUID.randomUUID().toString())
                .body(new CommitReservationRequest())
                .retrieve()
                .toBodilessEntity();

            log.info("Committed stock in inventory service for reservation {} on sale {}", reservationId, saleId);
        } catch (Exception ex) {
            log.error("Failed to commit stock for reservation {} on sale {}: {}", reservationId, saleId, ex.getMessage());
            throw new StockCommitFailedException("Failed to commit stock for reservation " + reservationId + " on sale " + saleId, ex);
        }
    }

    public void releaseStock(UUID saleId, UUID reservationId, String reason, String correlationId) {
        try {
            ReleaseReservationRequest request = new ReleaseReservationRequest(reason);

            restClient.post()
                .uri("/api/v1/sales/{saleId}/reservations/{reservationId}/release", saleId, reservationId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Correlation-Id", correlationId != null ? correlationId : UUID.randomUUID().toString())
                .body(request)
                .retrieve()
                .toBodilessEntity();

            log.info("Released stock in inventory service for reservation {} on sale {} (reason={})",
                reservationId, saleId, reason);
        } catch (Exception ex) {
            log.warn("Failed to release stock for reservation {} on sale {}: {}", reservationId, saleId, ex.getMessage());
        }
    }
}
