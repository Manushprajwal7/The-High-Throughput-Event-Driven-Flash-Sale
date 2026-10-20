package com.flashsale.gateway;

import com.flashsale.common.dto.ErrorResponse;
import com.flashsale.gateway.filter.CorrelationIdFilter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@ActiveProfiles("test")
class CircuitBreakerFallbackIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void shouldTriggerCircuitBreakerFallbackWhenDownstreamIsUnreachable() {
        String clientCorrelationId = "test-cb-trace-777";

        webTestClient.get()
            .uri("/test/orders/sample-order-id")
            .header(CorrelationIdFilter.CORRELATION_ID_HEADER, clientCorrelationId)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
            .expectHeader().valueEquals(CorrelationIdFilter.CORRELATION_ID_HEADER, clientCorrelationId)
            .expectBody(ErrorResponse.class)
            .value(errorResponse -> {
                assertThat(errorResponse).isNotNull();
                assertThat(errorResponse.status()).isEqualTo(503);
                assertThat(errorResponse.code()).isEqualTo("SERVICE_DEGRADED");
                assertThat(errorResponse.message()).contains("temporarily unavailable");
                assertThat(errorResponse.correlationId()).isEqualTo(clientCorrelationId);
            });
    }

    @Test
    void shouldPropagateCorrelationIdOnDirectFallbackCall() {
        webTestClient.get()
            .uri("/fallback/service-degraded")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
            .expectHeader().exists(CorrelationIdFilter.CORRELATION_ID_HEADER)
            .expectBody(ErrorResponse.class)
            .value(errorResponse -> {
                assertThat(errorResponse.status()).isEqualTo(503);
                assertThat(errorResponse.code()).isEqualTo("SERVICE_DEGRADED");
                assertThat(errorResponse.correlationId()).isNotNull();
            });
    }
}
