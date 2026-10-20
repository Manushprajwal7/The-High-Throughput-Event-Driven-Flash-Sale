package com.flashsale.gateway.controller;

import com.flashsale.common.dto.ErrorResponse;
import com.flashsale.gateway.filter.CorrelationIdFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

class FallbackControllerTest {

    private FallbackController controller;

    @BeforeEach
    void setUp() {
        controller = new FallbackController();
    }

    @Test
    void shouldReturn503ServiceDegradedWithCorrelationId() {
        String correlationId = "corr-fallback-test-123";
        MockServerHttpRequest request = MockServerHttpRequest.get("/fallback/service-degraded")
            .header(CorrelationIdFilter.CORRELATION_ID_HEADER, correlationId)
            .build();

        ResponseEntity<ErrorResponse> response = controller.handleServiceDegraded(request).block();

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER)).isEqualTo(correlationId);

        ErrorResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo(503);
        assertThat(body.code()).isEqualTo("SERVICE_DEGRADED");
        assertThat(body.message()).contains("temporarily unavailable");
        assertThat(body.correlationId()).isEqualTo(correlationId);
    }
}
