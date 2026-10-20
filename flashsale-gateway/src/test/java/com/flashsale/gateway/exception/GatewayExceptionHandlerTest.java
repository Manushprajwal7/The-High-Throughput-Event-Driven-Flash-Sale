package com.flashsale.gateway.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashsale.common.dto.ErrorResponse;
import com.flashsale.gateway.filter.CorrelationIdFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ResponseStatusException;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayExceptionHandlerTest {

    private GatewayExceptionHandler exceptionHandler;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        exceptionHandler = new GatewayExceptionHandler(objectMapper);
    }

    @Test
    void shouldFormatRateLimitExceededAs429ErrorResponse() throws Exception {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders")
            .header(CorrelationIdFilter.CORRELATION_ID_HEADER, "corr-rate-limit-1")
            .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ResponseStatusException ex = new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Rate limit exceeded");
        exceptionHandler.handle(exchange, ex).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER))
            .isEqualTo("corr-rate-limit-1");

        String responseBody = exchange.getResponse().getBodyAsString().block();
        ErrorResponse errorResponse = objectMapper.readValue(responseBody, ErrorResponse.class);

        assertThat(errorResponse.status()).isEqualTo(429);
        assertThat(errorResponse.code()).isEqualTo("RATE_LIMIT_EXCEEDED");
        assertThat(errorResponse.correlationId()).isEqualTo("corr-rate-limit-1");
    }

    @Test
    void shouldFormatConnectExceptionAs503ServiceDegraded() throws Exception {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/sales")
            .header(CorrelationIdFilter.CORRELATION_ID_HEADER, "corr-conn-refused")
            .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ConnectException ex = new ConnectException("Connection refused: no further information");
        exceptionHandler.handle(exchange, ex).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        String responseBody = exchange.getResponse().getBodyAsString().block();
        ErrorResponse errorResponse = objectMapper.readValue(responseBody, ErrorResponse.class);

        assertThat(errorResponse.status()).isEqualTo(503);
        assertThat(errorResponse.code()).isEqualTo("SERVICE_DEGRADED");
        assertThat(errorResponse.correlationId()).isEqualTo("corr-conn-refused");
    }

    @Test
    void shouldFormatResourceNotFoundAs404ErrorResponse() throws Exception {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/unknown-path")
            .header(CorrelationIdFilter.CORRELATION_ID_HEADER, "corr-not-found")
            .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ResponseStatusException ex = new ResponseStatusException(HttpStatus.NOT_FOUND, "Route not found");
        exceptionHandler.handle(exchange, ex).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        String responseBody = exchange.getResponse().getBodyAsString().block();
        ErrorResponse errorResponse = objectMapper.readValue(responseBody, ErrorResponse.class);

        assertThat(errorResponse.status()).isEqualTo(404);
        assertThat(errorResponse.code()).isEqualTo("RESOURCE_NOT_FOUND");
    }
}
