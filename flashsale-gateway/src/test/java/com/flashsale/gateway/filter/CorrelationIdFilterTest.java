package com.flashsale.gateway.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CorrelationIdFilterTest {

    private CorrelationIdFilter filter;

    @BeforeEach
    void setUp() {
        filter = new CorrelationIdFilter();
    }

    @Test
    void shouldGenerateCorrelationIdWhenMissingInRequest() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/sales").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicReference<String> downstreamCorrelationId = new AtomicReference<>();
        GatewayFilterChain chain = ex -> {
            downstreamCorrelationId.set(ex.getRequest().getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER));
            return Mono.empty();
        };

        filter.filter(exchange, chain).block();

        // 1. Forwarded request must have generated correlation ID
        assertThat(downstreamCorrelationId.get()).isNotNull().isNotBlank();

        // 2. Outgoing response must have same correlation ID
        String responseCorrelationId = exchange.getResponse().getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER);
        assertThat(responseCorrelationId).isEqualTo(downstreamCorrelationId.get());
    }

    @Test
    void shouldPreserveAndPropagateExistingCorrelationId() {
        String existingId = "client-trace-uuid-12345";
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders")
            .header(CorrelationIdFilter.CORRELATION_ID_HEADER, existingId)
            .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicReference<String> downstreamCorrelationId = new AtomicReference<>();
        GatewayFilterChain chain = ex -> {
            downstreamCorrelationId.set(ex.getRequest().getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER));
            return Mono.empty();
        };

        filter.filter(exchange, chain).block();

        assertThat(downstreamCorrelationId.get()).isEqualTo(existingId);
        String responseCorrelationId = exchange.getResponse().getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER);
        assertThat(responseCorrelationId).isEqualTo(existingId);
    }
}
