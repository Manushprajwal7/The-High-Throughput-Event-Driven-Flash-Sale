package com.flashsale.gateway.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterConfigTest {

    private RateLimiterConfig config;
    private KeyResolver keyResolver;

    @BeforeEach
    void setUp() {
        config = new RateLimiterConfig();
        keyResolver = config.clientKeyResolver();
    }

    @Test
    void shouldResolveShopperIdHeaderWhenPresent() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders")
            .header("X-Shopper-Id", "shopper_456")
            .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        String key = keyResolver.resolve(exchange).block();
        assertThat(key).isEqualTo("shopper:shopper_456");
    }

    @Test
    void shouldResolveForwardedForIpWhenShopperIdAbsent() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders")
            .header("X-Forwarded-For", "203.0.113.195, 70.41.3.18")
            .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        String key = keyResolver.resolve(exchange).block();
        assertThat(key).isEqualTo("ip:203.0.113.195");
    }

    @Test
    void shouldResolveRemoteSocketAddressWhenHeadersAbsent() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders")
            .remoteAddress(new InetSocketAddress("192.168.1.100", 54321))
            .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        String key = keyResolver.resolve(exchange).block();
        assertThat(key).isEqualTo("ip:192.168.1.100");
    }

    @Test
    void shouldResolveAnonymousWhenNoIdentifiersAvailable() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        String key = keyResolver.resolve(exchange).block();
        assertThat(key).isEqualTo("anonymous");
    }
}
