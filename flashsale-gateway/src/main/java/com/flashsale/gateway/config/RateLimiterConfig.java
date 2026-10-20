package com.flashsale.gateway.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

@Configuration
public class RateLimiterConfig {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterConfig.class);

    @Bean
    @Primary
    public KeyResolver clientKeyResolver() {
        return exchange -> {
            // 1. Prefer authenticated shopper header
            String shopperId = exchange.getRequest().getHeaders().getFirst("X-Shopper-Id");
            if (shopperId != null && !shopperId.isBlank()) {
                return Mono.just("shopper:" + shopperId.trim());
            }

            // 2. Check X-Forwarded-For header
            String forwardedFor = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
            if (forwardedFor != null && !forwardedFor.isBlank()) {
                String clientIp = forwardedFor.split(",")[0].trim();
                return Mono.just("ip:" + clientIp);
            }

            // 3. Fall back to remote socket address
            InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
            if (remoteAddress != null && remoteAddress.getAddress() != null) {
                return Mono.just("ip:" + remoteAddress.getAddress().getHostAddress());
            }

            return Mono.just("anonymous");
        };
    }

    @Bean
    public RedisRateLimiter redisRateLimiter(
        @Value("${flashsale.gateway.rate-limit.replenish-rate:20}") int replenishRate,
        @Value("${flashsale.gateway.rate-limit.burst-capacity:40}") int burstCapacity
    ) {
        log.info("Configured RedisRateLimiter with replenishRate={}, burstCapacity={}", replenishRate, burstCapacity);
        return new RedisRateLimiter(replenishRate, burstCapacity);
    }
}
