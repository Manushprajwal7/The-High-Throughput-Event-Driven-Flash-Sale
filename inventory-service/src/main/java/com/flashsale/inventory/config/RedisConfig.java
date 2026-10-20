package com.flashsale.inventory.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class RedisConfig {

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }

    @Bean
    public RedisScript<Long> reserveStockScript() {
        return RedisScript.of(new ClassPathResource("scripts/reserve_stock.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> releaseStockScript() {
        return RedisScript.of(new ClassPathResource("scripts/release_stock.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> commitStockScript() {
        return RedisScript.of(new ClassPathResource("scripts/commit_stock.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> reapExpiredReservationsScript() {
        return RedisScript.of(new ClassPathResource("scripts/reap_expired_reservations.lua"), Long.class);
    }
}
