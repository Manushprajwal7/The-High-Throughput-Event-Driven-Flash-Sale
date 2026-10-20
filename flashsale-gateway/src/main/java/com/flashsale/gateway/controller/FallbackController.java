package com.flashsale.gateway.controller;

import com.flashsale.common.dto.ErrorResponse;
import com.flashsale.gateway.filter.CorrelationIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/fallback")
public class FallbackController {

    private static final Logger log = LoggerFactory.getLogger(FallbackController.class);

    @RequestMapping("/service-degraded")
    public Mono<ResponseEntity<ErrorResponse>> handleServiceDegraded(ServerHttpRequest request) {
        String correlationId = request.getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        String path = request.getPath().value();
        log.warn("Circuit breaker fallback triggered for path: {} with correlation ID: {}", path, correlationId);

        ErrorResponse errorResponse = ErrorResponse.of(
            HttpStatus.SERVICE_UNAVAILABLE.value(),
            HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
            "SERVICE_DEGRADED",
            "Downstream service is temporarily unavailable or degraded. Please retry later.",
            path,
            correlationId
        );

        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .header(CorrelationIdFilter.CORRELATION_ID_HEADER, correlationId)
            .body(errorResponse));
    }
}
