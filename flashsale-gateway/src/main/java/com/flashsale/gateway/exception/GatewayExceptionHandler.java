package com.flashsale.gateway.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashsale.common.dto.ErrorResponse;
import com.flashsale.gateway.filter.CorrelationIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class GatewayExceptionHandler implements ErrorWebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GatewayExceptionHandler.class);

    private final ObjectMapper objectMapper;

    public GatewayExceptionHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        ServerHttpResponse response = exchange.getResponse();

        if (response.isCommitted()) {
            return Mono.error(ex);
        }

        String correlationId = exchange.getRequest().getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        HttpStatus status;
        String errorCode;
        String errorMessage;

        if (ex instanceof ResponseStatusException rse) {
            status = HttpStatus.resolve(rse.getStatusCode().value());
            if (status == null) {
                status = HttpStatus.INTERNAL_SERVER_ERROR;
            }
            if (status == HttpStatus.TOO_MANY_REQUESTS) {
                errorCode = "RATE_LIMIT_EXCEEDED";
                errorMessage = "Rate limit exceeded. Please retry later.";
            } else if (status == HttpStatus.NOT_FOUND) {
                errorCode = "RESOURCE_NOT_FOUND";
                errorMessage = "Requested route or resource not found.";
            } else {
                errorCode = "GATEWAY_ERROR";
                errorMessage = rse.getReason() != null ? rse.getReason() : ex.getMessage();
            }
        } else if (ex instanceof ConnectException) {
            status = HttpStatus.SERVICE_UNAVAILABLE;
            errorCode = "SERVICE_DEGRADED";
            errorMessage = "Downstream service is currently unreachable.";
        } else {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            errorCode = "INTERNAL_SERVER_ERROR";
            errorMessage = "An unexpected error occurred while processing the request.";
        }

        String path = exchange.getRequest().getPath().value();
        log.warn("Gateway error [status={}, code={}] on path {}: {}", status.value(), errorCode, path, ex.getMessage());

        ErrorResponse errorResponse = ErrorResponse.of(
            status.value(),
            status.getReasonPhrase(),
            errorCode,
            errorMessage,
            path,
            correlationId
        );

        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().set(CorrelationIdFilter.CORRELATION_ID_HEADER, correlationId);

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(errorResponse);
        } catch (JsonProcessingException jpe) {
            bytes = ("{\"status\":" + status.value() + ",\"error\":\"Internal Server Error\"}").getBytes(StandardCharsets.UTF_8);
        }

        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }
}
