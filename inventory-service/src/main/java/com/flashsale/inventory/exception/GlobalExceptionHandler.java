package com.flashsale.inventory.exception;

import com.flashsale.common.dto.ErrorResponse;
import com.flashsale.common.exception.FlashSaleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebExchange;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(FlashSaleException.class)
    public ResponseEntity<ErrorResponse> handleFlashSaleException(FlashSaleException ex, ServerWebExchange exchange) {
        String correlationId = getCorrelationId(exchange);
        HttpStatus status = HttpStatus.resolve(ex.getHttpStatus());
        if (status == null) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        log.warn("Business exception [{}]: {} (Path: {})", ex.getErrorCode(), ex.getMessage(), exchange.getRequest().getPath());

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            status.value(),
            status.getReasonPhrase(),
            ex.getErrorCode(),
            ex.getMessage(),
            exchange.getRequest().getPath().value(),
            correlationId,
            null
        );

        return ResponseEntity.status(status).body(response);
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(WebExchangeBindException ex, ServerWebExchange exchange) {
        String correlationId = getCorrelationId(exchange);
        Map<String, String> fieldErrors = new HashMap<>();
        ex.getFieldErrors().forEach(err -> fieldErrors.put(err.getField(), err.getDefaultMessage()));

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            HttpStatus.BAD_REQUEST.value(),
            HttpStatus.BAD_REQUEST.getReasonPhrase(),
            "VALIDATION_FAILED",
            "Invalid request payload",
            exchange.getRequest().getPath().value(),
            correlationId,
            fieldErrors
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, ServerWebExchange exchange) {
        String correlationId = getCorrelationId(exchange);

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            HttpStatus.BAD_REQUEST.value(),
            HttpStatus.BAD_REQUEST.getReasonPhrase(),
            "INVALID_ARGUMENT",
            ex.getMessage(),
            exchange.getRequest().getPath().value(),
            correlationId,
            null
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex, ServerWebExchange exchange) {
        String correlationId = getCorrelationId(exchange);
        log.error("Unhandled internal server error on {}: ", exchange.getRequest().getPath(), ex);

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            HttpStatus.INTERNAL_SERVER_ERROR.value(),
            HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            "INTERNAL_SERVER_ERROR",
            "An unexpected internal error occurred",
            exchange.getRequest().getPath().value(),
            correlationId,
            null
        );

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    private String getCorrelationId(ServerWebExchange exchange) {
        String id = exchange.getRequest().getHeaders().getFirst("X-Correlation-Id");
        return (id != null && !id.isBlank()) ? id : UUID.randomUUID().toString();
    }
}
