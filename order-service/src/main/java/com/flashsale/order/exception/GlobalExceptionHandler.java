package com.flashsale.order.exception;

import com.flashsale.common.dto.ErrorResponse;
import com.flashsale.common.exception.FlashSaleException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(FlashSaleException.class)
    public ResponseEntity<ErrorResponse> handleFlashSaleException(FlashSaleException ex, HttpServletRequest request) {
        String correlationId = getCorrelationId(request);
        HttpStatus status = HttpStatus.resolve(ex.getHttpStatus());
        if (status == null) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        log.warn("Business exception [{}]: {} (Path: {})", ex.getErrorCode(), ex.getMessage(), request.getRequestURI());

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            status.value(),
            status.getReasonPhrase(),
            ex.getErrorCode(),
            ex.getMessage(),
            request.getRequestURI(),
            correlationId,
            null
        );

        return ResponseEntity.status(status).body(response);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(MethodArgumentNotValidException ex, HttpServletRequest request) {
        String correlationId = getCorrelationId(request);
        Map<String, String> fieldErrors = new HashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(err -> fieldErrors.put(err.getField(), err.getDefaultMessage()));

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            HttpStatus.BAD_REQUEST.value(),
            HttpStatus.BAD_REQUEST.getReasonPhrase(),
            "VALIDATION_FAILED",
            "Invalid request payload",
            request.getRequestURI(),
            correlationId,
            fieldErrors
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(MissingRequestHeaderException ex, HttpServletRequest request) {
        String correlationId = getCorrelationId(request);

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            HttpStatus.BAD_REQUEST.value(),
            HttpStatus.BAD_REQUEST.getReasonPhrase(),
            "MISSING_HEADER",
            ex.getMessage(),
            request.getRequestURI(),
            correlationId,
            null
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, HttpServletRequest request) {
        String correlationId = getCorrelationId(request);

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            HttpStatus.BAD_REQUEST.value(),
            HttpStatus.BAD_REQUEST.getReasonPhrase(),
            "INVALID_ARGUMENT",
            ex.getMessage(),
            request.getRequestURI(),
            correlationId,
            null
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex, HttpServletRequest request) {
        String correlationId = getCorrelationId(request);
        log.error("Unhandled internal server error on {}: ", request.getRequestURI(), ex);

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            HttpStatus.INTERNAL_SERVER_ERROR.value(),
            HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            "INTERNAL_SERVER_ERROR",
            "An unexpected internal error occurred",
            request.getRequestURI(),
            correlationId,
            null
        );

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    private String getCorrelationId(HttpServletRequest request) {
        String id = request.getHeader("X-Correlation-Id");
        return (id != null && !id.isBlank()) ? id : UUID.randomUUID().toString();
    }
}
