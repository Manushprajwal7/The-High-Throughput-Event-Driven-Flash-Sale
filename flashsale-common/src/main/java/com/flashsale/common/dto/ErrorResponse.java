package com.flashsale.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * Standard error response body for all non-2xx API responses.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
    Instant timestamp,
    int status,
    String error,
    String code,
    String message,
    String path,
    String correlationId,
    Map<String, String> details
) {
    public static ErrorResponse of(int status, String error, String code, String message, String path, String correlationId) {
        return new ErrorResponse(Instant.now(), status, error, code, message, path, correlationId, null);
    }

    public static ErrorResponse of(int status, String error, String code, String message, String path, String correlationId, Map<String, String> details) {
        return new ErrorResponse(Instant.now(), status, error, code, message, path, correlationId, details);
    }
}
