package com.flashsale.order.controller;

import com.flashsale.common.dto.CancelOrderResponse;
import com.flashsale.common.dto.CreateOrderRequest;
import com.flashsale.common.dto.OrderDetailsResponse;
import com.flashsale.common.dto.OrderResponse;
import com.flashsale.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(
        @RequestHeader("Idempotency-Key") String idempotencyKey,
        @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
        @Valid @RequestBody CreateOrderRequest request
    ) {
        String effectiveCorrelationId = (correlationId != null && !correlationId.isBlank())
            ? correlationId
            : UUID.randomUUID().toString();

        OrderResponse response = orderService.createOrder(idempotencyKey, request, effectiveCorrelationId);

        // 202 Accepted for new pending order; 200 OK for idempotent replay
        HttpStatus status = response.message().contains("existing") ? HttpStatus.OK : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status).body(response);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderDetailsResponse> getOrder(@PathVariable UUID orderId) {
        OrderDetailsResponse response = orderService.getOrder(orderId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{orderId}/cancel")
    public ResponseEntity<CancelOrderResponse> cancelOrder(
        @PathVariable UUID orderId,
        @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
        @RequestBody(required = false) Map<String, String> payload
    ) {
        String effectiveCorrelationId = (correlationId != null && !correlationId.isBlank())
            ? correlationId
            : UUID.randomUUID().toString();

        String reason = (payload != null && payload.containsKey("reason"))
            ? payload.get("reason")
            : "CLIENT_REQUESTED_CANCELLATION";

        CancelOrderResponse response = orderService.cancelOrder(orderId, reason, effectiveCorrelationId);
        return ResponseEntity.ok(response);
    }
}
