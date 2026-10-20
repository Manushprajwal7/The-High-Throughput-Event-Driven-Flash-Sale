package com.flashsale.order.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashsale.common.dto.CreateOrderRequest;
import com.flashsale.common.dto.OrderDetailsResponse;
import com.flashsale.common.dto.OrderResponse;
import com.flashsale.common.exception.IdempotencyConflictException;
import com.flashsale.common.exception.OrderNotFoundException;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.order.exception.GlobalExceptionHandler;
import com.flashsale.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private OrderService orderService;

    @InjectMocks
    private OrderController orderController;

    private MockMvc mockMvc;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(orderController)
            .setControllerAdvice(new GlobalExceptionHandler())
            .build();
    }

    @Test
    void shouldReturn202ForNewAcceptedOrder() throws Exception {
        UUID saleId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        CreateOrderRequest request = new CreateOrderRequest(saleId, "shopper_1", 1);
        OrderResponse response = new OrderResponse(
            orderId, saleId, "shopper_1", 1, OrderStatus.PENDING, resId, Instant.now(), "Order accepted for processing"
        );

        when(orderService.createOrder(eq("key_123"), any(CreateOrderRequest.class), any()))
            .thenReturn(response);

        mockMvc.perform(post("/api/v1/orders")
                .header("Idempotency-Key", "key_123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.orderId").value(orderId.toString()))
            .andExpect(jsonPath("$.status").value("PENDING"))
            .andExpect(jsonPath("$.reservationId").value(resId.toString()));
    }

    @Test
    void shouldReturn200ForIdempotentReplay() throws Exception {
        UUID saleId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        CreateOrderRequest request = new CreateOrderRequest(saleId, "shopper_1", 1);
        OrderResponse response = new OrderResponse(
            orderId, saleId, "shopper_1", 1, OrderStatus.PENDING, resId, Instant.now(), "Order retrieved from existing idempotent submission"
        );

        when(orderService.createOrder(eq("key_123"), any(CreateOrderRequest.class), any()))
            .thenReturn(response);

        mockMvc.perform(post("/api/v1/orders")
                .header("Idempotency-Key", "key_123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.orderId").value(orderId.toString()))
            .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("existing")));
    }

    @Test
    void shouldReturn409ForIdempotencyConflict() throws Exception {
        UUID saleId = UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest(saleId, "shopper_1", 2);

        when(orderService.createOrder(eq("key_123"), any(CreateOrderRequest.class), any()))
            .thenThrow(new IdempotencyConflictException("Payload mismatch"));

        mockMvc.perform(post("/api/v1/orders")
                .header("Idempotency-Key", "key_123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.code").value("IDEMPOTENCY_CONFLICT"));
    }

    @Test
    void shouldReturn200ForExistingOrderQuery() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();

        OrderDetailsResponse response = new OrderDetailsResponse(
            orderId, saleId, "shopper_1", 1, OrderStatus.CONFIRMED, resId, "pay_123", null, Instant.now(), Instant.now()
        );

        when(orderService.getOrder(orderId)).thenReturn(response);

        mockMvc.perform(get("/api/v1/orders/{orderId}", orderId))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.orderId").value(orderId.toString()))
            .andExpect(jsonPath("$.status").value("CONFIRMED"));
    }

    @Test
    void shouldReturn404WhenOrderNotFound() throws Exception {
        UUID orderId = UUID.randomUUID();
        when(orderService.getOrder(orderId)).thenThrow(new OrderNotFoundException(orderId));

        mockMvc.perform(get("/api/v1/orders/{orderId}", orderId))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.code").value("ORDER_NOT_FOUND"));
    }
}
