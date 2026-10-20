package com.flashsale.order.service;

import com.flashsale.common.constant.KafkaTopics;
import com.flashsale.common.dto.CreateOrderRequest;
import com.flashsale.common.dto.OrderResponse;
import com.flashsale.common.dto.ReservationResponse;
import com.flashsale.common.model.OrderStatus;
import com.flashsale.order.client.InventoryClient;
import com.flashsale.order.entity.OutboxEventEntity;
import com.flashsale.order.relay.OutboxRelayWorker;
import com.flashsale.order.repository.OrderRepository;
import com.flashsale.order.repository.OutboxEventRepository;
import com.flashsale.order.repository.ReservationRepository;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class TransactionalOutboxIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxRelayWorker outboxRelayWorker;

    @MockBean
    private InventoryClient inventoryClient;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void cleanUp() {
        orderRepository.deleteAll();
        reservationRepository.deleteAll();
        outboxEventRepository.deleteAll();
    }

    @Test
    void shouldPersistOrderAndOutboxEventInSameTransactionAndRelayToKafka() {
        UUID saleId = UUID.randomUUID();
        UUID resId = UUID.randomUUID();
        String shopperId = "shopper_outbox_test";
        String idempotencyKey = "key_outbox_123";

        CreateOrderRequest request = new CreateOrderRequest(saleId, shopperId, 1);

        when(inventoryClient.reserveStock(eq(saleId), eq(shopperId), eq(1), any()))
            .thenReturn(new ReservationResponse(resId, saleId, 1, Instant.now().plusSeconds(120)));

        // 1. Submit purchase request
        OrderResponse response = orderService.createOrder(idempotencyKey, request, "test-correlation-id");

        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(OrderStatus.PENDING);
        UUID orderId = response.orderId();

        // 2. Verify durable relational persistence
        assertThat(orderRepository.count()).isEqualTo(1L);
        assertThat(reservationRepository.count()).isEqualTo(1L);
        assertThat(outboxEventRepository.count()).isEqualTo(1L);

        List<OutboxEventEntity> pendingEvents = outboxEventRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
        assertThat(pendingEvents).hasSize(1);

        OutboxEventEntity outboxEvent = pendingEvents.get(0);
        assertThat(outboxEvent.getAggregateType()).isEqualTo("ORDER");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(orderId.toString());
        assertThat(outboxEvent.getEventType()).isEqualTo("ORDER_RESERVED");
        assertThat(outboxEvent.getPublishedAt()).isNull(); // Pending publication
        assertThat(outboxEvent.getPayload()).contains(orderId.toString());
        assertThat(outboxEvent.getPayload()).contains(shopperId);

        // 3. Mock Kafka ACK and trigger relay worker
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(KafkaTopics.ORDERS_TOPIC, 0), 0, 0, 0, 0, 0);
        SendResult<String, String> sendResult = new SendResult<>(null, metadata);
        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_TOPIC), eq(orderId.toString()), eq(outboxEvent.getPayload())))
            .thenReturn(CompletableFuture.completedFuture(sendResult));

        outboxRelayWorker.pollAndRelayEvents();

        // 4. Verify Kafka send was called with orderId partition key
        verify(kafkaTemplate, times(1)).send(KafkaTopics.ORDERS_TOPIC, orderId.toString(), outboxEvent.getPayload());

        // 5. Verify outbox event is now marked published in DB
        OutboxEventEntity updatedEvent = outboxEventRepository.findById(outboxEvent.getId()).orElseThrow();
        assertThat(updatedEvent.getPublishedAt()).isNotNull();
        assertThat(updatedEvent.getLastError()).isNull();

        // Backlog is now 0
        assertThat(outboxEventRepository.countByPublishedAtIsNull()).isEqualTo(0L);
    }
}
