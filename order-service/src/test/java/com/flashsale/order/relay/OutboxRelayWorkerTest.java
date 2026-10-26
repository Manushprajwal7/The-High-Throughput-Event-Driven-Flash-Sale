package com.flashsale.order.relay;

import com.flashsale.common.constant.KafkaTopics;
import com.flashsale.order.entity.OutboxEventEntity;
import com.flashsale.order.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayWorkerTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxRelayWorker relayWorker;

    @BeforeEach
    void setUp() {
        relayWorker = new OutboxRelayWorker(outboxEventRepository, kafkaTemplate);
    }

    @Test
    void shouldRelayPendingEventAndMarkPublishedOnBrokerAck() {
        UUID eventId = UUID.randomUUID();
        String orderId = UUID.randomUUID().toString();
        OutboxEventEntity event = new OutboxEventEntity(
            eventId, "ORDER", orderId, "ORDER_RESERVED", 1, "{\"orderId\":\"" + orderId + "\"}"
        );

        when(outboxEventRepository.findEligibleEventsForPublishing(any(), any()))
            .thenReturn(List.of(event));

        RecordMetadata metadata = new RecordMetadata(new TopicPartition(KafkaTopics.ORDERS_TOPIC, 0), 0, 0, 0, 0, 0);
        SendResult<String, String> sendResult = new SendResult<>(null, metadata);
        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_TOPIC), eq(orderId), eq(event.getPayload())))
            .thenReturn(CompletableFuture.completedFuture(sendResult));

        relayWorker.pollAndRelayEvents();

        assertThat(event.getPublishedAt()).isNotNull();
        assertThat(event.getLastError()).isNull();

        verify(kafkaTemplate, times(1)).send(KafkaTopics.ORDERS_TOPIC, orderId, event.getPayload());
        verify(outboxEventRepository, times(1)).save(event);
    }

    @Test
    void shouldNotPollWhenPaused() {
        relayWorker.setPaused(true);
        relayWorker.pollAndRelayEvents();

        verify(outboxEventRepository, never()).findEligibleEventsForPublishing(any(), any());
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    void shouldIncrementAttemptAndNotMarkPublishedOnBrokerFailure() {
        UUID eventId = UUID.randomUUID();
        String orderId = UUID.randomUUID().toString();
        OutboxEventEntity event = new OutboxEventEntity(
            eventId, "ORDER", orderId, "ORDER_RESERVED", 1, "{\"orderId\":\"" + orderId + "\"}"
        );

        when(outboxEventRepository.findEligibleEventsForPublishing(any(), any()))
            .thenReturn(List.of(event));

        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker unavailable"));

        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_TOPIC), eq(orderId), eq(event.getPayload())))
            .thenReturn(failedFuture);

        relayWorker.pollAndRelayEvents();

        assertThat(event.getPublishedAt()).isNull(); // Must NOT mark published without broker ACK!
        assertThat(event.getAttemptCount()).isEqualTo(1);
        assertThat(event.getLastError()).contains("Kafka broker unavailable");

        verify(outboxEventRepository, times(1)).save(event);
    }

    @Test
    void shouldRouteToDlqWhenRetriesExhausted() {
        UUID eventId = UUID.randomUUID();
        String orderId = UUID.randomUUID().toString();
        OutboxEventEntity event = new OutboxEventEntity(
            eventId, "ORDER", orderId, "ORDER_RESERVED", 1, "{\"orderId\":\"" + orderId + "\"}"
        );
        event.setAttemptCount(4); // 5th attempt will exhaust

        when(outboxEventRepository.findEligibleEventsForPublishing(any(), any()))
            .thenReturn(List.of(event));

        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Fatal serialization/broker error"));

        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_TOPIC), eq(orderId), eq(event.getPayload())))
            .thenReturn(failedFuture);

        RecordMetadata dlqMetadata = new RecordMetadata(new TopicPartition(KafkaTopics.ORDERS_DLQ_TOPIC, 0), 0, 0, 0, 0, 0);
        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_DLQ_TOPIC), eq(orderId), eq(event.getPayload())))
            .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, dlqMetadata)));

        relayWorker.pollAndRelayEvents();

        assertThat(event.getAttemptCount()).isEqualTo(5);
        assertThat(event.getPublishedAt()).isNotNull();
        assertThat(event.getLastError()).contains("EXHAUSTED_RETRIES_ROUTED_TO_DLQ");

        verify(kafkaTemplate, times(1)).send(KafkaTopics.ORDERS_DLQ_TOPIC, orderId, event.getPayload());
        verify(outboxEventRepository, times(1)).save(event);
    }
}
