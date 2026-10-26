package com.flashsale.order.relay;

import com.flashsale.common.constant.KafkaTopics;
import com.flashsale.order.entity.OutboxEventEntity;
import com.flashsale.order.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayKafkaOutageHeadOfLineTest {

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
    @DisplayName("Scenario 02 (REM-05): Failing event receives exponential backoff and does not block subsequent eligible events")
    void failingEventBackoffAllowsSubsequentEventsToProceedWithoutHoLBlocking() {
        UUID event1Id = UUID.randomUUID();
        UUID event2Id = UUID.randomUUID();
        String order1Id = UUID.randomUUID().toString();
        String order2Id = UUID.randomUUID().toString();

        OutboxEventEntity event1 = new OutboxEventEntity(
            event1Id, "ORDER", order1Id, "ORDER_RESERVED", 1, "{\"orderId\":\"" + order1Id + "\"}"
        );
        OutboxEventEntity event2 = new OutboxEventEntity(
            event2Id, "ORDER", order2Id, "ORDER_RESERVED", 1, "{\"orderId\":\"" + order2Id + "\"}"
        );

        // Tick 1: Both events are eligible
        when(outboxEventRepository.findEligibleEventsForPublishing(any(Instant.class), any(Pageable.class)))
            .thenReturn(List.of(event1, event2));

        // Event 1 fails (Kafka broker times out)
        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker temporary timeout"));
        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_TOPIC), eq(order1Id), eq(event1.getPayload())))
            .thenReturn(failedFuture);

        // Event 2 succeeds
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(KafkaTopics.ORDERS_TOPIC, 0), 0, 0, 0, 0, 0);
        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_TOPIC), eq(order2Id), eq(event2.getPayload())))
            .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, metadata)));

        relayWorker.pollAndRelayEvents();

        // Verify Event 1 was NOT published and received exponential backoff
        assertThat(event1.getPublishedAt()).isNull();
        assertThat(event1.getAttemptCount()).isEqualTo(1);
        assertThat(event1.getNextRetryAt()).isNotNull();
        assertThat(event1.getNextRetryAt()).isAfter(Instant.now());
        assertThat(event1.getStatus()).isEqualTo("PENDING");

        // Verify Event 2 was published successfully despite Event 1's failure
        assertThat(event2.getPublishedAt()).isNotNull();
        assertThat(event2.getStatus()).isEqualTo("PUBLISHED");
        assertThat(event2.getLastError()).isNull();

        verify(outboxEventRepository, times(1)).save(event1);
        verify(outboxEventRepository, times(1)).save(event2);
    }

    @Test
    @DisplayName("Scenario 02 (REM-05): Exhausted retries when DLQ also fails sets EXHAUSTED_DLQ_FAILED and backoff to avoid HoL blocking")
    void exhaustedRetriesWithDlqFailureAppliesLongBackoffToAvoidBlocking() {
        UUID eventId = UUID.randomUUID();
        String orderId = UUID.randomUUID().toString();
        OutboxEventEntity event = new OutboxEventEntity(
            eventId, "ORDER", orderId, "ORDER_RESERVED", 1, "{\"orderId\":\"" + orderId + "\"}"
        );
        event.setAttemptCount(4); // Attempt 5 will exhaust

        when(outboxEventRepository.findEligibleEventsForPublishing(any(Instant.class), any(Pageable.class)))
            .thenReturn(List.of(event));

        // Primary send fails
        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Primary broker failure"));
        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_TOPIC), eq(orderId), eq(event.getPayload())))
            .thenReturn(failedFuture);

        // DLQ send also fails
        CompletableFuture<SendResult<String, String>> dlqFailedFuture = new CompletableFuture<>();
        dlqFailedFuture.completeExceptionally(new RuntimeException("DLQ cluster unavailable"));
        when(kafkaTemplate.send(eq(KafkaTopics.ORDERS_DLQ_TOPIC), eq(orderId), eq(event.getPayload())))
            .thenReturn(dlqFailedFuture);

        relayWorker.pollAndRelayEvents();

        assertThat(event.getAttemptCount()).isEqualTo(5);
        assertThat(event.getStatus()).isEqualTo("EXHAUSTED_DLQ_FAILED");
        assertThat(event.getNextRetryAt()).isAfter(Instant.now().plusSeconds(250)); // Delayed ~5 mins
        assertThat(event.getPublishedAt()).isNull();

        verify(outboxEventRepository, times(1)).save(event);
    }
}
