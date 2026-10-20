package com.flashsale.order.relay;

import com.flashsale.common.constant.KafkaTopics;
import com.flashsale.order.entity.OutboxEventEntity;
import com.flashsale.order.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class OutboxRelayWorker {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelayWorker.class);
    private static final int MAX_PUBLISH_ATTEMPTS = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private final Counter publishedCounter;
    private final Counter retriesCounter;
    private final Counter dlqCounter;

    @Value("${flashsale.outbox.relay-enabled:true}")
    private boolean relayEnabled = true;

    public OutboxRelayWorker(
        OutboxEventRepository outboxEventRepository,
        KafkaTemplate<String, String> kafkaTemplate
    ) {
        this(outboxEventRepository, kafkaTemplate, new SimpleMeterRegistry());
    }

    @Autowired
    public OutboxRelayWorker(
        OutboxEventRepository outboxEventRepository,
        KafkaTemplate<String, String> kafkaTemplate,
        MeterRegistry meterRegistry
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.publishedCounter = meterRegistry.counter("flashsale.outbox.events", "status", "published");
        this.retriesCounter = meterRegistry.counter("flashsale.outbox.events", "status", "retry");
        this.dlqCounter = meterRegistry.counter("flashsale.outbox.events", "status", "dlq");
    }

    public void setPaused(boolean isPaused) {
        this.paused.set(isPaused);
        log.info("Outbox relay worker paused state set to: {}", isPaused);
    }

    public boolean isPaused() {
        return this.paused.get();
    }

    public void setRelayEnabled(boolean relayEnabled) {
        this.relayEnabled = relayEnabled;
    }

    public boolean isRelayEnabled() {
        return this.relayEnabled;
    }

    @Scheduled(fixedDelayString = "${flashsale.outbox.relay-interval-ms:200}")
    public void scheduledRelay() {
        if (!relayEnabled || paused.get()) {
            return;
        }
        pollAndRelayEvents();
    }

    public void pollAndRelayEvents() {
        if (paused.get()) {
            return;
        }

        List<OutboxEventEntity> pendingEvents = outboxEventRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
        if (pendingEvents.isEmpty()) {
            return;
        }

        log.debug("Found {} pending outbox events to relay", pendingEvents.size());

        for (OutboxEventEntity event : pendingEvents) {
            relaySingleEvent(event);
        }
    }

    @Transactional
    public void relaySingleEvent(OutboxEventEntity event) {
        String topic = KafkaTopics.ORDERS_TOPIC;
        String key = event.getAggregateId();

        try {
            // Synchronously wait for broker acknowledgement
            kafkaTemplate.send(topic, key, event.getPayload()).get(5, TimeUnit.SECONDS);

            event.setPublishedAt(Instant.now());
            event.setLastError(null);
            outboxEventRepository.save(event);
            publishedCounter.increment();

            log.info("Successfully published outbox event {} (aggregateId={}) to topic {}",
                event.getId(), key, topic);
        } catch (Exception ex) {
            retriesCounter.increment();
            int attempts = event.getAttemptCount() + 1;
            event.setAttemptCount(attempts);
            String errMsg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
            event.setLastError(errMsg.length() > 1000 ? errMsg.substring(0, 1000) : errMsg);

            log.warn("Failed to publish outbox event {} on attempt {}/{} to topic {}: {}",
                event.getId(), attempts, MAX_PUBLISH_ATTEMPTS, topic, errMsg);

            if (attempts >= MAX_PUBLISH_ATTEMPTS) {
                routeToDeadLetterTopic(event, errMsg);
            } else {
                outboxEventRepository.save(event);
            }
        }
    }

    private void routeToDeadLetterTopic(OutboxEventEntity event, String originalError) {
        String dlqTopic = KafkaTopics.ORDERS_DLQ_TOPIC;
        String key = event.getAggregateId();
        try {
            kafkaTemplate.send(dlqTopic, key, event.getPayload()).get(5, TimeUnit.SECONDS);
            event.setPublishedAt(Instant.now());
            event.setLastError("EXHAUSTED_RETRIES_ROUTED_TO_DLQ: " + originalError);
            outboxEventRepository.save(event);
            dlqCounter.increment();
            log.error("Exhausted retries for event {}. Routed to DLQ: {}", event.getId(), dlqTopic);
        } catch (Exception dlqEx) {
            log.error("CRITICAL: Failed to route event {} to DLQ topic {}: {}",
                event.getId(), dlqTopic, dlqEx.getMessage());
            outboxEventRepository.save(event);
        }
    }
}
