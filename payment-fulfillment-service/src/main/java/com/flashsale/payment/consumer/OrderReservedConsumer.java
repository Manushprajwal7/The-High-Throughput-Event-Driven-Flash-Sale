package com.flashsale.payment.consumer;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashsale.common.constant.KafkaTopics;
import com.flashsale.common.dto.OrderEventEnvelope;
import com.flashsale.common.dto.OrderReservedPayload;
import com.flashsale.payment.service.FulfillmentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderReservedConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderReservedConsumer.class);

    private final FulfillmentService fulfillmentService;
    private final ObjectMapper objectMapper;

    public OrderReservedConsumer(FulfillmentService fulfillmentService, ObjectMapper objectMapper) {
        this.fulfillmentService = fulfillmentService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = KafkaTopics.ORDERS_TOPIC,
        groupId = "${spring.kafka.consumer.group-id:payment-fulfillment-workers}",
        autoStartup = "${flashsale.kafka.consumer-enabled:true}"
    )
    public void onOrderReserved(ConsumerRecord<String, String> record) {
        log.info("Received Kafka record on topic {} with key {}: partition={}, offset={}",
            record.topic(), record.key(), record.partition(), record.offset());

        try {
            JavaType type = objectMapper.getTypeFactory().constructParametricType(
                OrderEventEnvelope.class,
                OrderReservedPayload.class
            );
            OrderEventEnvelope<OrderReservedPayload> envelope = objectMapper.readValue(record.value(), type);

            if ("ORDER_RESERVED".equals(envelope.eventType())) {
                fulfillmentService.processOrderReserved(envelope);
            } else {
                log.debug("Ignoring eventType {} on topic {}", envelope.eventType(), record.topic());
            }
        } catch (Exception ex) {
            log.error("Failed to process record offset {} on topic {}: {}",
                record.offset(), record.topic(), ex.getMessage());
            // Re-throw so Spring Kafka's DefaultErrorHandler can retry or route to DLQ
            throw new RuntimeException("Error processing Kafka message", ex);
        }
    }
}
