package com.flashsale.common.constant;

public final class KafkaTopics {
    private KafkaTopics() {}

    public static final String ORDERS_TOPIC = "flashsale.orders.v1";
    public static final String ORDERS_DLQ_TOPIC = "flashsale.orders.dlq.v1";
}
