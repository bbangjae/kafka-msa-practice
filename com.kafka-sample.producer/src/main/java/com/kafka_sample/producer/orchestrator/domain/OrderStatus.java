package com.kafka_sample.producer.orchestrator.domain;

public enum OrderStatus {
    PENDING,
    PAYMENT_PENDING,
    PAYMENT_COMPLETED,
    INVENTORY_RESERVED,
    COMPLETED,
    CANCELLED,
    FAILED
}
