package com.kafka_sample.producer.orchestrator.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private String orderId;
    private String productId;
    private Integer quantity;
    private Double amount;
    private String customerId;
    private EventType eventType;

    public enum EventType {
        ORDER_CREATED,
        PAYMENT_REQUESTED,
        PAYMENT_COMPLETED,
        PAYMENT_FAILED,
        INVENTORY_RESERVED,
        INVENTORY_FAILED,
        ORDER_COMPLETED,
        ORDER_CANCELLED
    }
}
