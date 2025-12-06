package com.kafka_sample.producer.orchestrator.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId;
    private String productId;
    private Integer quantity;
    private Double amount;
    private OrderStatus status;
    private String customerId;
}
