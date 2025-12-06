package com.kafka_sample.producer.orchestrator.dto;

import com.kafka_sample.producer.orchestrator.domain.OrderStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {

    private String orderId;
    private String message;
    private OrderStatus status;
}
